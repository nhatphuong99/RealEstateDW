"""
Script: parse_to_staging.py
Muc dich: Doc du lieu Bronze (S3) da upload boi upload_bronze_to_s3.py,
parse HTML -> trich xuat field, dedup theo url, convert kieu du lieu,
roi load vao bang staging.stg_listing_detail trong Postgres DW de
kiem tra chat luong du lieu (QC) truoc khi thiet ke Silver chinh thuc.

Cach chay:
    python -m scripts.parse_to_staging                       # parse Bronze cua hom nay (GMT+7)
    python -m scripts.parse_to_staging  --date 2026-08-14
    python -m scripts.parse_to_staging  --date 2026-08-14 --limit 20   # chay thu, kiem tra selector

GHI CHU VE DO TIN CAY SELECTOR:
Toan bo selector duoi day (core fields + enrichment fields) da duoc XAC NHAN
qua nhieu sample HTML that tren alonhadat.com.vn (khong con la best-guess nhu
phien ban dau tien). Tuy nhien HTML thuc te tren 10.000 record co the van co
bien the chua gap trong sample (vi du: tin dang thieu section nao do, dinh
dang khac cho loai BDS dac biet...) -> luon chay thu voi --limit truoc, doc
cot parse_status/parse_errors de phat hien bien the moi.

Cac dieu da xac nhan quan trong tu sample that (khac voi ban dau doan):
- Gia (price): lay tu ATTRIBUTE "value" cua <data itemprop="price">, la so
  VND sach san, KHONG can parse chuoi "4,5 ty". value="0" + text co "Thoa
  thuan" -> gia thuong luong (price_is_negotiable = True).
- Dien tich (area): so nam o itemprop="value" LONG BEN TRONG itemprop="floorSize".
- Ngay dang (posted_date): lay tu ATTRIBUTE "datetime" cua <time itemprop=
  "datePosted">, KHONG lay text (text co the la "Hom qua"/"Hom nay"/"dd/mm/yyyy").
- Icon check (Phong an, Nha bep...): la THE <img alt="check"> hoac src chua
  "check", KHONG phai <i>/<svg> co class "check" nhu doan ban dau.
- Nhan "Cho de xe hoi" tren site GHI LA "Chổ để xe hơi" (sai chinh ta so voi
  "Chỗ" chuan) -> phai map dung chuoi nay.
- Cot "Huong" dung ky hieu "_" (gach duoi) rieng cho gia tri thieu, khac voi
  "---" dung o cac cot khac -> can chuan hoa chung ve None.
- The canh bao (warning) co the la <p class="warning" role="alert"> nam
  TRUOC ca <header>, khong nhat thiet la <div class="warning">.
- Dia chi co 2 ban: dia chi MOI (itemprop="address", da sap nhap) va dia chi
  CU (p.old-address, dang text tho, chua tach) -> giu ca 2, xu ly tach o Silver.
"""

import argparse
import json
import os
import re
from datetime import datetime, timedelta, timezone
from io import BytesIO

import boto3
import pandas as pd
import psycopg2
import psycopg2.extras as pg_extras
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.environ["S3_BRONZE_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
# Script nay chay tu host (khong phai trong container Airflow) -> dung DSN_LOCAL
DW_DSN = os.environ["POSTGRES_DW_DSN_LOCAL"]
GMT7 = timezone(timedelta(hours=7))

# Cac ky hieu the hien "khong co du lieu" tren site (da gap ca "_" va "---")
MISSING_MARKERS = {"_", "-", "--", "---", ""}

# -----------------------------------------------------------------------------
# Mapping nhan tieng Viet (trong section.moreinfor1) -> ten field chuan hoa.
# Da xac nhan qua nhieu sample HTML that.
# LUU Y: "Chổ để xe hơi" giu dung chinh ta (sai dau) nhu tren site that.
# -----------------------------------------------------------------------------
ENRICHMENT_LABEL_MAP = {
    "Mã tin": "listing_id_from_table",
    "Loại tin": "listing_type",
    "Loại BDS": "property_type",
    "Chiều ngang": "width_m_raw",
    "Chiều dài": "length_m_raw",
    "Hướng": "orientation_raw",
    "Đường trước nhà": "street_width_m_raw",
    "Pháp lý": "legal_status_raw",
    "Số lầu": "floors_raw",
    "Số phòng ngủ": "bedrooms_raw",
    "Phòng ăn": "has_dining_room_raw",
    "Nhà bếp": "has_kitchen_raw",
    "Sân thượng": "has_rooftop_raw",
    "Chổ để xe hơi": "has_car_parking_raw",   # dung "Chổ" theo dung HTML that
    "Chính chủ": "owner_direct_raw",
    "Thuộc dự án": "project_name_raw",
}

BOOLEAN_RAW_FIELDS = [
    "has_dining_room_raw", "has_kitchen_raw", "has_rooftop_raw",
    "has_car_parking_raw", "owner_direct_raw",
]


# -----------------------------------------------------------------------------
# Ham convert / lam sach du lieu
# -----------------------------------------------------------------------------
def clean_text_field(text):
    """Tra ve None neu text la 1 trong cac ky hieu 'thieu du lieu' cua site."""
    if text is None:
        return None
    stripped = text.strip()
    return None if stripped in MISSING_MARKERS else stripped


def get_clean_text(el):
    """
    Lay text tu 1 element, dung separator=" " giua cac the con de tranh dinh
    lien 2 doan text khong co khoang trang (vi du <a>Ten du an</a>(xem chi
    tiet) -> neu khong co separator se dinh thanh "...Point(xem chi tiet...").
    Sau do gop nhieu khoang trang/xuong dong lien tiep thanh 1 khoang trang.
    """
    if el is None:
        return None
    raw = el.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", raw).strip()


def parse_float_vn(text):
    """Chuyen chuoi kieu '4,65m' hoac '12m' -> float. Tra None neu khong parse duoc."""
    text = clean_text_field(text)
    if not text:
        return None
    cleaned = text.lower().replace("m", "").replace(",", ".").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_int_safe(text):
    text = clean_text_field(text)
    if not text:
        return None
    cleaned = re.sub(r"[^\d]", "", text)
    return int(cleaned) if cleaned else None


def parse_tribool(cell):
    """
    Tra ve True / False / None dua tren noi dung cell trong bang moreinfor1:
    - Rong hoac nam trong MISSING_MARKERS -> None (khong ro)
    - Co the <img alt="check"> hoac src chua "check" -> True (da xac nhan qua
      sample that: <img src="/publish/img/check.gif" alt="check" />)
    - Co text khac -> False (hiem gap trong thuc te)
    """
    if cell is None:
        return None
    text = cell.get_text(strip=True)
    if clean_text_field(text) is None and cell.find("img") is None:
        return None
    icon = cell.find("img", alt=re.compile("check", re.I))
    if icon is None:
        icon = cell.find("img", src=re.compile("check", re.I))
    if icon is not None:
        return True
    if clean_text_field(text) is None:
        return None
    return False


def extract_listing_id_from_url(url: str):
    """URL chi tiet dang ...-12345678.html -> lay phan so cuoi lam listing_id."""
    m = re.search(r"-(\d+)\.html?$", url)
    return m.group(1) if m else None


# -----------------------------------------------------------------------------
# Core fields: title, posted_date, price, area, address (moi + cu)
# Tat ca da xac nhan qua sample HTML that.
# -----------------------------------------------------------------------------
def extract_core_fields(article) -> dict:
    result = {
        "title": None,
        "posted_date": None,
        "price_vnd": None,
        "price_is_negotiable": False,
        "price_raw": None,
        "area_m2": None,
        "area_raw": None,
        "address_street_new": None,
        "address_ward_new": None,
        "address_province_new": None,
        "address_old_raw": None,
    }

    # --- Title ---
    title_el = article.find(attrs={"itemprop": "name"})
    if title_el is not None:
        result["title"] = get_clean_text(title_el)

    # --- Ngay dang: uu tien attribute datetime, KHONG dung text ("Hom qua"...) ---
    time_el = article.find("time", attrs={"itemprop": "datePosted"})
    if time_el is not None and time_el.get("datetime"):
        result["posted_date"] = time_el["datetime"].strip()  # dang "YYYY-MM-DD", Postgres tu parse duoc

    # --- Gia: so sach nam o attribute "value" cua the <data itemprop="price"> ---
    price_el = article.find(attrs={"itemprop": "price"})
    if price_el is not None:
        result["price_raw"] = price_el.get_text(strip=True)
        value_attr = price_el.get("value")
        if value_attr is not None:
            try:
                price_int = int(value_attr)
                if price_int == 0:
                    # value="0" la cach site the hien "Thoa thuan" (gia thuong luong)
                    result["price_is_negotiable"] = True
                    result["price_vnd"] = None
                else:
                    result["price_vnd"] = price_int
            except ValueError:
                pass

    # --- Dien tich: so nam o itemprop="value" LONG BEN TRONG itemprop="floorSize" ---
    area_wrap = article.find(attrs={"itemprop": "floorSize"})
    if area_wrap is not None:
        result["area_raw"] = get_clean_text(area_wrap)
        value_el = area_wrap.find(attrs={"itemprop": "value"})
        if value_el is not None:
            try:
                result["area_m2"] = float(value_el.get_text(strip=True).replace(",", "."))
            except ValueError:
                pass

    # --- Dia chi moi (itemprop="address", da sap nhap) ---
    address_el = article.find(attrs={"itemprop": "address"})
    if address_el is not None:
        street_el = address_el.find(attrs={"itemprop": "streetAddress"})
        ward_el = address_el.find(attrs={"itemprop": "addressLocality"})
        region_el = address_el.find(attrs={"itemprop": "addressRegion"})
        result["address_street_new"] = street_el.get_text(strip=True) if street_el else None
        result["address_ward_new"] = ward_el.get_text(strip=True) if ward_el else None
        result["address_province_new"] = region_el.get_text(strip=True) if region_el else None

    # --- Dia chi cu (p.old-address, text tho, chua tach - se xu ly o Silver) ---
    old_address_el = article.find("p", class_="old-address")
    if old_address_el is not None:
        result["address_old_raw"] = get_clean_text(old_address_el)

    return result


def extract_enrichment_fields(article) -> dict:
    """
    Trich xuat bang key-value trong section.moreinfor1.
    So cot khong deu giua cac dong (vi du dong "Thuoc du an" chi co 2 cell,
    dong thuong co 6 cell = 3 cap label/value) -> duyet theo cap lien tiep,
    khong gia dinh vi tri co dinh.
    """
    raw = {}
    section = article.find("section", class_="moreinfor1")
    if section is None:
        return raw

    for row in section.find_all("tr"):
        cells = row.find_all(["td", "th"])
        for i in range(0, len(cells) - 1, 2):
            label = cells[i].get_text(strip=True).rstrip(":")
            value_cell = cells[i + 1]
            field = ENRICHMENT_LABEL_MAP.get(label)
            if not field:
                continue
            if field in BOOLEAN_RAW_FIELDS:
                raw[field] = value_cell  # giu nguyen the de parse_tribool tim <img>
            else:
                raw[field] = get_clean_text(value_cell)

    return raw


def parse_one_listing(url: str, crawl_date, html_bytes, s3_key: str) -> dict:
    """Parse 1 record HTML -> dict field da chuan hoa, kem parse_status/parse_errors de QC."""
    errors = []
    record = {
        "listing_url": url,
        "listing_id": extract_listing_id_from_url(url),
        "crawl_date": crawl_date,
        "source_batch_s3_key": s3_key,
    }

    try:
        if isinstance(html_bytes, (bytes, bytearray)):
            html_text = html_bytes.decode("utf-8", errors="replace")
        else:
            html_text = str(html_bytes)

        soup = BeautifulSoup(html_text, "lxml")

        # Chi lay dung <article class="property"> (KHONG phai "property-item"
        # cua tin lien quan/sidebar) -> tranh lay nham du lieu tin khac.
        article = soup.find("article", class_="property")
        if article is None:
            errors.append("khong_tim_thay_article.property")
            article = soup  # fallback: parse toan trang, do tin cay thap hon

        # --- Core fields ---
        core = extract_core_fields(article)
        record.update(core)

        if record["title"] is None:
            errors.append("missing_title")
        if record["price_vnd"] is None and not record["price_is_negotiable"]:
            errors.append("missing_price")
        if record["area_m2"] is None:
            errors.append("missing_area")
        if record["address_street_new"] is None:
            errors.append("missing_address_new")

        # --- Enrichment fields ---
        raw = extract_enrichment_fields(article)

        record["listing_type"] = clean_text_field(raw.get("listing_type"))
        record["property_type"] = clean_text_field(raw.get("property_type"))
        record["orientation"] = clean_text_field(raw.get("orientation_raw"))
        record["legal_status"] = clean_text_field(raw.get("legal_status_raw"))
        record["project_name"] = clean_text_field(raw.get("project_name_raw"))

        record["width_m"] = parse_float_vn(raw.get("width_m_raw"))
        record["length_m"] = parse_float_vn(raw.get("length_m_raw"))
        record["street_width_m"] = parse_float_vn(raw.get("street_width_m_raw"))
        record["floors"] = parse_int_safe(raw.get("floors_raw"))
        record["bedrooms"] = parse_int_safe(raw.get("bedrooms_raw"))

        for bf in BOOLEAN_RAW_FIELDS:
            target = bf.replace("_raw", "")
            record[target] = parse_tribool(raw.get(bf))

        for required in ["property_type", "listing_type"]:
            if record.get(required) is None:
                errors.append(f"missing_{required}")

        # QC cheo: listing_id lay tu URL vs listing_id trong bang "Ma tin"
        table_id = raw.get("listing_id_from_table")
        if table_id and record["listing_id"] and table_id.strip() != record["listing_id"]:
            errors.append("listing_id_mismatch_url_vs_table")

        # --- Canh bao (co the la <p> hoac <div>, khong gioi han tag) ---
        warning_el = article.find(class_="warning")
        record["has_warning"] = warning_el is not None
        record["warning_text"] = get_clean_text(warning_el) if warning_el else None

        record["parse_status"] = "ok" if not errors else "partial"

    except Exception as e:  # noqa: BLE001 -- bat moi loi de khong lam sap ca batch
        errors.append(f"exception:{type(e).__name__}:{e}")
        record["parse_status"] = "failed"
        defaults = [
            "title", "posted_date", "price_vnd", "price_is_negotiable", "price_raw",
            "area_m2", "area_raw", "address_street_new", "address_ward_new",
            "address_province_new", "address_old_raw", "listing_type", "property_type",
            "orientation", "legal_status", "project_name", "width_m", "length_m",
            "street_width_m", "floors", "bedrooms", "has_dining_room", "has_kitchen",
            "has_rooftop", "has_car_parking", "owner_direct", "has_warning", "warning_text",
        ]
        for f in defaults:
            record.setdefault(f, None)

    record["parse_errors"] = json.dumps(errors, ensure_ascii=False) if errors else None
    return record


# -----------------------------------------------------------------------------
# Doc du lieu Bronze tu S3, dedup
# -----------------------------------------------------------------------------
def load_bronze_batches(s3_client, load_date: str) -> pd.DataFrame:
    prefix = f"test/{load_date}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    dfs = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("data.parquet"):
                print(f"[INFO] Dang doc {obj['Key']} ...")
                resp = s3_client.get_object(Bucket=S3_BUCKET, Key=obj["Key"])
                buf = BytesIO(resp["Body"].read())
                df = pd.read_parquet(buf)
                df["_s3_key"] = obj["Key"]
                dfs.append(df)
    if not dfs:
        return pd.DataFrame(columns=["url", "crawl_date", "html", "_s3_key"])
    return pd.concat(dfs, ignore_index=True)


def dedup_bronze(df: pd.DataFrame) -> pd.DataFrame:
    """
    Bronze cho phep trung lap (append-only, dung thiet ke). O buoc load vao
    staging thi PHAI dedup theo url: giu lai ban ghi co crawl_date MOI NHAT.
    """
    before = len(df)
    df_sorted = df.sort_values("crawl_date")
    df_deduped = df_sorted.drop_duplicates(subset="url", keep="last").reset_index(drop=True)
    after = len(df_deduped)
    print(f"[INFO] Dedup theo url: {before} -> {after} record (loai {before - after} ban trung)")
    return df_deduped


# -----------------------------------------------------------------------------
# Load vao Postgres (upsert, idempotent: chay lai an toan, khong tao dup)
# -----------------------------------------------------------------------------
STAGING_COLUMNS = [
    "listing_url", "listing_id", "crawl_date",
    "title", "posted_date",
    "price_vnd", "price_is_negotiable", "price_raw",
    "area_m2", "area_raw",
    "address_street_new", "address_ward_new", "address_province_new", "address_old_raw",
    "listing_type", "property_type",
    "width_m", "length_m", "orientation", "street_width_m",
    "legal_status", "floors", "bedrooms",
    "has_dining_room", "has_kitchen", "has_rooftop",
    "has_car_parking", "owner_direct", "project_name",
    "has_warning", "warning_text",
    "parse_status", "parse_errors", "source_batch_s3_key",
]


def upsert_staging(records: list):
    if not records:
        print("[INFO] Khong co record nao de load.")
        return

    rows = [tuple(r.get(c) for c in STAGING_COLUMNS) for r in records]

    update_cols = [c for c in STAGING_COLUMNS if c != "listing_url"]
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    sql = f"""
        INSERT INTO staging.stg_listing_detail ({", ".join(STAGING_COLUMNS)})
        VALUES %s
        ON CONFLICT (listing_url) DO UPDATE SET
            {update_clause},
            loaded_at = now()
        WHERE EXCLUDED.crawl_date >= staging.stg_listing_detail.crawl_date;
    """

    with psycopg2.connect(DW_DSN) as conn:
        with conn.cursor() as cur:
            pg_extras.execute_values(cur, sql, rows, page_size=500)
        conn.commit()

    print(f"[XONG] Da upsert {len(rows)} record vao staging.stg_listing_detail")


def main():
    parser = argparse.ArgumentParser(description="Parse Bronze (S3) -> staging Postgres de QC")
    parser.add_argument(
        "--date",
        default=datetime.now(GMT7).strftime("%Y-%m-%d"),
        help="Ngay Bronze can parse, dang yyyy-MM-dd (mac dinh: hom nay, GMT+7)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Gioi han so record parse thu (dung khi kiem tra/chinh selector)",
    )
    args = parser.parse_args()

    s3 = boto3.client("s3", region_name=AWS_REGION)

    print(f"[INFO] Dang load Bronze batch cua ngay {args.date} tu S3 ...")
    df = load_bronze_batches(s3, args.date)
    if df.empty:
        print(f"[CANH BAO] Khong tim thay batch nao cho ngay {args.date}")
        return

    print(f"[INFO] Tong {len(df)} record tho doc duoc (co the con trung).")
    df = dedup_bronze(df)

    if args.limit:
        df = df.head(args.limit)
        print(f"[INFO] Gioi han thu nghiem: chi parse {len(df)} record dau tien.")

    records = []
    status_counter = {"ok": 0, "partial": 0, "failed": 0}
    for _, row in df.iterrows():
        rec = parse_one_listing(row["url"], row["crawl_date"], row["html"], row["_s3_key"])
        status_counter[rec["parse_status"]] += 1
        records.append(rec)

    print(f"[INFO] Ket qua parse: {status_counter}")
    upsert_staging(records)


if __name__ == "__main__":
    main()
