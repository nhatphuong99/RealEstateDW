"""
Script: parse_to_staging.py

*** SCRIPT NAY CHI DUNG DE EDA / KIEM TRA CHAT LUONG DU LIEU (QC) ***
*** KHONG PHAI LA PARSER BRONZE -> SILVER CHINH THUC ***

Bang staging_old_v2.stg_listing_detail ma script nay ghi vao la bang PHU, dung de
kiem tra: selector co dung khong, ty le parse thanh cong bao nhieu %, cac
bien the/exception nao con ton tai trong du lieu that (--limit de chay thu
tren mot phan nho truoc khi ket luan).

Parser BRONZE -> SILVER CHINH THUC se la 1 CODEBASE RIENG, dung PySpark
(khong phai Python/BeautifulSoup nhu o day), thiet ke va viet o GIAI DOAN
SAU (xem alonhadat_data_source_analysis.md muc 8). Cac quy tac xu ly da
XAC NHAN DUNG o day (parse_vn_number, tach area_is_undetermined khoi loi
missing_area, boolean tri-state, dia chi moi/cu...) la KIEN THUC THAM KHAO
can AP DUNG LAI (khong phai ke thua tu dong code) khi viet parser PySpark do.

Muc dich: Doc du lieu Bronze (S3, dang bronze/<yyyy-MM-dd>/crawl-<n>/partN.parquet
da duoc bronze_crawler tai ve), parse HTML -> trich xuat field, dedup theo url,
convert kieu du lieu, roi load vao bang staging_old_v2.stg_listing_detail trong
Postgres DW de kiem tra chat luong du lieu (QC) truoc khi thiet ke Silver chinh thuc.

Cach chay:
    python parse_to_staging.py                          # parse Bronze cua hom nay (GMT+7)
    python parse_to_staging.py --date 2026-08-14
    python parse_to_staging.py --date 2026-08-14 --limit 20   # chay thu, kiem tra selector

GHI CHU VE DO TIN CAY SELECTOR:
Toan bo selector duoi day (core fields + enrichment fields) da duoc XAC NHAN
qua nhieu sample HTML that tren alonhadat.com.vn. Tuy nhien HTML thuc te tren
764.212 record co the van co bien the chua gap trong sample -> luon chay thu
voi --limit truoc, doc cot parse_status/parse_errors de phat hien bien the moi.

Cac dieu da xac nhan quan trong tu sample that:
- Gia (price): lay tu ATTRIBUTE "value" cua <data itemprop="price">, la so
  VND sach san. value="0" + text co "Thoa thuan" -> gia thuong luong. Text
  hien thi KHAC NHAU giua "Can ban" (VD "3,95 ty") va "Cho thue" (VD "1,9
  ty /thang") nhung KHONG anh huong vi ta luon doc tu attribute, khong parse
  text (xem exception_data.md).
- Dien tich (area): so nam o itemprop="value" LONG BEN TRONG itemprop="floorSize".
  QUAN TRONG: dung parse_vn_number (KHONG phai float() truc tiep) vi dien
  tich > 1000 m2 hien thi dang "5.300" - dau "." la HANG NGHIN, khong phai
  thap phan (loi am tham neu lam sai, khong bi bat boi try/except). Gia tri
  "KXĐ" (Khong Xac Dinh) la 1 gia tri THAT do site ghi nhan -> luu rieng qua
  area_is_undetermined, KHONG tinh la loi parse (missing_area).
- Ngay dang (posted_date): lay tu ATTRIBUTE "datetime" cua <time itemprop=
  "datePosted">, KHONG lay text (text co the la "Hom qua"/"Hom nay").
- Icon check (Phong an, Nha bep...): la THE <img alt="check"> hoac src chua
  "check". Tren thuc te CHUA TUNG quan sat duoc gia tri False THAT (chi co
  check-icon=True hoac "---"=None, "khong co gia tri de phu nhan truc tiep")
  -> neu gap False that trong du lieu that, se duoc gan co QC
  unexpected_false_value_<field> de nguoi xem lai, khong am tham chap nhan.
- Nhan "Cho de xe hoi" tren site GHI LA "Chổ để xe hơi" (sai chinh ta so voi "Chỗ" chuan).
- Cot "Huong" dung ky hieu "_" rieng cho gia tri thieu, khac "---" o cac cot khac.
- The canh bao (warning) co the la <p class="warning"> hoac <div class="warning">.
- Tin HET HAN co <div class="expired"><img src=".../expired.png" /></div>.
- Dia chi co 2 ban: dia chi MOI (itemprop="address") va dia chi CU (p.old-address).
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

# Rieng dien tich: site dung "KXĐ" (Khong Xac Dinh) - KHAC voi MISSING_MARKERS
# o cho day la 1 GIA TRI THAT SU do site chu dong ghi nhan (khong phai do
# thieu du lieu/loi crawl) -> can phan biet rieng, khong gop chung voi
# "khong tim thay field" (xem area_is_undetermined trong extract_core_fields).
AREA_UNDETERMINED_MARKERS = {"KXĐ", "KXD"}  # ca 2 dang co dau/khong dau de an toan

# Regex nhan dien ten file part trong S3 key, ho tro ca 2 dang:
# ".../part37.parquet" va ".../part=37.parquet".
PART_FILENAME_RE = re.compile(r"part=?([0-9]+)\.parquet$")
TOTAL_DATASET_PARTS = 77


# =============================================================================
# BO LOC LOAI TIN: chi giu 'Cần bán' va 'Cho thuê' (bo qua 'Cần mua', 'Cần thuê', ...)
#
# LY DO: bai toan da chot (xem 01_phan_tich_yeu_cau_so_bo.md) la phan tich GIA
# RAO BAN/RAO THUE theo quan/phuong. Tin "Cần mua"/"Cần thuê" la tin CHIEU NGUOC
# LAI (nguoi mua/nguoi thue dang tim mua/thue, KHONG PHAI gia niem yet cua BDS)
# -> neu tinh chung vao phan tich gia se lam SAI LECH ket qua (khong phai gia
# thi truong, ma la gia ky vong cua nguoi mua/thue).
#
# Script nap toan bo dataset de EDA, KHONG loc theo listing_type.
# Ham `apply_listing_type_filter` van duoc giu lai de tai su dung neu can
# tao mot tap du lieu rieng cho phan tich gia rao ban/rao thue.
# =============================================================================
ALLOWED_LISTING_TYPES = {"Cần bán", "Cho thuê"}


def apply_listing_type_filter(records: list, quiet: bool = False) -> list:
    """
    CHI GIU cac record co listing_type thuoc ALLOWED_LISTING_TYPES.
    Record co listing_type = None (do loi/thieu du lieu khi parse) VAN duoc
    giu lai o day - vi day la loc theo LOAI TIN da xac dinh duoc, KHONG phai
    loc theo chat luong parse (chat luong parse da co parse_status/parse_errors
    rieng de xu ly, khong nen tron 2 muc dich loc vao 1 cho).

    quiet=True: khong in log tung lan goi - dung khi ham nay duoc goi LAP LAI
    theo tung part (xem main()), de tranh spam hang chuc dong log giong nhau.
    """
    before = len(records)
    filtered = [
        r for r in records
        if r.get("listing_type") is None or r["listing_type"] in ALLOWED_LISTING_TYPES
    ]
    after = len(filtered)
    if not quiet:
        print(
            f"[INFO] Loc loai tin (chi giu {sorted(ALLOWED_LISTING_TYPES)}): "
            f"{before} -> {after} record (loai {before - after} record thuoc loai tin khac, "
            f"vi du 'Cần mua', 'Cần thuê')"
        )
    return filtered


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
    lien 2 doan text khong co khoang trang. Sau do gop nhieu khoang trang/
    xuong dong lien tiep thanh 1 khoang trang.
    """
    if el is None:
        return None
    raw = el.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", raw).strip()


def parse_vn_number(text):
    """
    Parse so dang tieng Viet: dau "." la phan cach HANG NGHIN, dau "," la
    phan cach THAP PHAN (NGUOC voi chuan quoc te vi du "1,234.5").

    VD: "5.300" -> 5300.0 (KHONG PHAI 5.3!), "1.234,5" -> 1234.5, "35" -> 35.0.

    QUAN TRONG: da phat hien qua sample thuc te (exception_data.md) rang khi
    dien tich > 1000 m2, site hien thi dang "5.300 m2"/"1.000 m2" - neu chi
    doi "," -> "." (cach lam ban dau) se doc SAI thanh 5.3/1.0 m2 - loi AM
    THAM (khong bao gio bi bat boi try/except vi van la so hop le, chi la
    SAI GIA TRI) - nen phai xu ly dung tu dau, khong the phat hien qua test
    thong thuong ma phai co sample thuc te moi lo ra.

    Tra None neu khong parse duoc (VD gap "KXĐ" - "Khong Xac Dinh").
    """
    text = clean_text_field(text)
    if not text:
        return None
    cleaned = text.replace(".", "").replace(",", ".").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_float_vn(text):
    """Chuyen chuoi kieu '4,65m' / '1.000m' / '12m' -> float (m). Tra None neu khong parse duoc."""
    text = clean_text_field(text)
    if not text:
        return None
    without_unit = text.lower().replace("m", "").strip()
    return parse_vn_number(without_unit)


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
    - Co the <img alt="check"> hoac src chua "check" -> True
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
# Core fields: title, posted_date, price, area, address (moi + cu), is_expired
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
        "area_is_undetermined": False,
        "address_street_new": None,
        "address_ward_new": None,
        "address_province_new": None,
        "address_old_raw": None,
        "is_expired": False,
    }

    # --- Tin het han: <div class="expired"><img src=".../expired.png" /></div> ---
    expired_el = article.find(class_="expired")
    result["is_expired"] = expired_el is not None

    # --- Title ---
    title_el = article.find(attrs={"itemprop": "name"})
    if title_el is not None:
        result["title"] = get_clean_text(title_el)

    # --- Ngay dang: uu tien attribute datetime, KHONG dung text ("Hom qua"...) ---
    time_el = article.find("time", attrs={"itemprop": "datePosted"})
    if time_el is not None and time_el.get("datetime"):
        result["posted_date"] = time_el["datetime"].strip()  # dang "YYYY-MM-DD"

    # --- Gia: so sach nam o attribute "value" cua the <data itemprop="price"> ---
    price_el = article.find(attrs={"itemprop": "price"})
    if price_el is not None:
        result["price_raw"] = price_el.get_text(strip=True)
        value_attr = price_el.get("value")
        if value_attr is not None:
            try:
                price_int = int(value_attr)
                if price_int == 0:
                    result["price_is_negotiable"] = True
                    result["price_vnd"] = None
                else:
                    result["price_vnd"] = price_int
            except ValueError:
                pass

    # --- Dien tich: so nam o itemprop="value" LONG BEN TRONG itemprop="floorSize" ---
    # LUU Y: dung parse_vn_number (KHONG phai float() truc tiep) vi dien tich
    # > 1000 m2 hien thi dang "5.300" - dau "." la hang nghin, khong phai
    # thap phan (xem exception_data.md). Rieng gia tri "KXĐ" (Khong Xac Dinh)
    # la 1 GIA TRI THAT do site ghi nhan, khong phai loi parse -> tach rieng
    # co area_is_undetermined thay vi de area_m2=None chung voi truong hop loi.
    area_wrap = article.find(attrs={"itemprop": "floorSize"})
    if area_wrap is not None:
        result["area_raw"] = get_clean_text(area_wrap)
        value_el = area_wrap.find(attrs={"itemprop": "value"})
        if value_el is not None:
            value_text = value_el.get_text(strip=True)
            if value_text.upper() in AREA_UNDETERMINED_MARKERS:
                result["area_is_undetermined"] = True
            else:
                result["area_m2"] = parse_vn_number(value_text)

    # --- Dia chi moi (itemprop="address", da sap nhap) ---
    address_el = article.find(attrs={"itemprop": "address"})
    if address_el is not None:
        street_el = address_el.find(attrs={"itemprop": "streetAddress"})
        ward_el = address_el.find(attrs={"itemprop": "addressLocality"})
        region_el = address_el.find(attrs={"itemprop": "addressRegion"})
        result["address_street_new"] = street_el.get_text(strip=True) if street_el else None
        result["address_ward_new"] = ward_el.get_text(strip=True) if ward_el else None
        result["address_province_new"] = region_el.get_text(strip=True) if region_el else None

    # --- Dia chi cu (p.old-address, text tho, chua tach - xu ly o Silver) ---
    old_address_el = article.find("p", class_="old-address")
    if old_address_el is not None:
        result["address_old_raw"] = get_clean_text(old_address_el)

    return result


def extract_enrichment_fields(article) -> dict:
    """
    Trich xuat bang key-value trong section.moreinfor1.
    So cot khong deu giua cac dong -> duyet theo cap lien tiep, khong gia
    dinh vi tri co dinh.
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
                raw[field] = value_cell
            else:
                raw[field] = get_clean_text(value_cell)

    return raw


def parse_one_listing(url: str, crawl_date, html_bytes, s3_key: str, source_part=None) -> dict:
    """Parse 1 record HTML -> dict field da chuan hoa, kem parse_status/parse_errors de QC."""
    errors = []
    record = {
        "listing_url": url,
        "listing_id": extract_listing_id_from_url(url),
        "crawl_date": crawl_date,
        "source_batch_s3_key": s3_key,
        "source_part": source_part,
    }

    try:
        if isinstance(html_bytes, (bytes, bytearray)):
            html_text = html_bytes.decode("utf-8", errors="replace")
        else:
            html_text = str(html_bytes)

        soup = BeautifulSoup(html_text, "lxml")

        article = soup.find("article", class_="property")
        if article is None:
            errors.append("khong_tim_thay_article.property")
            article = soup

        # --- Core fields ---
        core = extract_core_fields(article)
        record.update(core)

        if record["title"] is None:
            errors.append("missing_title")
        if record["price_vnd"] is None and not record["price_is_negotiable"]:
            errors.append("missing_price")
        if record["area_m2"] is None and not record["area_is_undetermined"]:
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
            value = parse_tribool(raw.get(bf))
            record[target] = value
            if value is False:
                # Theo exception_data.md: tren thuc te CHUA TUNG quan sat duoc
                # gia tri False THAT (chi co check-icon=True hoac "---"=None,
                # "khong co gia tri de phu nhan truc tiep"). Neu gap truong
                # hop nay, day la BIEN THE MOI can nguoi xem lai, khong phai
                # loi crawl - gan co QC thay vi am tham chap nhan.
                errors.append(f"unexpected_false_value_{target}")

        for required in ["property_type", "listing_type"]:
            if record.get(required) is None:
                errors.append(f"missing_{required}")

        table_id = raw.get("listing_id_from_table")
        if table_id and record["listing_id"] and table_id.strip() != record["listing_id"]:
            errors.append("listing_id_mismatch_url_vs_table")

        warning_el = article.find(class_="warning")
        record["has_warning"] = warning_el is not None
        record["warning_text"] = get_clean_text(warning_el) if warning_el else None

        record["parse_status"] = "ok" if not errors else "partial"

    except Exception as e:  # noqa: BLE001
        errors.append(f"exception:{type(e).__name__}:{e}")
        record["parse_status"] = "failed"
        defaults = [
            "title", "posted_date", "price_vnd", "price_is_negotiable", "price_raw",
            "area_m2", "area_raw", "area_is_undetermined", "address_street_new", "address_ward_new",
            "address_province_new", "address_old_raw", "is_expired", "listing_type",
            "property_type", "orientation", "legal_status", "project_name", "width_m",
            "length_m", "street_width_m", "floors", "bedrooms", "has_dining_room",
            "has_kitchen", "has_rooftop", "has_car_parking", "owner_direct",
            "has_warning", "warning_text",
        ]
        for f in defaults:
            record.setdefault(f, None)

    record["parse_errors"] = json.dumps(errors, ensure_ascii=False) if errors else None
    return record


# -----------------------------------------------------------------------------
# Doc du lieu Bronze tu S3 (cau truc moi: bronze/<date>/crawl-n/partN.parquet), dedup
# -----------------------------------------------------------------------------
def list_bronze_part_keys(s3_client, load_date: str) -> list:
    """
    LIET KE (khong tai noi dung) tat ca file partN.parquet cua 1 ngay
    (gom ca nhieu thu muc crawl-1, crawl-2,... neu chay lai trong ngay).
    Bo qua file manifest.json (khong khop regex PART_FILENAME_RE).

    Tra ve danh sach (part_number, s3_key) DA SAP XEP TANG DAN theo part_number.
    QUAN TRONG: S3 list_objects_v2 tra ve key theo THU TU CHU CAI
    ("part1","part10","part11",...,"part2",...) KHONG PHAI thu tu so -> neu
    khong sap xep lai, --limit se lay nham file (VD lay part10 truoc part2).
    """
    prefix = f"bronze/{load_date}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    found = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            m = PART_FILENAME_RE.search(obj["Key"])
            if m:
                found.append((int(m.group(1)), obj["Key"]))
    found.sort(key=lambda pair: pair[0])
    return found


def list_dataset_part_keys(s3_client, prefix: str) -> list:
    """Liet ke 77 part dataset da duoc dataset_loader ghi vao S3.

    Prefix mac dinh la ``bronze/dataset/`` va ten file dang ``part=N.parquet``.
    Chi chap nhan part 1..77 de tranh vo tinh nap file khac cung prefix.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    found = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            match = PART_FILENAME_RE.search(obj["Key"])
            if match:
                part_number = int(match.group(1))
                if 1 <= part_number <= TOTAL_DATASET_PARTS:
                    found.append((part_number, obj["Key"]))
    found.sort(key=lambda pair: pair[0])
    return found


def read_one_part(s3_client, s3_key: str) -> pd.DataFrame:
    """
    Doc DUY NHAT 1 file part tu S3 vao RAM. Tach rieng ham nay (thay vi gop
    tat ca cac part vao 1 DataFrame lon) de moi lan chi giu toi da ~10.000
    dong (1 part) trong bo nho, tranh OOM khi dataset co 77+ part x 10.000
    dong HTML tho (rat de vuot RAM neu doc gop het truoc).
    """
    resp = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
    buf = BytesIO(resp["Body"].read())
    return pd.read_parquet(buf)


def dedup_within_part(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dedup theo url TRONG PHAM VI 1 FILE part (hiem khi xay ra, nhung giu de
    an toan). Dedup GIUA CAC FILE khac nhau KHONG can lam o day - buoc
    upsert_staging() da tu xu ly qua dieu kien "chi ghi de neu crawl_date
    moi hon" (xem ham upsert_staging, mau ON CONFLICT ... WHERE EXCLUDED.
    crawl_date >= ...) -> khong can giu ca dataset trong RAM de dedup toan cuc.
    """
    before = len(df)
    df_sorted = df.sort_values("crawl_date")
    df_deduped = df_sorted.drop_duplicates(subset="url", keep="last").reset_index(drop=True)
    after = len(df_deduped)
    if after < before:
        print(f"[INFO]   Dedup trong part nay: {before} -> {after} dong (trung {before - after})")
    return df_deduped


# -----------------------------------------------------------------------------
# Load vao Postgres (upsert, idempotent: chay lai an toan, khong tao dup)
# -----------------------------------------------------------------------------
STAGING_COLUMNS = [
    "listing_url", "listing_id", "crawl_date", "source_part",
    "title", "posted_date", "is_expired",
    "price_vnd", "price_is_negotiable", "price_raw",
    "area_m2", "area_raw", "area_is_undetermined",
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
        INSERT INTO staging_old_v2.stg_listing_detail ({", ".join(STAGING_COLUMNS)})
        VALUES %s
        ON CONFLICT (listing_url) DO UPDATE SET
            {update_clause},
            loaded_at = now()
            WHERE EXCLUDED.crawl_date >= staging_old_v2.stg_listing_detail.crawl_date;
    """

    with psycopg2.connect(DW_DSN) as conn:
        with conn.cursor() as cur:
            pg_extras.execute_values(cur, sql, rows, page_size=500)
        conn.commit()

    print(f"[XONG] Da upsert {len(rows)} record vao staging_old_v2.stg_listing_detail")


def main():
    parser = argparse.ArgumentParser(description="Parse Bronze (S3) -> staging Postgres de QC")
    parser.add_argument(
        "--date",
        default=None,
        help="Ngay Bronze crawl cu can parse, dang yyyy-MM-dd; mac dinh dung dataset/",
    )
    parser.add_argument(
        "--prefix",
        default="bronze/dataset/",
        help="S3 prefix cua dataset 77 part (mac dinh: bronze/dataset/)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Gioi han TONG SO DONG se parse (dung khi kiem tra/chinh selector). "
             "Doc TUAN TU theo part_number tang dan, DUNG NGAY khi du so dong, "
             "KHONG doc them cac part con lai (tranh OOM khi test tren dataset lon).",
    )
    args = parser.parse_args()

    s3 = boto3.client("s3", region_name=AWS_REGION)

    if args.date:
        source_description = f"bronze/{args.date}/"
        print(f"[INFO] Dang liet ke Bronze parts cua ngay {args.date} tu S3 ...")
        part_keys = list_bronze_part_keys(s3, args.date)
    else:
        source_description = args.prefix
        print(f"[INFO] Dang liet ke {TOTAL_DATASET_PARTS} dataset parts tu S3 prefix {args.prefix} ...")
        part_keys = list_dataset_part_keys(s3, args.prefix)

    if not part_keys:
        print(f"[CANH BAO] Khong tim thay part nao tai {source_description}")
        return
    found_parts = {part_number for part_number, _ in part_keys}
    missing_parts = sorted(set(range(1, TOTAL_DATASET_PARTS + 1)) - found_parts)
    print(f"[INFO] Tim thay {len(part_keys)} file part, se xu ly TUAN TU theo thu tu tang dan.")
    if not args.date and missing_parts:
        print(f"[CANH BAO] Con thieu {len(missing_parts)} part dataset: {missing_parts}")

    overall_status_counter = {"ok": 0, "partial": 0, "failed": 0}
    rows_processed_total = 0
    parts_processed = 0

    # Xu ly TUNG PART MOT (khong gop tat ca vao 1 DataFrame lon) -> chi giu
    # toi da ~10.000 dong trong RAM tai 1 thoi diem, tranh loi realloc/OOM
    # da gap khi dataset co hang chuc/hang tram file x 10.000 dong HTML tho.
    for part_number, s3_key in part_keys:
        if args.limit is not None and rows_processed_total >= args.limit:
            remaining_files = len(part_keys) - parts_processed
            print(
                f"[INFO] Da du {args.limit} dong theo --limit, DUNG SOM "
                f"(con {remaining_files} file chua doc toi, dung y do)."
            )
            break

        print(f"[INFO] Dang doc {s3_key} (part {part_number}) ...")
        df_part = read_one_part(s3, s3_key)
        df_part["_s3_key"] = s3_key
        df_part["_source_part"] = part_number

        df_part = dedup_within_part(df_part)

        if args.limit is not None:
            remaining = args.limit - rows_processed_total
            if len(df_part) > remaining:
                df_part = df_part.head(remaining)

        records = []
        for _, row in df_part.iterrows():
            rec = parse_one_listing(
                row["url"], row["crawl_date"], row["html"], row["_s3_key"], row["_source_part"]
            )
            overall_status_counter[rec["parse_status"]] += 1
            records.append(rec)

        upsert_staging(records)  # ghi ngay xuong Postgres, KHONG giu records cua cac part truoc trong RAM

        rows_processed_total += len(df_part)
        parts_processed += 1

    print(
        f"\n[XONG] Da xu ly {parts_processed}/{len(part_keys)} file part, "
        f"tong {rows_processed_total} dong.\n"
        f"       Ket qua parse: {overall_status_counter}\n"
        f"       Da nap toan bo record da parse, khong loc theo listing_type."
    )


if __name__ == "__main__":
    main()
