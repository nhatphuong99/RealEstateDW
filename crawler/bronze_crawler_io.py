"""
Module: bronze_crawler_io.py
Cac ham IO THAT (goi CDN qua HTTP, upload S3, verify bang pyarrow) - duoc
"tiem" vao ham dieu phoi run_crawl() trong bronze_crawler_core.py.

Tach rieng khoi core de core co the unit-test khong can mang/AWS that
(xem tests/test_bronze_crawler_resume.py).

BUG DA SUA (2026-08-15): ham part_exists_on_source() truoc day BAT MOI
requests.RequestException va tra ve False -> khi mang co van de (DNS/timeout/
bi chan...), ham nay bao "part khong ton tai" GIONG HET truong hop that su
khong ton tai (404). Hau qua: discover_new_parts() dung ngay o lan probe DAU
TIEN, ca crawl-run "thanh cong" ma xu ly 0 part, KHONG co canh bao gi.
Sua: phan biet RO RANG 404 (that su khong ton tai) voi MOI loai loi khac
(khong biet, phai bao loi ro rang bang PartCheckError).

DA DON DEP (2026-08-16): bo hoan toan viec tao "<part>.parquet.manifest.json"
tren S3 khi upload - kiem tra lai cho thay KHONG co code nao doc lai file nay
(parse_to_staging.py chu dong bo qua no; reconcile_missing_storage_objects()
chi kiem tra file .parquet). Toan bo noi dung cua no (part_number, source_url,
file_size_bytes, sha256, uploaded_at, s3_key) da duoc luu THANG vao Postgres
qua mark_success() ngay trong cung 1 lan xu ly - giu ca 2 ban la du thua,
tang chi phi S3 API call ma khong tang do tin cay.
"""

from __future__ import annotations

import hashlib
import io
import os

import boto3
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.environ["S3_BRONZE_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
SOURCE_URL_TEMPLATE = "https://cdn.cuhuuhoang.com/alonhadat/part{n}.parquet"
REQUEST_TIMEOUT_SEC = 30

# Mot so CDN chan/tra ve khac thuong voi request KHONG co User-Agent (vi thu
# vien requests mac dinh gui "python-requests/x.x" - de bi nhan dien la bot).
# Luon gui User-Agent ro rang, giong trinh duyet thong thuong.
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RealEstateDW-BronzeCrawler/1.0; +hoc-tap-ca-nhan)"
}

# Cac part DA BIET TRUOC so dong (part 1-76 = 10.000 dong, part 77 = 4.212 dong).
# Part MOI phat hien sau nay (78, 79, ...) se KHONG co trong dict nay -> bo qua
# kiem tra so dong tuyet doi, chi kiem tra file doc duoc va co it nhat 1 dong.
KNOWN_EXPECTED_ROWS = {i: 10000 for i in range(1, 77)}
KNOWN_EXPECTED_ROWS[77] = 4212


class PartCheckError(Exception):
    """
    Loi khi KHONG THE xac dinh CHAC CHAN 1 part co ton tai tren nguon hay
    khong (loi mang, DNS, timeout, status code bat thuong nhu 403/500/...).

    KHAC VOI truong hop XAC DINH RO RANG la khong ton tai (HTTP 404) - truong
    hop do part_exists_on_source() tra ve False binh thuong, KHONG raise loi nay.

    Phan biet nay QUAN TRONG: neu gop chung "loi mang" voi "khong ton tai",
    discover_new_parts() se dung ngay o lan probe DAU TIEN khi mang co van de
    TAM THOI, va NGHI SAI la nguon khong co part nao - day chinh la nguyen
    nhan gay ra loi "0 part duoc xu ly, khong canh bao gi" da gap phai.
    """


def part_exists_on_source(part_number: int) -> bool:
    """
    Kiem tra 1 part co ton tai tren CDN khong.

    DA XAC NHAN QUA THUC NGHIEM (2026-08-15): CDN nay (cdn.cuhuuhoang.com,
    dung sau Cloudflare) tra ve "401 Unauthorized" cho request HEAD - SAI
    CHUAN HTTP (dung ra phai la "405 Method Not Allowed" neu khong ho tro
    HEAD, hoac "200"/"404" neu co ho tro) - trong khi GET tren CUNG URL do
    lai tra ve dung "200" binh thuong. Vi vay KHONG dung HEAD cho CDN nay.

    Thay vao do, dung GET kem header "Range: bytes=0-0" de CHI tai 1 byte
    dau tien (response header co "accept-ranges: bytes" -> CDN co ho tro
    Range request) - vua kiem tra duoc ton tai, vua khong phai tai nguyen
    ca file (~40MB/part) chi de kiem tra.

    - HTTP 200 hoac 206 (Partial Content)  -> True (ton tai)
    - HTTP 404                              -> False (XAC DINH RO RANG khong ton tai)
    - Bat ky truong hop khac                -> RAISE PartCheckError (khong biet,
      KHONG duoc mac dinh la "khong ton tai")
    """
    url = SOURCE_URL_TEMPLATE.format(n=part_number)
    headers = {**DEFAULT_HEADERS, "Range": "bytes=0-0"}
    try:
        with requests.get(url, timeout=REQUEST_TIMEOUT_SEC, stream=True, headers=headers) as resp:
            if resp.status_code == 404:
                return False
            if resp.status_code in (200, 206):
                return True
            raise PartCheckError(
                f"GET (range-probe) part {part_number} tra status khong xac dinh: "
                f"{resp.status_code} (khong phai 200/206/404) - can kiem tra thu cong."
            )
    except requests.RequestException as e:
        raise PartCheckError(
            f"Loi ket noi khi kiem tra part {part_number} (GET range-probe {url}): "
            f"{type(e).__name__}: {e}"
        ) from e


def download_part(part_number: int) -> bytes:
    """
    Tai NGUYEN BYTE cua file parquet, KHONG doc/parse/convert gi ca.
    Dung yeu cau: khong convert cot html sang Base64 -> khong dung
    pandas.read_parquet()/to_parquet() o buoc nay (khong re-serialize).
    """
    url = SOURCE_URL_TEMPLATE.format(n=part_number)
    resp = requests.get(url, timeout=REQUEST_TIMEOUT_SEC, stream=True, headers=DEFAULT_HEADERS)
    resp.raise_for_status()  # raise HTTPError ro rang neu status khong phai 2xx
    chunks = []
    for chunk in resp.iter_content(chunk_size=1024 * 1024):
        if chunk:
            chunks.append(chunk)
    content = b"".join(chunks)
    if not content:
        raise ValueError(f"Tai part {part_number} nhung noi dung rong (0 byte)")
    return content


def verify_part(part_number: int, content: bytes) -> dict:
    """
    Doc METADATA cua file parquet (khong doc data, khong dung toi cot html)
    de xac nhan file khong bi hong/tai dang. Neu part nam trong
    KNOWN_EXPECTED_ROWS, doi chieu so dong; neu la part MOI phat hien, chi
    kiem tra file doc duoc va khong rong.
    """
    pf = pq.ParquetFile(io.BytesIO(content))
    actual_rows = pf.metadata.num_rows

    schema_cols = set(pf.schema_arrow.names)
    required_cols = {"url", "crawl_date", "html"}
    missing_cols = required_cols - schema_cols
    if missing_cols:
        raise ValueError(f"File thieu cot bat buoc: {missing_cols}")

    if actual_rows == 0:
        raise ValueError("File parquet rong (0 dong)")

    expected = KNOWN_EXPECTED_ROWS.get(part_number)
    if expected is not None and actual_rows != expected:
        raise ValueError(f"So dong khong khop: ky vong {expected}, thuc te {actual_rows}")

    return {"actual_rows": actual_rows}


def upload_part(part_number: int, content: bytes, *, load_date: str, crawl_no: int) -> dict:
    """
    Upload NGUYEN BYTE file goc len S3 (khong doc bang pandas/pyarrow.write lai
    -> giu dung dinh dang/encoding tu nguon).

    KHONG tao file manifest.json rieng nua (da bo - xem CHANGELOG 2026-08-16):
    toan bo thong tin manifest cu (part_number, source_url, file_size_bytes,
    sha256, uploaded_at, s3_key) da duoc luu THANG vao Postgres
    (crawl.dataset_part_queue) ngay trong cung 1 lan goi mark_success() -
    khong co code nao doc lai manifest.json tren S3, nen no chi la 1 lan
    put_object thua, tang chi phi ma khong mang lai gia tri.
    """
    s3 = boto3.client("s3", region_name=AWS_REGION)

    file_name = f"part{part_number}.parquet"
    s3_key_data = f"bronze/{load_date}/crawl-{crawl_no}/{file_name}"

    sha256 = hashlib.sha256(content).hexdigest()

    s3.put_object(Bucket=S3_BUCKET, Key=s3_key_data, Body=content)

    return {"s3_key": s3_key_data, "sha256": sha256}


def s3_object_exists(s3_key: str) -> bool:
    """
    Kiem tra 1 object co THAT SU con ton tai tren S3 khong - dung cho
    reconcile_missing_storage_objects() trong bronze_crawler_core.py, de
    phat hien truong hop ai do XOA FILE S3 TRUC TIEP (VD tren S3 console)
    trong khi Postgres van con ghi status='success' cho part do.

    - Object ton tai (head_object thanh cong)     -> True
    - 404/NoSuchKey (XAC DINH RO RANG khong con)   -> False
    - Loi khac (VD mat quyen truy cap tam thoi,
      loi mang...)                                 -> RAISE, KHONG duoc mac
      dinh coi la "da bi xoa" (tranh false positive lam mat cong tai lai
      file van con nguyen, chi vi 1 loi quyen truy cap thoang qua).
    """
    from botocore.exceptions import ClientError

    s3 = boto3.client("s3", region_name=AWS_REGION)
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        return True
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchKey"):
            return False
        raise
