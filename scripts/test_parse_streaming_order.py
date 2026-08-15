"""
Test: test_parse_streaming_order.py
Kiem tra 2 dieu quan trong cua parse_to_staging.py (sua sau khi gap loi OOM
thuc te khi doc gop toan bo Bronze parts vao 1 DataFrame):

1. Doc part THEO DUNG THU TU SO TANG DAN (1, 2, 3, 10, ...), du S3 tra ve
   danh sach theo THU TU CHU CAI ("part1","part10","part2",...).
2. Khi co --limit, DUNG SOM ngay khi du so dong - KHONG tai them file
   khong can thiet (day chinh la nguyen nhan gay OOM truoc do: code cu
   tai het moi file roi moi cat --limit).

Gia lap toan bo S3 (boto3) va Postgres (upsert_staging) trong RAM - KHONG
can AWS/Postgres that. Cach chay: python tests/test_parse_streaming_order.py
"""

import io
import os
import sys

# Set bien moi truong GIA (chi de module import duoc, khong ket noi that)
os.environ.setdefault("S3_BRONZE_BUCKET", "fake-bucket")
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("POSTGRES_DW_DSN_LOCAL", "postgresql://fake:fake@localhost:5433/fake")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import pandas as pd  # noqa: E402

import parse_to_staging as pts  # noqa: E402


# -----------------------------------------------------------------------------
# Gia lap S3 client (boto3) - chi can 2 method ma code that su dung:
# get_paginator("list_objects_v2") va get_object(Bucket, Key)
# -----------------------------------------------------------------------------
class FakePaginator:
    def __init__(self, keys_in_bucket_order):
        self.keys_in_bucket_order = keys_in_bucket_order

    def paginate(self, Bucket, Prefix):
        contents = [{"Key": k} for k in self.keys_in_bucket_order if k.startswith(Prefix)]
        yield {"Contents": contents}


class FakeS3Client:
    def __init__(self, parquet_bytes_by_key: dict, keys_in_bucket_order: list):
        self.parquet_bytes_by_key = parquet_bytes_by_key
        self.keys_in_bucket_order = keys_in_bucket_order
        self.get_object_calls = []  # ghi lai DUNG THU TU cac key da doc, de assert

    def get_paginator(self, op_name):
        assert op_name == "list_objects_v2"
        return FakePaginator(self.keys_in_bucket_order)

    def get_object(self, Bucket, Key):
        self.get_object_calls.append(Key)
        return {"Body": io.BytesIO(self.parquet_bytes_by_key[Key])}


def make_fake_part_bytes(part_number: int, n_rows: int = 3) -> bytes:
    """Tao du lieu parquet toi thieu (khong can HTML that) de test luong doc/ghep."""
    df = pd.DataFrame({
        "url": [f"https://alonhadat.com.vn/tin-p{part_number}-{i}-{1000*part_number+i}.html"
                for i in range(n_rows)],
        "crawl_date": pd.to_datetime([f"2026-08-15T00:00:{i:02d}" for i in range(n_rows)]),
        "html": [b"<article class='property'></article>" for _ in range(n_rows)],
    })
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow")
    return buf.getvalue()


def test_doc_dung_thu_tu_so_tang_dan_du_s3_tra_ve_theo_chu_cai():
    # S3 GIA co "hanh vi that": tra ve theo thu tu CHU CAI (1,10,2,3)
    keys_in_bucket_order = [
        "bronze/2026-08-15/crawl-1/part1.parquet",
        "bronze/2026-08-15/crawl-1/part10.parquet",
        "bronze/2026-08-15/crawl-1/part2.parquet",
        "bronze/2026-08-15/crawl-1/part3.parquet",
    ]
    parquet_bytes = {
        "bronze/2026-08-15/crawl-1/part1.parquet": make_fake_part_bytes(1),
        "bronze/2026-08-15/crawl-1/part10.parquet": make_fake_part_bytes(10),
        "bronze/2026-08-15/crawl-1/part2.parquet": make_fake_part_bytes(2),
        "bronze/2026-08-15/crawl-1/part3.parquet": make_fake_part_bytes(3),
    }
    fake_s3 = FakeS3Client(parquet_bytes, keys_in_bucket_order)

    part_keys = pts.list_bronze_part_keys(fake_s3, "2026-08-15")

    assert part_keys == [
        (1, "bronze/2026-08-15/crawl-1/part1.parquet"),
        (2, "bronze/2026-08-15/crawl-1/part2.parquet"),
        (3, "bronze/2026-08-15/crawl-1/part3.parquet"),
        (10, "bronze/2026-08-15/crawl-1/part10.parquet"),
    ], f"Thu tu sai: {part_keys}"

    print("PASS: test_doc_dung_thu_tu_so_tang_dan_du_s3_tra_ve_theo_chu_cai")


def test_dung_som_khi_du_limit_khong_tai_file_thua():
    keys_in_bucket_order = [
        "bronze/2026-08-15/crawl-1/part1.parquet",
        "bronze/2026-08-15/crawl-1/part10.parquet",
        "bronze/2026-08-15/crawl-1/part2.parquet",
        "bronze/2026-08-15/crawl-1/part3.parquet",
    ]
    parquet_bytes = {
        "bronze/2026-08-15/crawl-1/part1.parquet": make_fake_part_bytes(1, n_rows=3),
        "bronze/2026-08-15/crawl-1/part10.parquet": make_fake_part_bytes(10, n_rows=3),
        "bronze/2026-08-15/crawl-1/part2.parquet": make_fake_part_bytes(2, n_rows=3),
        "bronze/2026-08-15/crawl-1/part3.parquet": make_fake_part_bytes(3, n_rows=3),
    }
    fake_s3 = FakeS3Client(parquet_bytes, keys_in_bucket_order)

    # Gia lap upsert_staging: ghi lai record thay vi ghi that vao Postgres
    upserted_batches = []

    def fake_upsert_staging(records):
        upserted_batches.append(records)

    # Monkeypatch: thay boto3.client va upsert_staging trong module bang ban gia lap
    original_boto3_client = pts.boto3.client
    original_upsert = pts.upsert_staging
    pts.boto3.client = lambda *a, **kw: fake_s3
    pts.upsert_staging = fake_upsert_staging

    # Gia lap argv: python parse_to_staging.py --date 2026-08-15 --limit 7
    original_argv = sys.argv
    sys.argv = ["parse_to_staging.py", "--date", "2026-08-15", "--limit", "7"]

    try:
        pts.main()
    finally:
        pts.boto3.client = original_boto3_client
        pts.upsert_staging = original_upsert
        sys.argv = original_argv

    # part1 (3 dong) + part2 (3 dong) = 6, con thieu 1 -> doc them part3,
    # CAT xuong con 1 dong -> tong dung 7. TUYET DOI KHONG duoc doc part10.
    assert fake_s3.get_object_calls == [
        "bronze/2026-08-15/crawl-1/part1.parquet",
        "bronze/2026-08-15/crawl-1/part2.parquet",
        "bronze/2026-08-15/crawl-1/part3.parquet",
    ], f"Da tai nham file (le ra KHONG duoc dung toi part10): {fake_s3.get_object_calls}"

    total_records = sum(len(batch) for batch in upserted_batches)
    assert total_records == 7, f"Ky vong dung 7 record theo --limit, thuc te {total_records}"

    # part3 phai bi CAT xuong con 1 dong (vi chi con thieu 1 de du 7)
    assert len(upserted_batches[-1]) == 1, \
        f"Part cuoi cung (part3) phai chi con 1 dong sau khi cat theo limit, thuc te {len(upserted_batches[-1])}"

    print("PASS: test_dung_som_khi_du_limit_khong_tai_file_thua")


if __name__ == "__main__":
    test_doc_dung_thu_tu_so_tang_dan_du_s3_tra_ve_theo_chu_cai()
    test_dung_som_khi_du_limit_khong_tai_file_thua()
    print("\nTAT CA TEST DEU PASS.")
