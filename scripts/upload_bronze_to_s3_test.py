"""
Script: upload_bronze_to_s3.py
Muc dich: Doc file parquet du lieu mau alonhadat da co san o local (data/raw/),
chia thanh nhieu batch nho, roi upload tung batch len S3 theo cau truc:

    s3://<bucket>/bronze/<yyyy-MM-dd>/batch-<n>/data.parquet
    s3://<bucket>/bronze/<yyyy-MM-dd>/batch-<n>/manifest.json

Nguyen tac thiet ke:
- Bronze layer la raw, append-only, immutable => KHONG dedup, KHONG transform
  o buoc nay. Viec dedup/convert de danh cho buoc parse_to_staging.py.
- "<yyyy-MM-dd>" la NGAY CHAY SCRIPT (ngay du lieu duoc dua vao Bronze, gio GMT+7),
  KHONG phai crawl_date nam trong tung dong du lieu (2 khai niem khac nhau:
  crawl_date la luc du lieu goc duoc crawl, con day la luc minh load vao kho).
- batch-n duoc danh so tiep noi cac batch da ton tai trong CUNG ngay tren S3,
  de chay lai script nhieu lan trong 1 ngay khong bi ghi de batch cu.

Cach chay:
    python upload_bronze_to_s3.py
    python upload_bronze_to_s3.py --file data/raw/alonhadat-10000-8digit.parquet --rows-per-batch 1000
"""

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET = os.environ["S3_BRONZE_BUCKET"]
GMT7 = timezone(timedelta(hours=7))

REQUIRED_COLUMNS = {"url", "crawl_date", "html"}


def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def get_next_batch_start(s3_client, date_prefix: str) -> int:
    """
    Kiem tra cac batch da ton tai trong ngay (date_prefix) tren S3,
    tra ve so thu tu batch tiep theo (tranh ghi de batch cu neu script
    duoc chay lai nhieu lan trong cung 1 ngay).
    """
    prefix = f"test/{date_prefix}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    existing_batches = set()
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            # cp['Prefix'] dang: test/2026-08-14/batch-3/
            folder = cp["Prefix"].rstrip("/").split("/")[-1]  # "batch-3"
            if folder.startswith("batch-"):
                try:
                    existing_batches.add(int(folder.split("-")[1]))
                except (IndexError, ValueError):
                    continue
    return max(existing_batches, default=0) + 1


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def upload_batches(local_path: Path, rows_per_batch: int):
    if not local_path.exists():
        sys.exit(f"[LOI] Khong tim thay file: {local_path.resolve()}")

    print(f"[INFO] Dang doc {local_path} ...")
    df = pd.read_parquet(local_path)
    total_rows = len(df)
    print(f"[INFO] Tong so record: {total_rows}")

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        sys.exit(f"[LOI] File parquet thieu cot bat buoc: {missing}")

    s3 = get_s3_client()
    load_date = datetime.now(GMT7).strftime("%Y-%m-%d")
    start_batch = get_next_batch_start(s3, load_date)
    n_batches = math.ceil(total_rows / rows_per_batch)

    print(f"[INFO] Ngay load (Bronze prefix): {load_date}")
    print(f"[INFO] Se tao {n_batches} batch, bat dau tu batch-{start_batch}")

    manifest_summary = []
    tmp_dir = Path("/tmp/bronze_upload")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    for i in range(n_batches):
        batch_no = start_batch + i
        chunk = df.iloc[i * rows_per_batch:(i + 1) * rows_per_batch].reset_index(drop=True)

        # --- Ghi file parquet tam cho batch nay ---
        local_tmp = tmp_dir / f"bronze_batch_{batch_no}.parquet"
        chunk.to_parquet(local_tmp, engine="pyarrow", compression="zstd", index=False)

        s3_key_data = f"test/{load_date}/batch-{batch_no}/data.parquet"
        s3_key_manifest = f"test/{load_date}/batch-{batch_no}/manifest.json"

        print(f"[INFO] Upload batch-{batch_no}: {len(chunk)} record -> s3://{S3_BUCKET}/{s3_key_data}")
        s3.upload_file(str(local_tmp), S3_BUCKET, s3_key_data)

        # --- Manifest de trace lai: nguon, checksum, thoi diem upload ---
        file_bytes = local_tmp.read_bytes()
        manifest = {
            "source_local_file": str(local_path),
            "batch_no": batch_no,
            "row_count": len(chunk),
            "row_range": [i * rows_per_batch, i * rows_per_batch + len(chunk) - 1],
            "uploaded_at_gmt7": datetime.now(GMT7).isoformat(),
            "sha256": sha256_bytes(file_bytes),
            "s3_key": s3_key_data,
        }
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key_manifest,
            Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

        manifest_summary.append(manifest)
        local_tmp.unlink()  # don file tam

    print(
        f"[XONG] Da upload {n_batches} batch, tong {total_rows} record len "
        f"s3://{S3_BUCKET}/test/{load_date}/"
    )
    return manifest_summary


def main():
    parser = argparse.ArgumentParser(description="Upload du lieu local len S3 Bronze layer")
    parser.add_argument(
        "--file",
        default="data/raw/alonhadat-10000-8digit.parquet",
        help="Duong dan file parquet local can upload",
    )
    parser.add_argument(
        "--rows-per-batch",
        type=int,
        default=1000,
        help="So dong moi batch (mac dinh 1000 -> 10 batch cho 10.000 record)",
    )
    args = parser.parse_args()
    upload_batches(Path(args.file), args.rows_per_batch)


if __name__ == "__main__":
    main()