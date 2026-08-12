"""
Lưu/đọc HTML thô nén .gz trên S3 (Bronze layer).
Bronze là immutable/append-only theo đúng Medallion Architecture — không
có hàm update/delete ở đây, chỉ có save (write-once) và load (read).
"""
import gzip
import uuid
from datetime import date

import boto3

from crawler import config

_s3_client = None


def _get_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client(
            "s3",
            region_name=config.AWS_REGION,
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        )
    return _s3_client


def build_key(category: str, page_num: int, run_id: str, crawl_date: date = None) -> str:
    crawl_date = crawl_date or date.today()
    return (
        f"bronze/{crawl_date.isoformat()}/{category}/"
        f"page_{page_num}_{run_id}.html.gz"
    )


def save_gz(category: str, page_num: int, content: bytes, run_id: str = None) -> str:
    """Nén content bằng gzip rồi upload lên S3, trả về key đã lưu."""
    run_id = run_id or uuid.uuid4().hex[:8]
    key = build_key(category, page_num, run_id)
    compressed = gzip.compress(content)

    client = _get_client()
    client.put_object(
        Bucket=config.S3_BRONZE_BUCKET,
        Key=key,
        Body=compressed,
        ContentType="text/html",
        ContentEncoding="gzip",
    )
    return key


def load_gz(s3_key: str) -> bytes:
    """Tải file .gz từ S3 và giải nén, trả về HTML gốc (bytes)."""
    client = _get_client()
    obj = client.get_object(Bucket=config.S3_BRONZE_BUCKET, Key=s3_key)
    compressed = obj["Body"].read()
    return gzip.decompress(compressed)
