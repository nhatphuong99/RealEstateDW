"""
Lưu HTML thô lên S3 (Bronze layer) — dùng boto3.

Lưu THẲNG LÊN S3 vì:
    - Bronze phải immutable/bền vững — đĩa trong container mất khi
      container bị recreate (Docker Compose không đảm bảo volume trừ khi
      khai báo riêng, và S3 đã có sẵn trong kiến trúc hiện tại).
    - Tách fetch (DAG crawl) khỏi parse (DAG parse) hoàn toàn độc lập về
      hạ tầng — DAG parse có thể chạy trên máy/container khác mà vẫn đọc
      được dữ liệu.
"""
import gzip
import hashlib
from datetime import datetime, timezone

import boto3

from . import config

_s3 = boto3.client("s3")


def save_raw_html(url: str, html: str, category: str) -> str:
    """Nén gzip và lưu HTML thô, trả về S3 key đã lưu.
    Tên file = hash ngắn của URL (tránh ký tự đặc biệt / độ dài URL khi
    đặt làm tên file, và tránh trùng tên giữa các tin khác nhau)."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url_hash = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    key = f"{config.S3_PREFIX}/{category}/{today}/{url_hash}.html.gz"

    body = gzip.compress(html.encode("utf-8"))
    _s3.put_object(
        Bucket=config.S3_BUCKET,
        Key=key,
        Body=body,
        ContentType="text/html",
        ContentEncoding="gzip",
    )
    return key


def load_raw_html(key: str) -> str:
    """Tải và giải nén HTML thô từ S3."""
    obj = _s3.get_object(Bucket=config.S3_BUCKET, Key=key)
    return gzip.decompress(obj["Body"].read()).decode("utf-8")
