"""
config.py (project root)

Cấu hình dùng chung cho toàn project (≥2 package: crawler, parser).
Tham số đặc thù (proxy, Spark...) giữ riêng trong crawler/config.py hoặc parser/config.py.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

_ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH)


def _require(name: str) -> str:
    """Đọc biến môi trường bắt buộc, raise nếu thiếu."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Thiếu biến môi trường: {name}")
    return value


# Database (Postgres DW)
RUNNING_IN_CONTAINER = os.getenv("AIRFLOW_HOME") is not None

def get_postgres_dsn() -> str:
    """Trả về DSN đúng ngữ cảnh (container hoặc local)."""
    if RUNNING_IN_CONTAINER:
        return _require("POSTGRES_DW_DSN")
    return _require("POSTGRES_DW_DSN_LOCAL")


# AWS S3 (Bronze layer) — boto3/Spark tự đọc AWS_ACCESS_KEY_ID/SECRET từ os.environ.
def get_s3_bucket() -> str:
    return _require("S3_BRONZE_BUCKET")

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
