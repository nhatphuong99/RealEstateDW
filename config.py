"""
config.py (project root)

Cấu hình dùng chung cho toàn project. Chỉ đặt tham số dùng ở ≥2 package
(crawler, parser). Tham số đặc thù (proxy, Spark...) giữ riêng trong
crawler/config.py hoặc parser/config.py. Nội dung: DSN Postgres DW,
S3 bucket Bronze, AWS region, RUNNING_IN_CONTAINER.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# .env nằm cùng cấp với file config.py (project root).
_ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH)


def _require(name: str) -> str:
    """Đọc biến môi trường bắt buộc, raise nếu thiếu."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Thiếu biến môi trường: {name}")
    return value


# Database (Postgres DW) — .env luôn có cả DSN trong/ngoài container.
RUNNING_IN_CONTAINER = os.getenv("AIRFLOW_HOME") is not None

def get_postgres_dsn() -> str:
    """Trả về DSN đúng ngữ cảnh (container hoặc local)."""
    if RUNNING_IN_CONTAINER:
        return _require("POSTGRES_DW_DSN")
    return _require("POSTGRES_DW_DSN_LOCAL")


# AWS S3 (Bronze layer)
# Không re-export AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY — boto3/Spark tự đọc từ os.environ.
def get_s3_bucket() -> str:
    return _require("S3_BRONZE_BUCKET")

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
