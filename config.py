"""
config.py  (project root)

Cấu hình DÙNG CHUNG cho TOÀN BỘ project — Phương án B đã chốt: chỉ những
tham số thực sự dùng ở ≥2 package (hiện tại: crawler/ và parser/) mới đặt
ở đây. Tham số ĐẶC THÙ riêng của từng package (proxy, delay HTTP, cấu hình
Spark...) vẫn giữ nguyên trong crawler/config.py / parser/config.py,
không dồn hết về đây — tránh biến file này thành "bãi rác" tham số không
liên quan tới nhau.

Nội dung ở đây: DSN Postgres DW, S3 bucket Bronze, AWS region,
RUNNING_IN_CONTAINER — logic y hệt bản gốc trong crawler/config.py trước
khi tách (chuyển ra đây để parser/config.py dùng lại, không copy-paste
logic detect container/chọn DSN thêm 1 lần nữa).
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# .env nằm CÙNG cấp với chính file config.py này (project root) — không
# cần lùi cấp thư mục như crawler/config.py (file đó nằm trong crawler/,
# phải lùi 1 cấp mới tới root).
_ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH)


def _require(name: str) -> str:
    """Đọc 1 biến môi trường BẮT BUỘC — raise ngay với thông báo rõ ràng
    nếu thiếu, thay vì để lỗi lộ ra sau, sâu bên trong boto3/psycopg2."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Thiếu biến môi trường bắt buộc: {name}")
    return value


# ---------------------------------------------------------------------
# Database (Postgres DW) — 2 DSN riêng biệt CÙNG có mặt trong .env:
#   - POSTGRES_DW_DSN       : resolve qua tên service Docker, CHỈ dùng
#                             được BÊN TRONG container Airflow.
#   - POSTGRES_DW_DSN_LOCAL : dùng khi chạy trực tiếp trên máy host.
# .env luôn định nghĩa CẢ 2 biến cùng lúc -> PHẢI detect đúng ngữ cảnh
# đang chạy (RUNNING_IN_CONTAINER) để chọn đúng DSN, KHÔNG suy luận theo
# kiểu "biến nào tồn tại thì dùng biến đó".
# ---------------------------------------------------------------------
RUNNING_IN_CONTAINER = os.getenv("AIRFLOW_HOME") is not None


def get_postgres_dsn() -> str:
    """DSN đúng theo ngữ cảnh đang chạy — xem giải thích ở trên."""
    if RUNNING_IN_CONTAINER:
        return _require("POSTGRES_DW_DSN")
    return _require("POSTGRES_DW_DSN_LOCAL")


# ---------------------------------------------------------------------
# AWS S3 (Bronze layer)
# ---------------------------------------------------------------------
# KHÔNG re-export AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY ở đây — boto3
# (crawler) và Spark S3A qua EnvironmentVariableCredentialsProvider
# (parser, xem parser/config.py) đều tự đọc thẳng 2 biến chuẩn này từ
# os.environ, re-export thêm chỉ tăng diện lộ credential mà không mang
# lại lợi ích gì.
def get_s3_bucket() -> str:
    return _require("S3_BRONZE_BUCKET")


AWS_REGION = os.getenv("AWS_REGION", "us-east-1")