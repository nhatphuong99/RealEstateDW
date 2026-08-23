"""
crawler/config.py

Cấu hình dùng chung cho toàn bộ crawler. Đọc biến môi trường từ `.env`
(python-dotenv) để không hard-code credentials, đúng nguyên tắc bảo mật
đã đặt ra từ GĐ2 của đồ án.

Mọi module khác (web_crawler_io.py, proxy_manager.py, dags/...) import
từ đây thay vì tự đọc `os.environ` rải rác. `web_crawler_core.py` (logic
thuần, không I/O) KHÔNG import module này — core chỉ nhận `CrawlerConfig`
đã dựng sẵn qua tham số, giữ đúng nguyên tắc tách biệt logic khỏi I/O.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Tự tìm .env ở gốc project (2 cấp trên file này: crawler/ -> project root)
# — CỐ ĐỊNH theo vị trí file, đúng bất kể thư mục làm việc (cwd) lúc chạy
# là gì (khác với load_dotenv() mặc định dò theo cwd, có thể không tìm
# thấy .env nếu Airflow worker/script chạy từ thư mục khác project root).
_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH)


def _require(name: str) -> str:
    """Đọc 1 biến môi trường BẮT BUỘC — raise ngay với thông báo rõ ràng
    nếu thiếu, thay vì để lỗi lộ ra sau, sâu bên trong boto3/psycopg2."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Thiếu biến môi trường bắt buộc: {name}")
    return value


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw else default


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw else default


# ---------------------------------------------------------------------
# Database (Postgres DW) — 2 DSN riêng biệt CÙNG có mặt trong .env:
#   - POSTGRES_DW_DSN        : resolve qua tên service Docker, CHỈ dùng
#                               được BÊN TRONG container Airflow
#   - POSTGRES_DW_DSN_LOCAL  : dùng khi chạy trực tiếp trên máy host
#                               (VD script test/debug ngoài Docker)
# QUAN TRỌNG: vì .env luôn định nghĩa CẢ 2 biến cùng lúc, KHÔNG được chọn
# theo kiểu "biến nào đang tồn tại thì dùng biến đó" — khi chạy ngoài
# Docker, load_dotenv() vẫn nạp luôn POSTGRES_DW_DSN (trỏ tới hostname
# "postgres-dw" không resolve được trên host) nên sẽ chọn NHẦM nếu không
# detect đúng ngữ cảnh đang chạy trong hay ngoài container.
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
# KHÔNG re-export AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY ở đây — boto3 tự
# đọc thẳng các biến chuẩn này từ os.environ (đúng chuẩn AWS SDK), re-export
# thêm 1 chỗ chỉ tăng diện lộ credential mà không mang lại lợi ích gì.
def get_s3_bucket() -> str:
    return _require("S3_BRONZE_BUCKET")


AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


# ---------------------------------------------------------------------
# HTTP fetch (RequestsPageFetcher)
# ---------------------------------------------------------------------
CONNECT_TIMEOUT_SECONDS = _float("WEB_CRAWLER_CONNECT_TIMEOUT_SECONDS", 10.0)
READ_TIMEOUT_SECONDS = _float("WEB_CRAWLER_READ_TIMEOUT_SECONDS", 30.0)


# ---------------------------------------------------------------------
# Proxy (proxy_manager.py)
# ---------------------------------------------------------------------
# Lọc bớt ngay từ vòng health-check những proxy tuy "sống"
# nhưng phản hồi chậm — dấu hiệu quá tải/băng thông kém.
PROXY_HEALTH_CHECK_TIMEOUT_SECONDS = _float("PROXY_HEALTH_CHECK_TIMEOUT_SECONDS", 4.0)
PROXY_HEALTH_CHECK_WORKERS = _int("PROXY_HEALTH_CHECK_WORKERS", 20)
PROXY_MAX_CANDIDATES = _int("PROXY_MAX_CANDIDATES", 200)


# ---------------------------------------------------------------------
# Vòng lặp crawl chính (WebCrawlerCore.CrawlerConfig) — Nhóm B, DAG 2
# ---------------------------------------------------------------------
WEB_CRAWLER_MAX_DETAIL_PAGES_PER_RUN = _int("WEB_CRAWLER_MAX_DETAIL_PAGES_PER_RUN", 1000)
WEB_CRAWLER_TIME_BOX_SECONDS = _int("WEB_CRAWLER_TIME_BOX_SECONDS", 45 * 60)
WEB_CRAWLER_DELAY_MIN_SECONDS = _float("WEB_CRAWLER_DELAY_MIN_SECONDS", 5.0)
WEB_CRAWLER_DELAY_MAX_SECONDS = _float("WEB_CRAWLER_DELAY_MAX_SECONDS", 10.0)
# Ngân sách retry CÙNG 1 proxy — chỉ áp dụng cho lỗi mạng/server chung
# chung (FETCH_ERROR). Lỗi liên quan proxy (chết/treo/429/CAPTCHA) KHÔNG
# có ngân sách riêng: đổi proxy ngay, lặp tới khi hết proxy trong pool
# (quyết định 2026-08-19).
WEB_CRAWLER_MAX_FETCH_ERROR_RETRIES = _int("WEB_CRAWLER_MAX_FETCH_ERROR_RETRIES", 3)
WEB_CRAWLER_FLUSH_INTERVAL_SECONDS = _int("WEB_CRAWLER_FLUSH_INTERVAL_SECONDS", 10 * 60)
WEB_CRAWLER_FLUSH_PAGE_THRESHOLD = _int("WEB_CRAWLER_FLUSH_PAGE_THRESHOLD", 100)
# Ngưỡng "đủ dữ liệu để coi là thành công" (quyết định 2026-08-19): trigger
# flush sớm khi vừa đạt đủ số trang này lần đầu, VÀ là điều kiện để
# run_dag2() coi 1 run dừng bất thường (fetch_error/proxy_exhausted) vẫn
# là THÀNH CÔNG nếu đã kịp crawl đủ số trang này trước khi dừng.
WEB_CRAWLER_MIN_SUCCESS_PAGES = _int("WEB_CRAWLER_MIN_SUCCESS_PAGES", 10)


# ---------------------------------------------------------------------
# Nhóm A — DAG 1 (dataset_loader / dataset_loader_core.py)
# ---------------------------------------------------------------------
# Không cần biến DATASET_TOTAL_PARTS ở đây — con số 77 part là hằng số
# NGHIỆP VỤ cố định (CDN đã xác nhận), không phải tham số môi trường có
# thể thay đổi giữa các lần chạy -> để nguyên trong dataset_loader_core.py
# (TOTAL_PARTS), tránh khai báo trùng 2 nơi.
DATASET_CDN_BASE_URL = os.getenv(
    "DATASET_CDN_BASE_URL", "https://cdn.cuhuuhoang.com/alonhadat"
)
DATASET_S3_PREFIX = os.getenv("DATASET_S3_PREFIX", "bronze/dataset/")
# Probe (GET Range: bytes=0-0) chỉ cần 1 byte đầu -> về lý thuyết rất
# nhanh, nhưng 10s ban đầu không đủ buffer khi nhiều Task Instance chạy
# song song (max_active_tis_per_dag) cùng hit CDN -> tăng lên 20s
# (quyết định sau khi thấy ReadTimeout thật ở part 36 khi chạy 10 song song).
DATASET_PROBE_TIMEOUT_SECONDS = _float("DATASET_PROBE_TIMEOUT_SECONDS", 20.0)
# Download full file (part lớn nhất ~10.000 dòng) -> timeout dài hơn để
# chịu được mạng chậm tới CDN, khác hẳn timeout ngắn của DAG 2 (trang HTML nhỏ).
DATASET_DOWNLOAD_TIMEOUT_SECONDS = _float("DATASET_DOWNLOAD_TIMEOUT_SECONDS", 60.0)
DATASET_REQUEST_DELAY_SECONDS = _float("DATASET_REQUEST_DELAY_SECONDS", 2.0)
DATASET_MAX_ACTIVE_TASKS = _int("DATASET_MAX_ACTIVE_TASKS", 2)
