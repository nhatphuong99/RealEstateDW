"""
crawler/config.py

Cấu hình ĐẶC THÙ cho package crawler (Nhóm A + Nhóm B, DAG 1 & DAG 2).
Tham số DÙNG CHUNG với parser/ (DSN Postgres, S3 bucket, AWS region,
RUNNING_IN_CONTAINER) import lại từ config.py gốc — KHÔNG định nghĩa
trùng, đúng "Phương án B" đã chốt (xem config.py ở project root).

Mọi module khác trong crawler/ (web_crawler_io.py, dataset_loader_io.py,
proxy_manager.py, dags/...) import từ đây thay vì tự đọc os.environ rải
rác. web_crawler_core.py/dataset_loader_core.py (logic thuần, không I/O)
KHÔNG import module này — core chỉ nhận CrawlerConfig đã dựng sẵn qua
tham số, giữ đúng nguyên tắc tách biệt logic khỏi I/O.
"""

import importlib.util
import os
from pathlib import Path

from dotenv import load_dotenv

# Tự tìm .env ở gốc project (2 cấp trên file này: crawler/ -> project root)
# — CỐ ĐỊNH theo vị trí file, đúng bất kể thư mục làm việc (cwd) lúc chạy
# là gì (khác với load_dotenv() mặc định dò theo cwd, có thể không tìm
# thấy .env nếu Airflow worker/script chạy từ thư mục khác project root).
_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH)


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw else default


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw else default


# ---------------------------------------------------------------------
# Import lại config DÙNG CHUNG từ config.py gốc (project root) — qua
# đường dẫn tuyệt đối bằng importlib, KHÔNG `import config` trực tiếp vì
# phụ thuộc PYTHONPATH/cwd lúc chạy (khác nhau giữa host và trong
# container Airflow). Cùng pattern với parser/config.py.
# ---------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ROOT_CONFIG_PATH = _PROJECT_ROOT / "config.py"

_spec = importlib.util.spec_from_file_location("root_config", _ROOT_CONFIG_PATH)
if _spec is None or _spec.loader is None:
    raise RuntimeError(f"Không tìm thấy config.py gốc tại: {_ROOT_CONFIG_PATH}")
root_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(root_config)

get_postgres_dsn = root_config.get_postgres_dsn
get_s3_bucket = root_config.get_s3_bucket
RUNNING_IN_CONTAINER = root_config.RUNNING_IN_CONTAINER
AWS_REGION = root_config.AWS_REGION


# ---------------------------------------------------------------------
# HTTP fetch (RequestsPageFetcher)
# ---------------------------------------------------------------------
CONNECT_TIMEOUT_SECONDS = _float("WEB_CRAWLER_CONNECT_TIMEOUT_SECONDS", 10.0)
READ_TIMEOUT_SECONDS = _float("WEB_CRAWLER_READ_TIMEOUT_SECONDS", 30.0)


# ---------------------------------------------------------------------
# Proxy (proxy_manager.py)
# ---------------------------------------------------------------------
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
WEB_CRAWLER_MAX_FETCH_ERROR_RETRIES = _int("WEB_CRAWLER_MAX_FETCH_ERROR_RETRIES", 3)
WEB_CRAWLER_FLUSH_INTERVAL_SECONDS = _int("WEB_CRAWLER_FLUSH_INTERVAL_SECONDS", 10 * 60)
WEB_CRAWLER_FLUSH_PAGE_THRESHOLD = _int("WEB_CRAWLER_FLUSH_PAGE_THRESHOLD", 100)
WEB_CRAWLER_MIN_SUCCESS_PAGES = _int("WEB_CRAWLER_MIN_SUCCESS_PAGES", 10)


# ---------------------------------------------------------------------
# Nhóm A — DAG 1 (dataset_loader / dataset_loader_core.py)
# ---------------------------------------------------------------------
DATASET_CDN_BASE_URL = os.getenv(
    "DATASET_CDN_BASE_URL", "https://cdn.cuhuuhoang.com/alonhadat"
)
DATASET_S3_PREFIX = os.getenv("DATASET_S3_PREFIX", "bronze/dataset/")
DATASET_PROBE_TIMEOUT_SECONDS = _float("DATASET_PROBE_TIMEOUT_SECONDS", 20.0)
DATASET_DOWNLOAD_TIMEOUT_SECONDS = _float("DATASET_DOWNLOAD_TIMEOUT_SECONDS", 60.0)
DATASET_REQUEST_DELAY_SECONDS = _float("DATASET_REQUEST_DELAY_SECONDS", 2.0)
DATASET_MAX_ACTIVE_TASKS = _int("DATASET_MAX_ACTIVE_TASKS", 2)