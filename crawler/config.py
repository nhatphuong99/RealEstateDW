"""
crawler/config.py

Component 1+2 — config specific to the crawler (Dataset Loader + Web Crawler).
Shared params (Postgres DSN, S3, AWS region) imported from the root config.py.
"""

import importlib.util
import os
from pathlib import Path
from dotenv import load_dotenv

from bronze_paths import DATASET_PREFIX as _DEFAULT_DATASET_S3_PREFIX
from bronze_paths import WEB_PREFIX as BRONZE_WEB_PREFIX

_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH)


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw else default


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw else default


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


# Import the root config via absolute path (avoids depending on PYTHONPATH/cwd).
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ROOT_CONFIG_PATH = _PROJECT_ROOT / "config.py"

_spec = importlib.util.spec_from_file_location("root_config", _ROOT_CONFIG_PATH)
if _spec is None or _spec.loader is None:
    raise RuntimeError(f"Root config.py not found at: {_ROOT_CONFIG_PATH}")
root_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(root_config)

get_postgres_dsn = root_config.get_postgres_dsn
get_s3_bucket = root_config.get_s3_bucket
RUNNING_IN_CONTAINER = root_config.RUNNING_IN_CONTAINER
AWS_REGION = root_config.AWS_REGION


# HTTP fetch
CONNECT_TIMEOUT_SECONDS = _float("WEB_CRAWLER_CONNECT_TIMEOUT_SECONDS", 10.0)
READ_TIMEOUT_SECONDS = _float("WEB_CRAWLER_READ_TIMEOUT_SECONDS", 30.0)

# Proxy
PROXY_HEALTH_CHECK_TIMEOUT_SECONDS = _float("PROXY_HEALTH_CHECK_TIMEOUT_SECONDS", 4.0)
PROXY_HEALTH_CHECK_WORKERS = _int("PROXY_HEALTH_CHECK_WORKERS", 20)
PROXY_MAX_CANDIDATES = _int("PROXY_MAX_CANDIDATES", 200)

# Params for the raw proxy-list fetch step, separate from health-check timeout.
PROXYSCRAPE_TIMEOUT_SECONDS = _float("PROXYSCRAPE_TIMEOUT_SECONDS", 15.0)
PROXYSCRAPE_LIMIT = _int("PROXYSCRAPE_LIMIT", 500)
GEONODE_TIMEOUT_SECONDS = _float("GEONODE_TIMEOUT_SECONDS", 15.0)
GEONODE_LIMIT = _int("GEONODE_LIMIT", 100)

# Crawl loop (Component 2, DAG 2)
WEB_CRAWLER_MAX_DETAIL_PAGES_PER_RUN = _int("WEB_CRAWLER_MAX_DETAIL_PAGES_PER_RUN", 1000)
# 45 minutes — leaves ~20 min buffer for DAG 3+4 to finish within the same hour slot.
WEB_CRAWLER_TIME_BOX_SECONDS = _int("WEB_CRAWLER_TIME_BOX_SECONDS", 45 * 60)
WEB_CRAWLER_DELAY_MIN_SECONDS = _float("WEB_CRAWLER_DELAY_MIN_SECONDS", 5.0)
WEB_CRAWLER_DELAY_MAX_SECONDS = _float("WEB_CRAWLER_DELAY_MAX_SECONDS", 10.0)
WEB_CRAWLER_MAX_FETCH_ERROR_RETRIES = _int("WEB_CRAWLER_MAX_FETCH_ERROR_RETRIES", 3)
WEB_CRAWLER_FLUSH_INTERVAL_SECONDS = _int("WEB_CRAWLER_FLUSH_INTERVAL_SECONDS", 10 * 60)
WEB_CRAWLER_FLUSH_PAGE_THRESHOLD = _int("WEB_CRAWLER_FLUSH_PAGE_THRESHOLD", 100)
WEB_CRAWLER_MIN_SUCCESS_PAGES = _int("WEB_CRAWLER_MIN_SUCCESS_PAGES", 10)
WEB_CRAWLER_RECONCILE_STALE_RUN_AFTER_SECONDS = _int(
    "WEB_CRAWLER_RECONCILE_STALE_RUN_AFTER_SECONDS", 2 * 60 * 60
)

# Dataset loader (Component 1, DAG 1)
DATASET_CDN_BASE_URL = _require("DATASET_CDN_BASE_URL")
# Default falls back to bronze_paths.DATASET_PREFIX — the single source of
# truth for this prefix — rather than repeating the literal string here.
DATASET_S3_PREFIX = os.getenv("DATASET_S3_PREFIX", _DEFAULT_DATASET_S3_PREFIX)
DATASET_PROBE_TIMEOUT_SECONDS = _float("DATASET_PROBE_TIMEOUT_SECONDS", 20.0)
DATASET_DOWNLOAD_TIMEOUT_SECONDS = _float("DATASET_DOWNLOAD_TIMEOUT_SECONDS", 60.0)
DATASET_REQUEST_DELAY_SECONDS = _float("DATASET_REQUEST_DELAY_SECONDS", 2.0)
DATASET_MAX_ACTIVE_TASKS = _int("DATASET_MAX_ACTIVE_TASKS", 2)
