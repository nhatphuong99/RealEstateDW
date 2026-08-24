"""
parser/config.py

Cấu hình ĐẶC THÙ cho package parser (Spark IO layer, Phase 2). Chỉ chứa
tham số riêng của parser (SparkSession). Tham số DÙNG CHUNG với crawler/
(DSN Postgres, S3 bucket, AWS region, RUNNING_IN_CONTAINER) import lại
từ config.py gốc — KHÔNG định nghĩa trùng.

parser/bronze_to_silver_core.py (logic thuần) KHÔNG import module này.
"""

import importlib.util
import os
from pathlib import Path

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
# SparkSession (Task 9)
# ---------------------------------------------------------------------
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "bronze_to_silver")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")
SPARK_JARS_DIR = os.getenv("SPARK_JARS_DIR", "/opt/spark-jars")
