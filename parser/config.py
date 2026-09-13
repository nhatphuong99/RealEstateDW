"""
parser/config.py

Component 3+4 — config specific to the parser (Spark I/O layer). Params
shared with crawler/ (Postgres DSN, S3 bucket, AWS region) are imported
from the root config.py, not redefined here.

bronze_to_silver_core.py (pure logic) does not import this module.
"""

import importlib.util
import os
from pathlib import Path

from bronze_paths import DATASET_PREFIX as BRONZE_DATASET_PREFIX
from bronze_paths import WEB_PREFIX as BRONZE_WEB_PREFIX

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

# BRONZE_DATASET_PREFIX / BRONZE_WEB_PREFIX (imported above) come straight
# from bronze_paths.py — the single source of truth for these prefixes —
# re-exported here so I/O modules in this package can keep using the
# familiar `from parser.config import ...` pattern instead of reaching
# into bronze_paths directly.

SPARK_MAX_ACTIVE_TASKS = 1

BRONZE_TMP_DIR_PREFIX = "bronze_dl_"

# ---------------------------------------------------------------------
# SparkSession (Component 3, Step 2)
# ---------------------------------------------------------------------
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "bronze_to_silver")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[2]")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")
SPARK_JARS_DIR = os.getenv("SPARK_JARS_DIR", "/opt/spark-jars")
