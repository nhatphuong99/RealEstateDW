"""
config.py (project root)

Shared config for the whole project (>=2 packages: crawler, parser).
Package-specific params (proxy, Spark...) stay in crawler/config.py or
parser/config.py.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

_ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH)


def _require(name: str) -> str:
    """Read a required env var, raise if missing."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


# Database (Postgres DW)
RUNNING_IN_CONTAINER = os.getenv("AIRFLOW_HOME") is not None

def get_postgres_dsn() -> str:
    """Return the DSN for the current context (container or local)."""
    if RUNNING_IN_CONTAINER:
        return _require("POSTGRES_DW_DSN")
    return _require("POSTGRES_DW_DSN_LOCAL")


# AWS S3 (Bronze layer) — boto3/Spark read AWS_ACCESS_KEY_ID/SECRET from os.environ directly.
def get_s3_bucket() -> str:
    return _require("S3_BRONZE_BUCKET")

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
