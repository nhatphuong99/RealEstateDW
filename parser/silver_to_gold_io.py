"""
parser/silver_to_gold_io.py

Component 4 (ETL Silver -> Gold) — orchestration, the only Python file for
the Gold ETL. All the logic lives in 2 SQL files:
sql/queries/etl_silver_to_gold.sql, sql/queries/validate_gold_load.sql.

No silver_to_gold_core.py: Silver and Gold live in the same Postgres, so
this transforms directly in SQL instead of pulling data out into Spark/Python.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import psycopg2

from parser.config import get_postgres_dsn

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ETL_SQL_PATH = _PROJECT_ROOT / "sql" / "queries" / "etl_silver_to_gold.sql"
_VALIDATE_SQL_PATH = _PROJECT_ROOT / "sql" / "queries" / "validate_gold_load.sql"


def run_etl_silver_to_gold() -> None:
    """Steps 1-2 — run etl_silver_to_gold.sql verbatim (it wraps its own
    BEGIN;...COMMIT;), so conn must be autocommit=True to avoid nesting
    transactions.

    A single idempotent full-refresh transaction — a failure at any step
    rolls back everything. If row_count_match FAILs with actual=0, run
    diagnose_gold_join_loss.sql to pinpoint the cause.
    """
    t0 = time.perf_counter()
    sql_text = _ETL_SQL_PATH.read_text(encoding="utf-8")

    conn = psycopg2.connect(get_postgres_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql_text)
        logger.info(
            "[TIMING] run_etl_silver_to_gold: %.1fs", time.perf_counter() - t0
        )
    finally:
        conn.close()


def validate_gold_load() -> None:
    """Step 3 — run validate_gold_load.sql (5 checks: row_count_match,
    is_current_unique_per_listing, fact_fk_not_null,
    price_per_m2_extreme_all_flagged, area_within_sanitized_bounds), raises
    RuntimeError listing exactly which checks failed — so Airflow marks the
    task failed instead of silently passing on bad Gold data.
    """
    sql_text = _VALIDATE_SQL_PATH.read_text(encoding="utf-8")

    conn = psycopg2.connect(get_postgres_dsn())
    conn.autocommit = True  # read-only, no write transaction needed
    try:
        with conn.cursor() as cur:
            cur.execute(sql_text)
            rows = cur.fetchall()
    finally:
        conn.close()

    failed_checks = [
        f"  - {check_name}: expected={expected}, actual={actual}"
        for check_name, expected, actual, passed in rows
        if not passed
    ]

    if failed_checks:
        raise RuntimeError(
            f"validate_gold_load failed "
            f"({len(failed_checks)}/{len(rows)} checks did not pass):\n"
            + "\n".join(failed_checks)
            + "\n\nIf row_count_match is off (especially actual=0), run "
            "sql/queries/diagnose_gold_join_loss.sql to pinpoint the exact cause."
        )

    logger.info("[VALIDATE] All %d checks passed.", len(rows))
