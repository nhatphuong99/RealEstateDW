"""
parser/bronze_file_state_io.py

Component 3 (ETL Bronze -> Silver) — control-plane: scan S3 for new Bronze
files (Step 1) and run the full ETL for a single file (Steps 2-7, combining
Spark parse + SCD2 merge). Kept separate from bronze_to_silver_io.py
because this is the orchestration/control-plane layer, not pure Spark logic.
"""

from __future__ import annotations

import os
from pathlib import Path

import boto3
import psycopg2
from psycopg2.extras import execute_values

from parser.bronze_to_silver_io import (
    UNIFIED_PARSE_SCHEMA,
    build_spark_session,
    download_bronze_file,
    parse_partition,
    read_bronze_parquet,
    write_staging_and_quarantine,
)
from parser.config import (
    BRONZE_DATASET_PREFIX as _DATASET_PREFIX,
    BRONZE_TMP_DIR_PREFIX,
    BRONZE_WEB_PREFIX as _WEB_PREFIX,
    get_postgres_dsn,
    get_s3_bucket,
)

import shutil
import tempfile
import time
import logging

logger = logging.getLogger(__name__)

_BRONZE_PREFIX = "bronze/"

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_MERGE_SCD2_SQL_PATH = _PROJECT_ROOT / "sql" / "queries" / "merge_scd2_listing_history.sql"


# ---------------------------------------------------------------------------
# Step 1 — discover_pending_files
# ---------------------------------------------------------------------------


def _infer_source(s3_key: str) -> str:
    """Infer 'source' ('dataset'|'web') from the s3_key prefix."""
    if s3_key.startswith(_DATASET_PREFIX):
        return "dataset"
    if s3_key.startswith(_WEB_PREFIX):
        return "web"
    raise ValueError(f"Could not infer source (dataset/web) from s3_key: {s3_key!r}")


def list_bronze_parquet_keys(s3_client=None) -> list[str]:
    """List every .parquet key under the bronze/ prefix (paginated —
    list_objects_v2 caps out at 1000 objects/page)."""
    s3_client = s3_client or boto3.client("s3")
    bucket = get_s3_bucket()
    paginator = s3_client.get_paginator("list_objects_v2")

    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=_BRONZE_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                keys.append(key)
    return keys


def discover_pending_files() -> int:
    """Scan S3, insert new keys into pipeline.bronze_file_state
    (status='pending'). Idempotent via ON CONFLICT DO NOTHING. Returns the
    number of rows actually newly inserted."""
    keys = list_bronze_parquet_keys()
    if not keys:
        return 0

    rows = [(key, _infer_source(key)) for key in keys]

    conn = psycopg2.connect(get_postgres_dsn())
    try:
        with conn:
            with conn.cursor() as cur:
                inserted = execute_values(
                    cur,
                    """
                    INSERT INTO pipeline.bronze_file_state (s3_key, source)
                    VALUES %s
                    ON CONFLICT (s3_key) DO NOTHING
                    RETURNING s3_key
                    """,
                    rows,
                    fetch=True,
                )
    finally:
        conn.close()

    return len(inserted)


def cleanup_orphaned_tmp_dirs(max_age_hours: float = 12.0) -> int:
    """Delete leftover /tmp/bronze_dl_* directories from a prior run that
    was killed abruptly. Only touches the 'bronze_dl_' prefix. Returns the
    number of directories removed."""
    base_tmp_dir = tempfile.gettempdir()
    cutoff = time.time() - max_age_hours * 3600
    removed = 0

    for entry in os.scandir(base_tmp_dir):
        if not entry.is_dir(follow_symlinks=False):
            continue
        if not entry.name.startswith(BRONZE_TMP_DIR_PREFIX):
            continue
        try:
            if entry.stat().st_mtime < cutoff:
                shutil.rmtree(entry.path, ignore_errors=True)
                removed += 1
        except FileNotFoundError:
            continue  # already removed by another process between scan and stat

    return removed


# ---------------------------------------------------------------------------
# Steps 2-7 — run_etl_bronze_to_silver
# ---------------------------------------------------------------------------


def _mark_processing(conn, s3_key: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pipeline.bronze_file_state SET status = 'processing' WHERE s3_key = %s",
            (s3_key,),
        )


def _mark_done(conn, s3_key: str, rows_parsed: int, rows_quarantined: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline.bronze_file_state
            SET status = 'done',
                rows_parsed = %s,
                rows_quarantined = %s,
                processed_at = now(),
                last_error = NULL
            WHERE s3_key = %s
            """,
            (rows_parsed, rows_quarantined, s3_key),
        )


def _mark_failed(conn, s3_key: str, error_message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline.bronze_file_state
            SET status = 'failed',
                processed_at = now(),
                last_error = %s
            WHERE s3_key = %s
            """,
            (error_message[:2000], s3_key),  # truncated to avoid huge error logs
        )


def _run_scd2_merge(conn) -> None:
    """Run merge_scd2_listing_history.sql verbatim (Step 6) — the file
    wraps its own BEGIN;...COMMIT;, so conn must be autocommit=True to
    avoid nesting transactions."""
    sql_text = _MERGE_SCD2_SQL_PATH.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql_text)

def _truncate_staging_after_success(conn) -> None:
    """Clean up silver.listing_staging_batch right after a successful SCD2 merge."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE silver.listing_staging_batch")


def run_etl_bronze_to_silver(s3_key: str) -> None:
    """The single entry point for the PythonOperator, processes one file
    per call (dynamic mapping — one Task Instance per file, retries don't
    drag the whole batch down). bronze_file_state lifecycle:
    pending -> processing -> done | failed."""
    t0 = time.perf_counter()
    conn = psycopg2.connect(get_postgres_dsn())
    conn.autocommit = True
    try:
        _mark_processing(conn, s3_key)

        with conn.cursor() as cur:
            cur.execute("TRUNCATE silver.listing_staging_batch")
            cur.execute(
                "DELETE FROM silver.parse_quarantine WHERE source_bronze_key = %s",
                (s3_key,),
            )

        # Download Bronze locally before starting Spark — avoids the JVM
        # competing for CPU with the boto3 download.
        tmp_dir = download_bronze_file(s3_key)
        try:
            local_path = os.path.join(tmp_dir.name, os.path.basename(s3_key))

            t_build_start = time.perf_counter()
            spark = build_spark_session()
            logger.info("[TIMING] build_spark_session: %.1fs", time.perf_counter() - t_build_start)

            try:
                t_read_start = time.perf_counter()
                bronze_df = read_bronze_parquet(spark, local_path, s3_key)
                n_rows = bronze_df.count()  # already cached, nearly free — just for logging
                logger.info("[TIMING] read_bronze_parquet: %.1fs (%d rows)",
                            time.perf_counter() - t_read_start, n_rows)

                t_parse_start = time.perf_counter()
                source_part = os.path.splitext(os.path.basename(s3_key))[0]
                parsed_rdd = bronze_df.rdd.mapPartitions(parse_partition(source_part, s3_key))
                combined_df = spark.createDataFrame(parsed_rdd, schema=UNIFIED_PARSE_SCHEMA)
                rows_parsed, rows_quarantined = write_staging_and_quarantine(combined_df)
                logger.info("[TIMING] parse + write JDBC: %.1fs (parsed=%d, quarantined=%d)",
                            time.perf_counter() - t_parse_start, rows_parsed, rows_quarantined)

                bronze_df.unpersist()
            finally:
                t_stop_start = time.perf_counter()
                spark.stop()
                logger.info("[TIMING] spark.stop(): %.1fs", time.perf_counter() - t_stop_start)
        finally:
            tmp_dir.cleanup()


        t_merge_start = time.perf_counter()
        _run_scd2_merge(conn)
        logger.info("[TIMING] SCD2 merge: %.1fs", time.perf_counter() - t_merge_start)

        try:
            _truncate_staging_after_success(conn)
        except Exception as cleanup_exc:  # noqa: BLE001
            logger.warning(
                "[CLEANUP] Failed to truncate staging after merge for %s (not fatal): %s",
                s3_key, cleanup_exc,
            )

        _mark_done(conn, s3_key, rows_parsed, rows_quarantined)
        logger.info("[TIMING] TOTAL: %.1fs for file %s", time.perf_counter() - t0, s3_key)

    except Exception as exc:
        _mark_failed(conn, s3_key, str(exc))
        raise
    finally:
        conn.close()

def get_pending_s3_keys() -> list[str]:
    """List of s3_keys with status='pending', input for
    run_etl_bronze_to_silver's .expand()."""
    conn = psycopg2.connect(get_postgres_dsn())
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT s3_key FROM pipeline.bronze_file_state WHERE status = 'pending' "
                "ORDER BY discovered_at"
            )
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def reset_stuck_files() -> int:
    """Reset files stuck in 'failed' (Airflow retries exhausted) or
    'processing' (task killed mid-run) back to 'pending'."""
    conn = psycopg2.connect(get_postgres_dsn())
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pipeline.bronze_file_state
                    SET status = 'pending', last_error = NULL
                    WHERE status IN ('failed', 'processing')
                    """
                )
                return cur.rowcount
    finally:
        conn.close()
