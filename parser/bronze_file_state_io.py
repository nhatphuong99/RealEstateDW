"""
parser/bronze_file_state_io.py

I/O control-plane cho Phase 4: quét S3 tìm file Bronze mới (Task 18) và
chạy ETL trọn vẹn 1 file Bronze -> Silver, gộp Phase 2 (Spark parse) +
Phase 3 (SQL merge SCD2) (Task 19). Tách riêng khỏi bronze_to_silver_io.py
vì đây là lớp orchestration/control-plane, không phải logic Spark thuần.
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
    parse_partition,
    read_bronze_parquet,
    write_staging_and_quarantine,
)
from parser.config import get_postgres_dsn, get_s3_bucket

_BRONZE_PREFIX = "bronze/"
_DATASET_PREFIX = "bronze/dataset/"
_WEB_PREFIX = "bronze/web/"

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_MERGE_SCD2_SQL_PATH = _PROJECT_ROOT / "sql" / "queries" / "merge_scd2_listing_history.sql"


# ---------------------------------------------------------------------------
# Task 18 — discover_pending_files (đã hoàn thành, PASS smoke test)
# ---------------------------------------------------------------------------


def _infer_source(s3_key: str) -> str:
    """Suy 'source' ('dataset'|'web') từ prefix của s3_key."""
    if s3_key.startswith(_DATASET_PREFIX):
        return "dataset"
    if s3_key.startswith(_WEB_PREFIX):
        return "web"
    raise ValueError(f"Không suy được source (dataset/web) từ s3_key: {s3_key!r}")


def list_bronze_parquet_keys(s3_client=None) -> list[str]:
    """List toàn bộ key .parquet dưới prefix bronze/ trên S3.
    Dùng paginator vì list_objects_v2 giới hạn 1000 object/page."""
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
    """Task 18 — quét S3, insert key Bronze mới vào crawl.bronze_file_state
    với status='pending' (giá trị DEFAULT của cột).

    Idempotent qua INSERT ... ON CONFLICT (s3_key) DO NOTHING (s3_key là
    PRIMARY KEY). Dùng execute_values(..., fetch=True) + RETURNING thay vì
    cur.rowcount, vì rowcount sau executemany không đáng tin.

    Trả về số dòng MỚI thực sự được insert.
    """
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
                    INSERT INTO crawl.bronze_file_state (s3_key, source)
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


# ---------------------------------------------------------------------------
# Task 19 — run_etl_bronze_to_silver: gộp Phase 2 + Phase 3 cho 1 file
# ---------------------------------------------------------------------------


def _mark_processing(conn, s3_key: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE crawl.bronze_file_state SET status = 'processing' WHERE s3_key = %s",
            (s3_key,),
        )


def _mark_done(conn, s3_key: str, rows_parsed: int, rows_quarantined: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE crawl.bronze_file_state
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
            UPDATE crawl.bronze_file_state
            SET status = 'failed',
                processed_at = now(),
                last_error = %s
            WHERE s3_key = %s
            """,
            (error_message[:2000], s3_key),  # cắt bớt tránh log lỗi khổng lồ
        )


def _run_scd2_merge(conn) -> None:
    """Chạy nguyên văn merge_scd2_listing_history.sql — file này đã tự bọc
    BEGIN;...COMMIT; riêng (xem sql/queries/merge_scd2_listing_history.sql),
    nên conn PHẢI ở chế độ autocommit=True để không bị lồng transaction
    (psycopg2 mặc định tự mở transaction ngầm nếu autocommit=False)."""
    sql_text = _MERGE_SCD2_SQL_PATH.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql_text)


def run_etl_bronze_to_silver(s3_key: str) -> None:
    """Task 19 — điểm gọi duy nhất cho PythonOperator: gộp Phase 2 (Spark
    parse Bronze -> Silver staging, Task 9-12) + Phase 3 (SQL merge SCD2,
    Task 14) cho ĐÚNG 1 file Bronze (s3_key).

    Xử lý 1 file/lần gọi (không lặp nhiều file trong hàm) để Task 20 dùng
    Airflow dynamic task mapping — mỗi file là 1 Task Instance độc lập,
    retry riêng file lỗi mà không kéo lại cả batch.

    Vòng đời crawl.bronze_file_state: pending -> processing -> done | failed.
    Lỗi ở bất kỳ bước nào -> đánh dấu failed + last_error rồi raise lại để
    Airflow tự retry (quyết định D1 — không tự viết retry loop).
    """
    conn = psycopg2.connect(get_postgres_dsn())
    conn.autocommit = True  # từng UPDATE là 1 statement atomic riêng; script
                            # merge SCD2 tự quản lý transaction của chính nó.
    try:
        _mark_processing(conn, s3_key)

        with conn.cursor() as cur:
            # TRUNCATE là trách nhiệm orchestrator (đã chốt trong
            # 005_etl_bronze_to_silver_control.sql), KHÔNG phải Phase 2.
            cur.execute("TRUNCATE silver.listing_staging_batch")

            # parse_quarantine là log VĨNH VIỄN (append qua nhiều file, không
            # TRUNCATE toàn bảng được) -> phải dọn riêng phần của đúng s3_key
            # này để idempotent khi 1 file bị chạy lại (retry Airflow, hoặc
            # smoke test tay) - cùng nguyên tắc đã áp dụng ở smoke_test.py.
            cur.execute(
                "DELETE FROM silver.parse_quarantine WHERE source_bronze_key = %s",
                (s3_key,),
    )

        spark = build_spark_session()
        try:
            # trong run_etl_bronze_to_silver(), parser/bronze_file_state_io.py
            bronze_df = read_bronze_parquet(spark, s3_key)
            source_part = os.path.splitext(os.path.basename(s3_key))[0]
            parsed_rdd = bronze_df.rdd.mapPartitions(parse_partition(source_part, s3_key))
            combined_df = spark.createDataFrame(parsed_rdd, schema=UNIFIED_PARSE_SCHEMA)

            rows_parsed, rows_quarantined = write_staging_and_quarantine(combined_df)

            bronze_df.unpersist()  # <-- THÊM: giải phóng cache HTML thô ngay sau khi
                                    # write_staging_and_quarantine() đã dùng xong (success_df/
                                    # quarantine_df cache riêng của nó không còn phụ thuộc bronze_df nữa)
        finally:
            spark.stop()

        _run_scd2_merge(conn)

        _mark_done(conn, s3_key, rows_parsed, rows_quarantined)

    except Exception as exc:  # noqa: BLE001 - cố tình bắt mọi lỗi để ghi last_error
        _mark_failed(conn, s3_key, str(exc))
        raise
    finally:
        conn.close()

def get_pending_s3_keys() -> list[str]:
    """Lấy danh sách s3_key đang status='pending' trong crawl.bronze_file_state,
    dùng làm input cho .expand() của task run_etl_bronze_to_silver (Task 20)."""
    conn = psycopg2.connect(get_postgres_dsn())
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT s3_key FROM crawl.bronze_file_state WHERE status = 'pending' "
                "ORDER BY discovered_at"
            )
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()