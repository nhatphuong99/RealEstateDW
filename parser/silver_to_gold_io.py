"""
parser/silver_to_gold_io.py

Orchestration cho ETL Silver -> Gold (DAG 4). File Python DUY NHẤT của
Gold ETL — KHÔNG chứa business logic, toàn bộ logic nằm trong 2 file SQL
(sql/queries/etl_silver_to_gold.sql, sql/queries/validate_gold_load.sql).

Không có silver_to_gold_core.py: Silver và Gold cùng 1 Postgres, transform
làm thẳng bằng SQL thay vì kéo dữ liệu ra Spark/Python.
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
    """Task entrypoint duy nhất cho PythonOperator.

    Chạy nguyên văn etl_silver_to_gold.sql — file này đã tự bọc
    BEGIN;...COMMIT; riêng, nên conn PHẢI ở chế độ autocommit=True để
    không bị lồng transaction (psycopg2 mặc định tự mở transaction ngầm
    nếu autocommit=False) — giống hệt cách _run_scd2_merge() xử lý
    merge_scd2_listing_history.sql trong bronze_file_state_io.py.

    Không có control-plane riêng (khác Bronze->Silver): đây là 1
    transaction full-refresh idempotent duy nhất, không cần theo dõi
    trạng thái từng dòng/file — lỗi ở bất kỳ bước nào trong 6 bước của
    etl_silver_to_gold.sql sẽ tự ROLLBACK TOÀN BỘ transaction (nguyên tử) —
    kể cả các bước Dim đã chạy đúng trước đó — nên nếu gặp
    row_count_match FAIL với actual=0, khả năng cao là 1 trong 6 bước bị
    lỗi cứng (VD NOT NULL violation, cột không tồn tại do schema-drift)
    khiến TOÀN BỘ batch bị rollback, không phải do JOIN chỉ rớt vài dòng —
    chạy sql/queries/diagnose_gold_join_loss.sql để xác định chính xác.
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
    """Chạy validate_gold_load.sql (6 check: row_count_match,
    is_current_unique_per_listing, reconfirmed_ratio_info [chỉ thông tin,
    luôn pass], fact_fk_not_null, price_per_m2_extreme_all_flagged,
    area_within_sanitized_bounds), raise RuntimeError liệt kê rõ check nào
    passed=FALSE — để Airflow đánh dấu task fail thay vì âm thầm pass khi
    dữ liệu Gold sai lệch. Gọi SAU run_etl_silver_to_gold() trong DAG
    (task 2, nối tiếp bằng >>).
    """
    sql_text = _VALIDATE_SQL_PATH.read_text(encoding="utf-8")

    conn = psycopg2.connect(get_postgres_dsn())
    conn.autocommit = True  # chỉ SELECT, không cần transaction ghi
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
            f"validate_gold_load thất bại "
            f"({len(failed_checks)}/{len(rows)} check không đạt):\n"
            + "\n".join(failed_checks)
            + "\n\nNếu row_count_match lệch (đặc biệt actual=0), chạy "
            "sql/queries/diagnose_gold_join_loss.sql để xác định chính xác nguyên nhân."
        )

    logger.info("[VALIDATE] Toàn bộ %d check đều đạt.", len(rows))
