"""
dags/bronze_to_silver.py

DAG 3 — ETL Bronze -> Silver (Phase 2 Spark parse + Phase 3 SQL merge SCD2).

TỰ ĐỘNG HÓA (mới): task cuối `trigger_silver_to_gold` nối thẳng sang DAG 4
(silver_to_gold), hoàn thiện chuỗi crawl -> Silver -> Gold khởi động từ
DAG 2 (@hourly). `schedule=None` GIỮ NGUYÊN — DAG này không tự chạy theo
lịch riêng, luôn được DAG 2 trigger.

Dependency `run_etl.expand(s3_key=keys) >> trigger_silver_to_gold`: task
trigger chỉ chạy SAU KHI TOÀN BỘ mapped task instance của run_etl (mỗi
file Bronze pending là 1 instance) hoàn tất — kể cả khi `keys` rỗng (0
file pending trong chu kỳ này, VD web crawl không phát hiện tin mới),
Airflow coi 0 mapped instance là thỏa mãn vô điều kiện và vẫn chạy
trigger_silver_to_gold bình thường -> Gold vẫn refresh (no-op an toàn nhờ
ETL Gold idempotent, không hại gì).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from parser import config
from parser.bronze_file_state_io import (
    cleanup_orphaned_tmp_dirs,
    discover_pending_files,
    get_pending_s3_keys,
    reset_stuck_files,
    run_etl_bronze_to_silver,
)

default_args = {
    "owner": "phuong",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_to_silver",
    description="DAG 3 - ETL Bronze -> Silver (Spark parse + SQL merge SCD2), tự động nối sang DAG 4",
    schedule=None,  # KHÔNG tự chạy theo lịch riêng — luôn được DAG 2 trigger
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["silver", "etl", "spark", "dag3"],
) as dag:
    # Xóa file tạm còn sót
    cleanup_tmp = task(task_id="cleanup_orphaned_tmp_dirs")(cleanup_orphaned_tmp_dirs)()

    # Reset parquet bị kẹt ('failed'/'processing') -> 'pending'
    reset_stuck = task(task_id="reset_stuck_files")(reset_stuck_files)()
    discover = task(task_id="discover_pending_files")(discover_pending_files)()
    keys = task(task_id="get_pending_s3_keys")(get_pending_s3_keys)()

    run_etl = task(
        task_id="run_etl_bronze_to_silver",
        max_active_tis_per_dag=config.SPARK_MAX_ACTIVE_TASKS,
    )(run_etl_bronze_to_silver)

    # trigger_run_id="{{ run_id }}" của DAG 3 (KHÔNG phải run_id gốc của DAG 2)
    # — mỗi DAG namespace run_id riêng, không xung đột, vẫn đủ để trace theo
    # thời gian trigger trong Airflow UI.
    trigger_silver_to_gold = TriggerDagRunOperator(
        task_id="trigger_silver_to_gold",
        trigger_dag_id="silver_to_gold",
        trigger_run_id="{{ run_id }}",
        wait_for_completion=True,
        deferrable=True,
        poke_interval=15,  # Gold ETL là SQL-only, thường nhanh hơn Silver nhiều — poll dày hơn
    )

    cleanup_tmp >> reset_stuck >> discover >> keys
    run_etl.expand(s3_key=keys) >> trigger_silver_to_gold
