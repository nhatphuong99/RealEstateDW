from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task

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
    description="DAG 3 - ETL Bronze -> Silver (Phase 2 Spark parse + Phase 3 SQL merge SCD2)",
    schedule=None,  # chạy tay trước, đặt lịch thật sau khi tin tưởng pipeline
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

    cleanup_tmp >> reset_stuck >> discover >> keys
    run_etl.expand(s3_key=keys)
