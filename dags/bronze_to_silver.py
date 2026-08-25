from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task

from parser.bronze_file_state_io import (
    discover_pending_files,
    get_pending_s3_keys,
    run_etl_bronze_to_silver,
)

default_args = {
    "owner": "phuong",
    "retries": 3,
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
    # Task 1 (không mapped) — quét S3, insert file Bronze mới vào
    # crawl.bronze_file_state (idempotent, xem Task 18).
    discover = task(task_id="discover_pending_files")(discover_pending_files)()

    # Task 2 (không mapped) — trả về list[s3_key] status='pending', dùng
    # trực tiếp làm input cho .expand() của Task 3.
    keys = task(task_id="get_pending_s3_keys")(get_pending_s3_keys)()

    # Task 3 (mapped) — 1 Task Instance/file, retry riêng file lỗi mà không
    # kéo lại cả batch (Task 19, lựa chọn A đã chốt).
    run_etl = task(task_id="run_etl_bronze_to_silver")(run_etl_bronze_to_silver)

    discover >> keys
    run_etl.expand(s3_key=keys)