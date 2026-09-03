"""dags/silver_to_gold.py — Thành phần 4 — DAG 4: ETL Silver -> Gold.

Bước cuối trong chuỗi tự động hourly: DAG 2 (@hourly) -> DAG 3 -> DAG 4
(file này). `schedule=None` — DAG này không tự chạy theo lịch riêng,
luôn được DAG 3 trigger.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task

from parser.silver_to_gold_io import run_etl_silver_to_gold, validate_gold_load

default_args = {
    "owner": "phuong",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="silver_to_gold",
    description="DAG 4 - ETL Silver -> Gold (bước cuối chuỗi tự động DAG2->3->4), "
                 "full-refresh idempotent qua 1 transaction SQL",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["gold", "etl", "dag4"],
) as dag:
    # Không dùng Spark -> không cần giới hạn max_active_tis_per_dag.
    # Không cần .expand(): chỉ 1 transaction SQL full-refresh duy nhất.
    run_etl = task(task_id="run_etl_silver_to_gold")(run_etl_silver_to_gold)()
    validate = task(task_id="validate_gold_load")(validate_gold_load)()

    # Nối tường minh bằng >> — cả 2 hàm đều trả None, không có XCom truyền qua lại.
    run_etl >> validate
