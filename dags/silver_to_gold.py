"""dags/silver_to_gold.py — DAG 4: ETL Silver -> Gold."""
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
    description="DAG 4 - ETL Silver -> Gold (5 Dim + Fact Transaction/Observation-grain, "
                 "full-refresh idempotent qua 1 transaction SQL)",
    schedule=None,  # chạy tay trước, đặt lịch thật sau khi tin tưởng pipeline (giống DAG 3)
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["gold", "etl", "dag4"],
) as dag:
    # Không dùng Spark -> không cần max_active_tis_per_dag giới hạn SparkSession.
    # Không cần .expand(): chỉ 1 transaction SQL full-refresh duy nhất,
    # không lặp theo danh sách file như run_etl_bronze_to_silver ở DAG 3.
    run_etl = task(task_id="run_etl_silver_to_gold")(run_etl_silver_to_gold)()
    validate = task(task_id="validate_gold_load")(validate_gold_load)()

    # Nối tường minh bằng >> (không dùng chain theo XCom như DAG 3) vì cả
    # 2 hàm đều trả None -- không có dữ liệu truyền qua lại giữa 2 task.
    run_etl >> validate
