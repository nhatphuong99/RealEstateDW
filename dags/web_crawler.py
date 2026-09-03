"""
dags/web_crawler.py

Thành phần 2 — DAG 2: crawl trực tiếp alonhadat.com.vn theo control-plane
pipeline.listing_progress / detail_queue.

File này chỉ khai báo lịch chạy/retry — logic nghiệp vụ nằm ở
crawler/web_crawler_core.py, crawler/web_crawler_io.py, crawler/proxy_manager.py.

Task cuối `trigger_bronze_to_silver` nối sang DAG 3, DAG 3 tự nối sang
DAG 4 — cả chuỗi crawl -> Silver -> Gold chạy tự động mỗi giờ chỉ từ 1
lịch @hourly duy nhất ở đây. DAG 1 đứng ngoài chuỗi, trigger tay khi cần.

`wait_for_completion=True` + `deferrable=True`: chờ DAG 3+4 xong mới DONE
(nhờ đó `max_active_runs=1` tự ngăn 2 chu kỳ hourly chồng nhau), đồng thời
giải phóng worker slot trong lúc chờ.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from crawler.web_crawler_io import run_dag2

default_args = {
    "owner": "phuong",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="web_crawler",
    description="DAG 2 - crawl trực tiếp alonhadat.com.vn, tự động nối sang DAG 3 -> DAG 4",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bronze", "crawler", "web", "dag2"],
) as dag:
    crawl_web_detail_pages = PythonOperator(
        task_id="crawl_web_detail_pages",
        python_callable=run_dag2,
        # run_id của chính DAG run -> khớp trực tiếp pipeline.run_state.run_id.
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    # trigger_run_id dùng lại run_id của DAG 2 -> cùng 1 chu kỳ hourly có
    # 1 run_id xuyên suốt DAG 2/3/4, dễ truy vết trên Airflow UI.
    trigger_bronze_to_silver = TriggerDagRunOperator(
        task_id="trigger_bronze_to_silver",
        trigger_dag_id="bronze_to_silver",
        trigger_run_id="{{ run_id }}",
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
    )

    crawl_web_detail_pages >> trigger_bronze_to_silver
