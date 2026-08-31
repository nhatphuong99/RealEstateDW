"""
dags/web_crawler.py

DAG 2 — crawl trực tiếp alonhadat.com.vn theo control-plane
pipeline.listing_progress / pipeline.detail_queue.

File này CHỈ khai báo lịch chạy/retry — logic nghiệp vụ nằm ở
crawler/web_crawler_core.py (thuần), crawler/web_crawler_io.py (I/O thật),
crawler/proxy_manager.py (proxy). `run_dag2()` là điểm gọi duy nhất, tự
raise RuntimeError khi stop_reason bất thường để Airflow retry.

TỰ ĐỘNG HÓA: task cuối `trigger_bronze_to_silver` nối sang DAG 3, DAG 3 tự
nối sang DAG 4 — cả chuỗi crawl -> Silver -> Gold chạy tự động mỗi giờ chỉ
từ 1 lịch @hourly duy nhất ở đây. DAG 1 (dataset_loader) đứng ngoài chuỗi,
trigger tay khi cần.

`wait_for_completion=True` + `deferrable=True`: task này chờ cả DAG 3+4
chạy xong mới DONE, nhờ đó `max_active_runs=1` tự ngăn 2 chu kỳ hourly
chồng nhau. `deferrable=True` giải phóng worker slot trong lúc chờ (qua
airflow-triggerer) — quan trọng vì Spark ở DAG 3 cũng cần slot riêng.
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
    # Không retry_exponential_backoff — 429/CAPTCHA thường không tự hết
    # trong vài phút, cố định 5 phút đơn giản và đủ dùng cho quy mô đồ án.
}

with DAG(
    dag_id="web_crawler",
    description="DAG 2 - crawl trực tiếp alonhadat.com.vn, tự động nối sang DAG 3 -> DAG 4",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,           # không chạy bù các giờ đã qua khi mới bật DAG
    max_active_runs=1,       # concurrency luôn = 1 — KHÔNG cho 2 run hourly chồng nhau
    default_args=default_args,
    tags=["bronze", "crawler", "alonhadat", "dag2"],
) as dag:
    crawl_web_detail_pages = PythonOperator(
        task_id="crawl_web_detail_pages",
        python_callable=run_dag2,
        # Truyền run_id của chính DAG run vào crawler -> pipeline.run_state.run_id
        # khớp trực tiếp với Airflow UI, dễ truy vết khi debug.
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    # trigger_run_id="{{ run_id }}" — dùng LUÔN run_id của chính DAG 2 run này
    # đặt tên cho DAG run bên DAG 3 -> dễ truy vết trong Airflow UI: cùng
    # 1 run_id string xuất hiện ở cả DAG 2/3/4 nghĩa là cùng 1 chu kỳ hourly.
    # allowed_states/failed_states KHÔNG truyền tay — mặc định của
    # TriggerDagRunOperator là allowed_states=[success], failed_states=[failed],
    # đã đúng ý: DAG 3 fail -> task này fail -> DAG 2 run bị đánh FAILED,
    # KHÔNG trigger tiếp bước sau (trigger_rule mặc định all_success).
    trigger_bronze_to_silver = TriggerDagRunOperator(
        task_id="trigger_bronze_to_silver",
        trigger_dag_id="bronze_to_silver",
        trigger_run_id="{{ run_id }}",
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
    )

    crawl_web_detail_pages >> trigger_bronze_to_silver
