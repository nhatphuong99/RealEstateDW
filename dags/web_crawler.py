"""
dags/web_crawler.py

DAG 2 — crawl trực tiếp alonhadat.com.vn theo control-plane
crawl.listing_progress / crawl.detail_queue (mục 5, 6 tài liệu thiết kế).

File này CHỈ khai báo lịch chạy/retry cho Airflow — toàn bộ logic nghiệp
vụ nằm ở:
    - crawler/web_crawler_core.py  (logic thuần, không I/O)
    - crawler/web_crawler_io.py    (I/O thật: Postgres/HTTP/S3/Proxy)
    - crawler/proxy_manager.py        (fetch + health-check proxy)

`run_dag2()` (trong web_crawler_io.py) là điểm gọi DUY NHẤT — lắp ráp
mọi factory, chạy 1 lần, tự raise RuntimeError nếu stop_reason bất thường
(fetch_error/blocked/proxy_exhausted) để Airflow đánh dấu task FAILED và
kích hoạt retry bên dưới.

TỰ ĐỘNG HÓA (mới): task cuối `trigger_bronze_to_silver` nối thẳng sang DAG 3
(bronze_to_silver) — DAG 3 tự nối tiếp sang DAG 4 (silver_to_gold) ở cuối
file của nó. Toàn bộ chuỗi crawl -> Silver -> Gold chạy tự động mỗi giờ chỉ
từ 1 lịch @hourly duy nhất ở đây. DAG 1 (dataset_loader) KHÔNG nằm trong
chuỗi này — dataset CDN cố định, chỉ cần trigger tay 1 lần khi cần.

`wait_for_completion=True` + `deferrable=True`: task này CHỜ đến khi DAG 3
(và cả DAG 4 phía sau nó) chạy xong hẳn mới coi là DONE — nhờ vậy
`max_active_runs=1` của DAG này tự động ngăn 2 chu kỳ hourly chồng lên
nhau (chu kỳ sau chỉ bắt đầu khi CẢ CHUỖI 3 DAG của chu kỳ trước đã xong).
`deferrable=True` giải phóng worker slot trong lúc chờ (dùng
`airflow-triggerer`, đã có sẵn trong docker-compose.yaml) thay vì giữ
worker "ngủ" chờ suốt — quan trọng vì Spark trong DAG 3 cũng cần worker
slot riêng (SPARK_MAX_ACTIVE_TASKS=1), không nên bị task chờ này chiếm mất.
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
        # Truyền run_id của chính DAG run vào crawler -> crawl.run_state.run_id
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
