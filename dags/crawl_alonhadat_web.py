"""
dags/crawl_alonhadat_web.py

DAG 2 — crawl trực tiếp alonhadat.com.vn theo control-plane
crawl.listing_progress / crawl.detail_queue (mục 5, 6 tài liệu thiết kế).

File này CHỈ khai báo lịch chạy/retry cho Airflow — toàn bộ logic nghiệp
vụ nằm ở:
    - crawler/bronze_crawler_core.py  (logic thuần, không I/O)
    - crawler/bronze_crawler_io.py    (I/O thật: Postgres/HTTP/S3/Proxy)
    - crawler/proxy_manager.py        (fetch + health-check proxy)

`run_dag2()` (trong bronze_crawler_io.py) là điểm gọi DUY NHẤT — lắp ráp
mọi factory, chạy 1 lần, tự raise RuntimeError nếu stop_reason bất thường
(fetch_error/blocked/proxy_exhausted) để Airflow đánh dấu task FAILED và
kích hoạt retry bên dưới.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from crawler.bronze_crawler_io import run_dag2

default_args = {
    "owner": "phuong",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # Không retry_exponential_backoff — 429/CAPTCHA thường không tự hết
    # trong vài phút, cố định 5 phút đơn giản và đủ dùng cho quy mô đồ án.
}

with DAG(
    dag_id="crawl_alonhadat_web",
    description="DAG 2 - crawl trực tiếp alonhadat.com.vn (listing_progress/detail_queue)",
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
