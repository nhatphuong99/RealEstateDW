"""
DAG 2 (Parse): đọc HTML [URL-DS] đã lưu ở Bronze, extract field từng
article.property-item, upsert vào staging.listings_raw (Silver-ready).

Tách biệt HOÀN TOÀN khỏi DAG Fetch (không dùng ExternalTaskSensor phụ
thuộc trực tiếp): nếu phát hiện bug selector sau này, chỉ cần sửa code
parser.py và chạy lại DAG này — KHÔNG cần crawl lại (đúng lợi ích đã ghi
trong tong_hop_boi_canh, mục 6). Vì không phụ thuộc rate-limit của site
ngoài, có thể chạy tần suất cao hơn nhiều so với DAG Fetch.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from crawler.parser import run_parse_batch

default_args = {
    "owner": "phuong",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="dag_parse_alonhadat",
    description="Parse Bronze (S3) -> staging.listings_raw (Silver-ready)",
    default_args=default_args,
    schedule="*/20 * * * *",  # mỗi 20 phút — không phụ thuộc rate-limit ngoài
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dep305x", "real-estate", "parser", "silver"],
) as dag:

    def _run_parse_task(**context):
        stats = run_parse_batch(limit=200)
        context["ti"].xcom_push(key="parse_stats", value=stats)
        return stats

    parse_task = PythonOperator(
        task_id="parse_batch",
        python_callable=_run_parse_task,
    )
