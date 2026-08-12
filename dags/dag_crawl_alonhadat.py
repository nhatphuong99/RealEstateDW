"""
DAG 1 (Fetch): crawl HTML [URL-DS] từ alonhadat.com.vn, lưu Bronze (S3).

Lịch chạy: nhiều lần ngắn/ngày (KHÔNG chạy 1 task dài với vòng lặp sleep
— anti-pattern giữ worker slot đã ghi trong tong_hop_boi_canh, mục 5).
Mỗi lần trigger xử lý tối đa MAX_PAGES_PER_RUN [URL-DS] rồi kết thúc.

Cron "0 1,5,9,13,17,21 * * *" -> 6 lần/ngày, cách nhau 4 tiếng, tránh đúng
mốc ~4 tiếng đã ghi nhận có rủi ro 429 cao hơn bằng cách rải đều thay vì
gộp cụm giờ gần nhau.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from crawler.crawl_runner import run_batch

default_args = {
    "owner": "phuong",
    "retries": 1,  # retry ở tầng Airflow chỉ cho lỗi hạ tầng (DB down,...),
                   # KHÔNG phải cơ chế xử lý 429 (đã có ở tầng queue)
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_crawl_alonhadat",
    description="Fetch [URL-DS] alonhadat.com.vn -> Bronze (S3)",
    default_args=default_args,
    schedule="0 1,5,9,13,17,21 * * *",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,  # không cho 2 run chồng lên nhau, tránh double-claim
    tags=["dep305x", "real-estate", "crawler", "bronze"],
) as dag:

    def _run_batch_task(**context):
        summary = run_batch()
        context["ti"].xcom_push(key="run_summary", value=vars(summary))
        return vars(summary)

    fetch_task = PythonOperator(
        task_id="fetch_batch",
        python_callable=_run_batch_task,
    )
