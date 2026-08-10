"""
Airflow DAG — Crawl alonhadat.com.vn theo batch ngắn, chạy NHIỀU LẦN/NGÀY.
Thay thế hoàn toàn cho "scrapy runspider" — dùng package crawler tự viết.

Vì sao không phải 1 DAG chạy 1 lần rồi lặp vô hạn (như vòng lặp sleep
trong Note.md): mỗi lần trigger DAG chỉ xử lý 1 batch (~120 URL, ~10-15
phút ở tốc độ 5-8s/request) rồi kết thúc sạch — đúng tinh thần "chia
nhiều lần chạy ngắn trong ngày" đã rút ra từ phát hiện rate-limit 429.
"""
from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="crawl_alonhadat_batch",
    schedule="0 8, 0, 3, 6, 9, 12, 15, 18, 21 * * *",  # 8 lần/ngày, cách nhau 3 tiếng
    start_date=datetime(2026, 8, 1),
    catchup=False,
    default_args=default_args,
    tags=["crawler", "alonhadat", "bronze"],
)
def crawl_alonhadat_batch():

    @task
    def ensure_seeds():
        """Chỉ enqueue seed (trang 1 mỗi category) nếu hàng đợi đang RỖNG
        HOÀN TOÀN — tránh insert lại seed mỗi lần DAG chạy (ON CONFLICT DO
        NOTHING trong enqueue_urls đã an toàn, nhưng check này tránh query
        dư thừa khi hàng đợi đã có dữ liệu)."""
        from crawler import queue_manager, pagination
        if not queue_manager.has_pending():
            inserted = pagination.enqueue_category_seeds()
            return {"seeds_inserted": inserted}
        return {"seeds_inserted": 0}

    @task
    def crawl_batch():
        from crawler import crawl_runner
        return crawl_runner.run_batch()

    ensure_seeds() >> crawl_batch()


crawl_alonhadat_batch()
