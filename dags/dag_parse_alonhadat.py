"""
Airflow DAG — Parse HTML thô đã crawl. Chạy ĐỘC LẬP với DAG crawl (lệch
30 phút sau mỗi lần crawl), dùng tinh thần tách fetch/parse: nếu selector
sai / site đổi cấu trúc, chỉ cần sửa parser_runner.py và CHẠY LẠI DAG NÀY,
không cần crawl lại từ đầu.
"""
from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {"retries": 2, "retry_delay": timedelta(minutes=5)}


@dag(
    dag_id="parse_alonhadat_batch",
    schedule="30 0, 3, 6, 9, 12, 15, 18, 21 * * *",  # chạy sau mỗi lần crawl 30 phút
    start_date=datetime(2026, 8, 1),
    catchup=False,
    default_args=default_args,
    tags=["parser", "alonhadat", "silver"],
)
def parse_alonhadat_batch():

    @task
    def parse_batch():
        from crawler import parser_runner
        return parser_runner.run()

    parse_batch()


parse_alonhadat_batch()
