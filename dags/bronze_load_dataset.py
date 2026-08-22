"""
dags/bronze_load_dataset.py

DAG 1 — tải 77 part cố định (part1..part77.parquet) từ CDN dataset lên S3
(Bronze layer), theo control-plane crawl.dataset_part_state.

File này CHỈ khai báo lịch chạy/retry cho Airflow — toàn bộ logic nghiệp
vụ nằm ở:
    - crawler/dataset_loader_core.py  (logic thuần, không I/O)
    - crawler/dataset_loader_io.py    (I/O thật: Postgres/HTTP/S3)

Dùng TaskFlow API (`@task`/`.expand()`) áp thẳng lên 2 hàm đã có sẵn trong
dataset_loader_io.py — KHÔNG viết thêm hàm bọc, giữ đúng nguyên tắc DAG
file chỉ wiring, không chứa logic nghiệp vụ (giống crawl_alonhadat_web.py
bên Nhóm B).

Task 2 (`process_one_part`) là dynamic-mapped: mỗi part cần xử lý là 1
Task Instance riêng, chạy song song có giới hạn — part nào lỗi không ảnh
hưởng các part khác. KHÔNG tự viết retry loop trong code (xem
dataset_loader_core.py) — dựa hẳn vào retries/retry_delay của Airflow
task (quyết định D1). Khác hẳn Nhóm B: CDN ổn định, không proxy/CAPTCHA/
rate-limit -> không cần state machine phức tạp.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task

from crawler.dataset_loader_io import (
    compute_parts_to_process_task,
    process_one_part_task,
)

default_args = {
    "owner": "phuong",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    # CDN ổn định hơn hẳn alonhadat.com.vn (không proxy/CAPTCHA/rate-limit)
    # -> retry_delay ngắn hơn Nhóm B (5 phút) là đủ.
}

with DAG(
    dag_id="bronze_load_dataset",
    description="DAG 1 - tai 77 part co dinh tu CDN dataset len S3 (dataset_part_state)",
    schedule=None,             # chạy tay — dataset CDN cố định, không có lịch định kỳ
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bronze", "crawler", "dataset", "dag1"],
) as dag:
    # Task 1 (không mapped) — trả về list[part_number] cần xử lý, dùng
    # trực tiếp làm input cho .expand() của Task 2.
    compute_parts = task(task_id="compute_parts_to_process")(compute_parts_to_process_task)

    # Task 2 (mapped) — 1 Task Instance / part, chạy song song tối đa 5
    # cùng lúc (lịch sự với CDN, tránh mở 77 connection đồng thời).
    process_part = task(
        task_id="process_one_part",
        max_active_tis_per_dag=5,
    )(process_one_part_task)

    process_part.expand(part_number=compute_parts())
