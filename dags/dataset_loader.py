"""
dags/dataset_loader.py

Thành phần 1 — DAG 1: tải 77 part cố định từ CDN dataset lên S3 (Bronze),
theo control-plane pipeline.dataset_part_state.

File này chỉ khai báo lịch chạy/retry — logic nghiệp vụ nằm ở
crawler/dataset_loader_core.py (thuần) và crawler/dataset_loader_io.py (I/O).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task

from crawler import config
from crawler.dataset_loader_io import (
    compute_parts_to_process_task,
    process_one_part_task,
)

default_args = {
    "owner": "phuong",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dataset_loader",
    description="DAG 1 - tai 77 part co dinh tu CDN dataset len S3 (dataset_part_state)",
    schedule=None,             # chạy tay — dataset CDN cố định, không có lịch định kỳ
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bronze", "crawler", "dataset", "dag1"],
) as dag:
    # Bước 1 (không mapped) — trả về list[part_number] cần xử lý.
    compute_parts = task(task_id="compute_parts_to_process")(compute_parts_to_process_task)

    # Bước 2-4 (mapped) — giới hạn đồng thời tránh burst request lên CDN.
    process_part = task(
        task_id="process_one_part",
        max_active_tis_per_dag=config.DATASET_MAX_ACTIVE_TASKS,
    )(process_one_part_task)

    process_part.expand(part_number=compute_parts())
