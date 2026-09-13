"""
dags/dataset_loader.py

Component 1 — DAG 1: fetch 77 fixed dataset parts from a CDN into S3
(Bronze), tracked via the pipeline.dataset_part_state control-plane.

This file only declares the schedule/retries — business logic lives in
crawler/dataset_loader_core.py (pure) and crawler/dataset_loader_io.py (I/O).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
    description="DAG 1 - fetch 77 fixed dataset parts from a CDN into S3 (dataset_part_state)",
    schedule=None,             # manual trigger — the CDN dataset is fixed, no recurring schedule needed
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bronze", "crawler", "dataset", "dag1"],
) as dag:
    # Step 1 (non-mapped) — returns the list[part_number] to process.
    compute_parts = task(task_id="compute_parts_to_process")(compute_parts_to_process_task)

    # Steps 2-4 (mapped) — capped concurrency to avoid bursting the CDN.
    process_part = task(
        task_id="process_one_part",
        max_active_tis_per_dag=config.DATASET_MAX_ACTIVE_TASKS,
    )(process_one_part_task)

    trigger_bronze_to_silver = TriggerDagRunOperator(
        task_id="trigger_bronze_to_silver",
        trigger_dag_id="bronze_to_silver",
        trigger_run_id="{{ run_id }}",
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
    )

    # Only start DAG 3 once every part required by DAG 1 has finished processing.
    process_part.expand(part_number=compute_parts()) >> trigger_bronze_to_silver
