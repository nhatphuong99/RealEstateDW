"""
dags/web_crawler.py

Component 2 — DAG 2: crawls alonhadat.com.vn directly, tracked via the
pipeline.listing_progress / detail_queue control-plane.

This file only declares the schedule/retries — business logic lives in
crawler/web_crawler_core.py, crawler/web_crawler_io.py, crawler/proxy_manager.py.

The final task `trigger_bronze_to_silver` chains into DAG 3, which itself
chains into DAG 4 — the whole crawl -> Silver -> Gold chain runs
automatically every hour from this single @hourly schedule. DAG 1 sits
outside this chain and is triggered manually when needed.

`wait_for_completion=True` + `deferrable=True`: waits for DAG 3+4 to
finish before marking DONE (which, combined with `max_active_runs=1`,
naturally prevents two hourly cycles from overlapping), while freeing up
the worker slot while waiting.
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
}

with DAG(
    dag_id="web_crawler",
    description="DAG 2 - crawls alonhadat.com.vn directly, auto-chains into DAG 3 -> DAG 4",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bronze", "crawler", "web", "dag2"],
) as dag:
    crawl_web_detail_pages = PythonOperator(
        task_id="crawl_web_detail_pages",
        python_callable=run_dag2,
        # Uses this DAG run's own run_id -> maps directly to pipeline.run_state.run_id.
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    # trigger_run_id reuses DAG 2's run_id -> a single run_id spans DAG
    # 2/3/4 for the same hourly cycle, making it easy to trace in the
    # Airflow UI.
    trigger_bronze_to_silver = TriggerDagRunOperator(
        task_id="trigger_bronze_to_silver",
        trigger_dag_id="bronze_to_silver",
        trigger_run_id="{{ run_id }}",
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
    )

    crawl_web_detail_pages >> trigger_bronze_to_silver
