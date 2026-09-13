"""
dags/bronze_to_silver.py

Component 3 — DAG 3: ETL Bronze -> Silver (Spark parse + SQL SCD2 merge).

The final task `trigger_silver_to_gold` chains into DAG 4, completing the
crawl -> Silver -> Gold chain started by DAG 1 or DAG 2. `schedule=None`
— this DAG is always triggered by DAG 1/2, never runs on its own schedule.

`run_etl.expand(s3_key=keys) >> trigger_silver_to_gold`: the trigger only
runs after every mapped run_etl instance has finished — even when `keys`
is empty, the trigger still runs normally (the Gold ETL is idempotent, a
no-op is safe).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from parser import config
from parser.bronze_file_state_io import (
    cleanup_orphaned_tmp_dirs,
    discover_pending_files,
    get_pending_s3_keys,
    reset_stuck_files,
    run_etl_bronze_to_silver,
)

default_args = {
    "owner": "phuong",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_to_silver",
    description="DAG 3 - ETL Bronze -> Silver (Spark parse + SQL SCD2 merge), auto-chains into DAG 4",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["silver", "etl", "spark", "dag3"],
) as dag:
    # Remove leftover temp files
    cleanup_tmp = task(task_id="cleanup_orphaned_tmp_dirs")(cleanup_orphaned_tmp_dirs)()

    # Reset parquet files stuck in ('failed'/'processing') -> 'pending'
    reset_stuck = task(task_id="reset_stuck_files")(reset_stuck_files)()
    discover = task(task_id="discover_pending_files")(discover_pending_files)()
    keys = task(task_id="get_pending_s3_keys")(get_pending_s3_keys)()

    run_etl = task(
        task_id="run_etl_bronze_to_silver",
        max_active_tis_per_dag=config.SPARK_MAX_ACTIVE_TASKS,
    )(run_etl_bronze_to_silver)

    # trigger_run_id="{{ run_id }}" refers to DAG 3's own run_id (not DAG
    # 2's original one) — each DAG namespaces its run_id separately, still
    # traceable by trigger time in the Airflow UI.
    trigger_silver_to_gold = TriggerDagRunOperator(
        task_id="trigger_silver_to_gold",
        trigger_dag_id="silver_to_gold",
        trigger_run_id="{{ run_id }}",
        wait_for_completion=True,
        deferrable=True,
        poke_interval=15,  # Gold ETL is SQL-only, usually fast -> poll more frequently
    )

    cleanup_tmp >> reset_stuck >> discover >> keys
    run_etl.expand(s3_key=keys) >> trigger_silver_to_gold
