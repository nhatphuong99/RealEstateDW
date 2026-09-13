"""dags/silver_to_gold.py — Component 4 — DAG 4: ETL Silver -> Gold.

Final step of the automated hourly chain: DAG 2 (@hourly) -> DAG 3 -> DAG 4
(this file). `schedule=None` — this DAG never runs on its own schedule,
always triggered by DAG 3.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task

from parser.silver_to_gold_io import run_etl_silver_to_gold, validate_gold_load

default_args = {
    "owner": "phuong",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="silver_to_gold",
    description="DAG 4 - ETL Silver -> Gold (final step of the DAG2->3->4 automated chain), "
                 "idempotent full-refresh via a single SQL transaction",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["gold", "etl", "dag4"],
) as dag:
    # No Spark involved -> no need for max_active_tis_per_dag.
    # No .expand() needed: just a single full-refresh SQL transaction.
    run_etl = task(task_id="run_etl_silver_to_gold")(run_etl_silver_to_gold)()
    validate = task(task_id="validate_gold_load")(validate_gold_load)()

    # Explicit >> chaining — both functions return None, no XCom passed between them.
    run_etl >> validate
