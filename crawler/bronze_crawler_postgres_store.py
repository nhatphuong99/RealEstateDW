"""
Module: bronze_crawler_postgres_store.py
Postgres-backed implementation cua PartQueueStore (interface dinh nghia trong
bronze_crawler_core.py). Dung cho production, chay ben trong Airflow task.

PGCLIENTENCODING=UTF8 nen duoc set trong .env / moi truong chay (da gap loi
encoding truoc do tren Windows theo error_log.md cua du an).
"""

from __future__ import annotations

import os

import psycopg2
import psycopg2.extras as pg_extras
from dotenv import load_dotenv

load_dotenv()

# Chay ben trong container Airflow chinh thuc luon co bien moi truong
# AIRFLOW_HOME (thuong la "/opt/airflow") - day la cach TIN CAY de phan biet
# "dang chay trong container" vs "dang chay tren host", vi ca 2 bien DSN deu
# ton tai dong thoi trong .env (load_dotenv() nap ca 2 bat ke dang chay o dau,
# nen KHONG THE dung "bien nao co gia tri" de suy doan boi ca 2 luon co gia tri).
_RUNNING_INSIDE_AIRFLOW_CONTAINER = os.environ.get("AIRFLOW_HOME") is not None

DW_DSN = (
    os.environ["POSTGRES_DW_DSN"]
    if _RUNNING_INSIDE_AIRFLOW_CONTAINER
    else os.environ["POSTGRES_DW_DSN_LOCAL"]
)
SOURCE_URL_TEMPLATE = "https://cdn.cuhuuhoang.com/alonhadat/part{n}.parquet"


class PostgresPartQueueStore:
    def __init__(self, dsn: str = DW_DSN):
        self.dsn = dsn

    def _connect(self):
        return psycopg2.connect(self.dsn)

    def get_max_known_part(self) -> int:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(part_number), 0) FROM crawl.dataset_part_queue;")
            return cur.fetchone()[0]

    def insert_new_parts(self, part_numbers: list) -> None:
        if not part_numbers:
            return
        rows = [(pn, SOURCE_URL_TEMPLATE.format(n=pn), "pending") for pn in part_numbers]
        with self._connect() as conn, conn.cursor() as cur:
            pg_extras.execute_values(
                cur,
                """
                INSERT INTO crawl.dataset_part_queue (part_number, source_url, status)
                VALUES %s
                ON CONFLICT (part_number) DO NOTHING;
                """,
                rows,
            )
            conn.commit()

    def reset_stuck_downloading(self, older_than_minutes: int) -> int:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE crawl.dataset_part_queue
                SET status = 'pending'
                WHERE status = 'downloading'
                  AND claimed_at < now() - (%s || ' minutes')::interval;
                """,
                (older_than_minutes,),
            )
            affected = cur.rowcount
            conn.commit()
            return affected

    def list_processable_parts(self) -> list:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT part_number FROM crawl.dataset_part_queue
                WHERE status IN ('pending', 'failed')
                ORDER BY part_number ASC;
                """
            )
            return [row[0] for row in cur.fetchall()]

    def claim_part(self, part_number: int, run_id) -> bool:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE crawl.dataset_part_queue
                SET status = 'downloading', claimed_by_run_id = %s,
                    claimed_at = now(), attempts = attempts + 1
                WHERE part_number = %s AND status IN ('pending', 'failed');
                """,
                (run_id, part_number),
            )
            claimed = cur.rowcount == 1
            conn.commit()
            return claimed

    def mark_success(self, part_number: int, *, s3_key, sha256, file_size_bytes, actual_rows) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE crawl.dataset_part_queue
                SET status = 'success', s3_key = %s, sha256 = %s,
                    file_size_bytes = %s, actual_rows = %s,
                    finished_at = now(), updated_at = now(), last_error = NULL
                WHERE part_number = %s;
                """,
                (s3_key, sha256, file_size_bytes, actual_rows, part_number),
            )
            conn.commit()

    def mark_failed(self, part_number: int, error: str) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE crawl.dataset_part_queue
                SET status = 'failed', last_error = %s, updated_at = now()
                WHERE part_number = %s;
                """,
                (error[:2000], part_number),
            )
            conn.commit()
