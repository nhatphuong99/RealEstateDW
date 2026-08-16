"""
DAG: bronze_initial_load_alonhadat_dataset

Muc dich: NAP GOC (initial bulk load) cho PART 1-50 cua dataset alonhadat -
chay MOT LAN DUY NHAT (thu cong, khong theo lich) de mo phong "lan load dau
tien len S3" truoc khi chuyen sang giai doan crawl DINH KY (xem DAG
bronze_crawl_alonhadat_dataset.py, xu ly PHAN part 51+ nhu la du lieu
"phat sinh dan theo thoi gian").

VI SAO TACH RIENG DAG NAY (khong dung chung DAG dinh ky):
- Muc dich nghiep vu khac nhau: day la "nap 1 khoi du lieu da biet truoc"
  (giong nhu backfill), khong phai "kiem tra dinh ky xem co gi moi khong".
- schedule=None (CHI trigger thu cong) - chay lai nhieu lan (VD lam lai demo
  tu dau) khong gay hai gi (idempotent: insert_new_parts dung ON CONFLICT DO
  NOTHING, cac part da 'success' se KHONG bi tai lai).
- GIOI HAN CUNG o part 50 (khong dung discover_new_parts vuot qua 50) de dam
  bao ranh gioi RO RANG giua "du lieu goc" va "du lieu mo phong phat sinh
  sau" phuc vu demo/bao cao do an - xem bronze_crawler_simulation.py.

Cau truc S3: s3://<bucket>/bronze/<yyyy-MM-dd>/crawl-<n>/partN.parquet (+ manifest.json)
(giong het cau truc cua DAG dinh ky - ca 2 DAG dung CHUNG 1 hang doi Postgres
va CHUNG 1 quy uoc S3, chi khac nhau o PHAM VI part va CACH kich hoat).
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

# Xem docker-compose.yaml: volume "./crawler:/opt/airflow/crawler"
sys.path.append("/opt/airflow/crawler")  # noqa: E402

from bronze_crawler_core import run_crawl  # noqa: E402
from bronze_crawler_postgres_store import (  # noqa: E402
    PostgresPartQueueStore,
    DW_DSN,
    start_crawl_run,
    finalize_crawl_run,
)
from bronze_crawler_io import (  # noqa: E402
    part_exists_on_source,
    download_part,
    verify_part,
    upload_part,
    s3_object_exists,
)
from bronze_crawler_simulation import make_capped_part_exists_fn  # noqa: E402

GMT7 = timezone(timedelta(hours=7))
logger = logging.getLogger(__name__)

# Ranh gioi CUNG cua lan nap goc - CHI sua o day neu muon doi pham vi "du lieu
# goc" (VD muon nap goc 1-40 thay vi 1-50) - nho dong bo lai voi tai lieu bao cao.
INITIAL_LOAD_MAX_PART = 50


def _on_dag_failure(context):
    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance")
    logger.error(
        "[CANH BAO] DAG %s that bai o task '%s' (run_id=%s). "
        "Kiem tra bang crawl.dataset_part_queue WHERE status='failed' de biet chi tiet loi.",
        dag_run.dag_id if dag_run else "?",
        task_instance.task_id if task_instance else "?",
        dag_run.run_id if dag_run else "?",
    )


@dag(
    dag_id="bronze_initial_load_alonhadat_dataset",
    schedule=None,  # CHI trigger thu cong - day la thao tac "nap goc 1 lan", khong phai dinh ky
    start_date=datetime(2026, 8, 1, tzinfo=GMT7),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    on_failure_callback=_on_dag_failure,
    tags=["bronze", "crawler", "alonhadat", "initial-load"],
)
def bronze_initial_load_alonhadat_dataset():

    @task
    def seed_and_start_run() -> dict:
        """
        Dua part 1..INITIAL_LOAD_MAX_PART vao queue (idempotent - ON CONFLICT
        DO NOTHING, an toan neu chay lai DAG nhieu lan), roi tao 1 dong
        crawl_run moi. Gop 2 buoc vao 1 task de don gian hoa DAG (khac voi
        DAG dinh ky can tach discovery rieng vi co logic mo phong phuc tap hon).
        """
        store = PostgresPartQueueStore()
        store.insert_new_parts(list(range(1, INITIAL_LOAD_MAX_PART + 1)))

        run_info = start_crawl_run()
        logger.info(
            "Bat dau NAP GOC crawl_run id=%s run_date=%s crawl-%s (pham vi part 1-%d)",
            run_info["run_id"], run_info["run_date"], run_info["run_no"], INITIAL_LOAD_MAX_PART,
        )
        return run_info

    @task
    def download_parts_sequential(run_info: dict) -> dict:
        """
        Tai TUAN TU toan bo part 1..INITIAL_LOAD_MAX_PART con 'pending'/'failed'.
        part_exists_fn duoc BOC (cap) o muc INITIAL_LOAD_MAX_PART, de dam bao
        DAG nay TUYET DOI khong vo tinh pham sang pham vi part 51+ (du CDN
        that co san toan bo 77 part) - giu ranh gioi demo ro rang.
        """
        store = PostgresPartQueueStore()
        load_date = run_info["run_date"]
        crawl_no = run_info["run_no"]

        capped_part_exists_fn = make_capped_part_exists_fn(
            part_exists_on_source, INITIAL_LOAD_MAX_PART
        )

        result = run_crawl(
            store=store,
            part_exists_fn=capped_part_exists_fn,
            download_fn=download_part,
            verify_fn=verify_part,
            upload_fn=lambda pn, content: upload_part(
                pn, content, load_date=load_date, crawl_no=crawl_no
            ),
            run_id=run_info["run_id"],
            batch_size=10,
            consecutive_failure_limit=5,   # nap goc: cho phep loi lien tiep nhieu hon 1 chut
            per_part_retries=3,
            base_delay=2.0,
            delay_between_downloads=0.3,
            stuck_reset_minutes=30,
            s3_object_exists_fn=s3_object_exists,
            gap_scan_enabled=True,   # van tu chua lanh NEU chinh lan nap goc nay bi lo hong giua chung
        )

        logger.info(
            "Ket qua NAP GOC: LO HONG duoc bo sung=%s, da xu ly=%d, thanh cong=%d, "
            "that bai=%d, circuit_breaker_tripped=%s, discovery_error=%s, reconcile_error=%s",
            result.gap_parts_found, len(result.processed), len(result.succeeded),
            len(result.failed), result.circuit_breaker_tripped,
            result.discovery_error, result.reconcile_error,
        )

        return {
            "run_id": run_info["run_id"],
            "success_count": len(result.succeeded),
            "failed_count": len(result.failed),
            "failed_parts": result.failed,
        }

    @task
    def finalize(summary: dict) -> None:
        status = finalize_crawl_run(
            summary["run_id"], summary["success_count"], summary["failed_count"], dsn=DW_DSN
        )
        logger.info("NAP GOC hoan tat voi status=%s", status)

        if summary["failed_count"] > 0:
            raise AirflowException(
                f"[CANH BAO] NAP GOC con {summary['failed_count']} part loi: "
                f"{summary['failed_parts']}. Xem chi tiet trong "
                f"crawl.dataset_part_queue (status='failed', cot last_error). "
                f"Co the trigger lai chinh DAG nay de retry (idempotent)."
            )

    run_info = seed_and_start_run()
    summary = download_parts_sequential(run_info)
    finalize(summary)


bronze_initial_load_alonhadat_dataset()
