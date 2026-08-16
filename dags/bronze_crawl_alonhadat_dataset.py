"""
DAG: bronze_crawl_alonhadat_dataset

Muc dich: Tai du lieu tho (partN.parquet) tu CDN nguon ve S3 Bronze layer,
tu dong resume khi crash, tu dong phat hien part MOI xuat hien (part78,
part79, ...), TU DONG QUET VA BO SUNG cac part BI THIEU O GIUA khoang da
biet (VD: part3, part23-24, part27-29 bi thieu du da biet toi part77 - xem
scan_and_fill_gaps() trong bronze_crawler_core.py), TU DONG DOI CHIEU voi S3
THAT de phat hien truong hop file bi xoa THU CONG (VD tren S3 console) trong
khi Postgres van con ghi 'success' (xem reconcile_missing_storage_objects()),
va bao loi ro rang khi con part khong tai duoc sau het so lan retry.

Cau truc S3: s3://<bucket>/bronze/<yyyy-MM-dd>/crawl-<n>/partN.parquet (+ manifest.json)

Task "download_parts_sequential" la 1 TASK DUY NHAT xu ly TOAN BO cac part
con lai (pending/failed) + part moi phat hien, THEO VONG LAP TUAN TU (khong
tach thanh N task rieng) - de tan dung ca 2 lop chiu loi:
  - Airflow task-level retry: neu ca task crash (VD het RAM, container chet
    giua chung), Airflow tu goi lai task; nho state nam trong Postgres
    (khong phai bien trong RAM cua task), lan chay moi TU DONG resume dung
    cho bi bo do, khong tai lai tu dau.
  - Circuit breaker trong logic (xem bronze_crawler_core.py): neu loi lien
    tiep qua nguong (mac dinh 3), dung SOM va de lai cho lan chay theo lich
    tiep theo, tranh co gang vo ich khi nguon dang gap su co dien rong.

GIA DINH HIEN TAI (co the doi neu yeu cau thay doi):
  - 1 part loi KHONG chan cac part sau no trong CUNG 1 luot chay (tru khi
    kich hoat circuit breaker do loi lien tiep).
  - Lich chay hang ngay chu yeu de: (a) dam bao hoan tat cac part con thieu,
    (b) phat hien part moi neu nguon duoc bo sung du lieu.
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
from bronze_crawler_postgres_store import PostgresPartQueueStore, DW_DSN  # noqa: E402
from bronze_crawler_io import (  # noqa: E402
    part_exists_on_source,
    download_part,
    verify_part,
    upload_part,
    s3_object_exists,
)

GMT7 = timezone(timedelta(hours=7))
logger = logging.getLogger(__name__)


def _on_dag_failure(context):
    """
    Placeholder canh bao khi DAG that bai. Hien tai chi log ro rang; sau nay
    co the noi them Slack/Email tai day (VD: goi webhook Slack, hoac dung
    EmailOperator/SlackWebhookOperator trong 1 task rieng duoc goi tu day).
    """
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
    dag_id="bronze_crawl_alonhadat_dataset",
    schedule="0 2 * * *",  # 2h sang moi ngay - kiem tra timezone cua container Airflow
    start_date=datetime(2026, 8, 1, tzinfo=GMT7),
    catchup=False,
    max_active_runs=1,  # KHONG cho 2 lan chay chong nhau (dung chung 1 queue trong Postgres)
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    on_failure_callback=_on_dag_failure,
    tags=["bronze", "crawler", "alonhadat"],
)
def bronze_crawl_alonhadat_dataset():

    @task
    def start_crawl_run() -> dict:
        """Tao 1 dong crawl_run moi cho lan chay hom nay, tra ve run_id + crawl_no."""
        import psycopg2

        run_date = datetime.now(GMT7).date()
        with psycopg2.connect(DW_DSN) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(run_no), 0) + 1 FROM crawl.crawl_run WHERE run_date = %s;",
                (run_date,),
            )
            run_no = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO crawl.crawl_run (run_date, run_no, status)
                VALUES (%s, %s, 'running')
                RETURNING id;
                """,
                (run_date, run_no),
            )
            run_id = cur.fetchone()[0]
            conn.commit()

        logger.info("Bat dau crawl_run id=%s run_date=%s crawl-%s", run_id, run_date, run_no)
        return {"run_id": run_id, "run_date": str(run_date), "run_no": run_no}

    @task
    def download_parts_sequential(run_info: dict) -> dict:
        """
        Task DUY NHAT xu ly toan bo vong doi: reset stuck -> discover part moi
        -> tai TUAN TU tung part theo dung thu tu tang dan -> verify -> upload
        -> cap nhat status. Dung SOM neu circuit breaker kich hoat.
        """
        store = PostgresPartQueueStore()
        load_date = run_info["run_date"]
        crawl_no = run_info["run_no"]

        result = run_crawl(
            store=store,
            part_exists_fn=part_exists_on_source,
            download_fn=download_part,
            verify_fn=verify_part,
            upload_fn=lambda pn, content: upload_part(
                pn, content, load_date=load_date, crawl_no=crawl_no
            ),
            run_id=run_info["run_id"],
            batch_size=10,
            consecutive_failure_limit=3,   # VD: loi lien tiep 3 part thi dung
            per_part_retries=3,            # retry toi da 3 lan / part, backoff tang dan
            base_delay=2.0,
            delay_between_downloads=0.5,   # gioi han toc do nhe giua cac lan tai
            stuck_reset_minutes=30,
            s3_object_exists_fn=s3_object_exists,  # doi chieu 'success' voi S3 that
            # === [CHI DUNG DE TEST] ===============================================
            # Bo comment dong duoi de GIOI HAN so part xu ly trong 1 lan chay DAG,
            # dung khi muon test an toan tren vai part truoc khi tha chay full
            # 77+ part - KHONG can seed/xoa tay du lieu trong Postgres.
            # Nho XOA/COMMENT LAI truoc khi chay production that.
            # max_parts_per_run=3,
            # =======================================================================
        )

        logger.info(
            "Ket qua crawl: DOI CHIEU S3 phat hien mat file=%s, LO HONG duoc bo sung=%s, "
            "part MOI phat hien=%s, da xu ly=%d, thanh cong=%d, that bai=%d, "
            "circuit_breaker_tripped=%s, discovery_error=%s, reconcile_error=%s",
            result.reconciled_missing_parts, result.gap_parts_found, result.new_parts_discovered,
            len(result.processed), len(result.succeeded), len(result.failed),
            result.circuit_breaker_tripped, result.discovery_error, result.reconcile_error,
        )

        # Neu buoc kham pha part moi bi loi (VD: mat ket noi toi CDN) VA cung
        # khong xu ly duoc part nao (queue dang trong hoac cung khong con
        # pending) -> day la 1 lan chay VO ICH, PHAI bao loi ro rang thay vi
        # de task "thanh cong" voi 0 part (day chinh la loi da xay ra truoc do).
        if result.discovery_error and not result.processed:
            raise AirflowException(
                f"Khong kham pha duoc part moi VA khong con part nao trong "
                f"queue de xu ly. Loi kham pha: {result.discovery_error}. "
                f"Kiem tra ket noi mang tu container airflow-worker toi CDN nguon "
                f"(vi du: docker compose exec airflow-worker curl -I "
                f"https://cdn.cuhuuhoang.com/alonhadat/part1.parquet)."
            )

        return {
            "run_id": run_info["run_id"],
            "success_count": len(result.succeeded),
            "failed_count": len(result.failed),
            "failed_parts": result.failed,
            "circuit_breaker_tripped": result.circuit_breaker_tripped,
            "discovery_error": result.discovery_error,
        }

    @task
    def finalize_crawl_run(summary: dict) -> None:
        """
        Cap nhat crawl_run voi ket qua cuoi cung. Neu con part 'failed' sau
        het retry -> RAISE de task nay that bai, keo theo DAG that bai va
        kich hoat on_failure_callback (noi se noi Slack/Email sau nay).
        """
        import psycopg2

        run_id = summary["run_id"]
        failed_count = summary["failed_count"]
        status = "completed" if failed_count == 0 else "completed_with_errors"

        with psycopg2.connect(DW_DSN) as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE crawl.crawl_run
                SET status = %s, success_parts = %s, failed_parts = %s, finished_at = now()
                WHERE id = %s;
                """,
                (status, summary["success_count"], failed_count, run_id),
            )
            conn.commit()

        if failed_count > 0:
            raise AirflowException(
                f"[CANH BAO] Con {failed_count} part loi sau het retry: "
                f"{summary['failed_parts']}. Xem chi tiet trong "
                f"crawl.dataset_part_queue (status='failed', cot last_error)."
            )

    run_info = start_crawl_run()
    summary = download_parts_sequential(run_info)
    finalize_crawl_run(summary)


bronze_crawl_alonhadat_dataset()
