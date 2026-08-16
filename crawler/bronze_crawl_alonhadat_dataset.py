"""
DAG: bronze_crawl_alonhadat_dataset

Muc dich: Crawl DINH KY cho phan PART 51+ cua dataset alonhadat - tu dong
resume khi crash, tu dong phat hien part MOI xuat hien, TU DONG QUET VA BO
SUNG cac part BI THIEU O GIUA khoang da biet (xem scan_and_fill_gaps() trong
bronze_crawler_core.py), TU DONG DOI CHIEU voi S3 THAT de phat hien truong
hop file bi xoa THU CONG (xem reconcile_missing_storage_objects()), va bao
loi ro rang khi con part khong tai duoc sau het so lan retry.

Day la DAG PHAN 2 trong 2 DAG (xem bronze_initial_load_alonhadat_dataset.py
cho phan NAP GOC part 1-50). Chay DAG nay SAU KHI DAG nap goc da hoan tat.

=== CO CHE MO PHONG "PART MOI PHAT SINH DAN" (chi de demo/bao cao do an) ===
Dataset that alonhadat CO DINH 77 part - de demo tinh nang "phat hien part
moi + retry part loi qua nhieu lan trigger DAG" mot cach thuyet phuc, DAG
nay dung 1 Airflow Variable (`alonhadat_sim_max_visible_part`) de GIOI HAN
TAM NHIN cua crawler toi 1 nguong nho hon 77, va TANG DAN 1-5 moi lan DAG
chay (xem task prepare_part_visibility) - mo phong dung nhu nguon dang
"cong bo them du lieu theo thoi gian".

DE CHUYEN SANG PRODUCTION THAT (khi dataset that su co the vuot qua 77 sau
nay, VA KHONG can mo phong nua): set Airflow Variable
`alonhadat_simulation_mode` = "false" (mac dinh "true" neu chua tung set).
Khi do task prepare_part_visibility se tra ve None, va DAG se dung THANG
part_exists_on_source (khong bi boc/gioi han) - hanh vi giong het 1 crawler
production binh thuong, tu kham pha bat ky part nao THAT SU ton tai tren CDN.

Cau truc S3: s3://<bucket>/bronze/<yyyy-MM-dd>/crawl-<n>/partN.parquet (+ manifest.json)

Task "download_parts_sequential" la 1 TASK DUY NHAT xu ly TOAN BO cac part
con lai (pending/failed) + part moi phat hien, THEO VONG LAP TUAN TU (khong
tach thanh N task rieng) - de tan dung ca 2 lop chiu loi:
  - Airflow task-level retry: neu ca task crash, Airflow tu goi lai task;
    nho state nam trong Postgres, lan chay moi TU DONG resume dung cho bi
    bo do, khong tai lai tu dau.
  - Circuit breaker trong logic: neu loi lien tiep qua nguong (mac dinh 3),
    dung SOM va de lai cho lan chay theo lich tiep theo.

GIA DINH HIEN TAI (co the doi neu yeu cau thay doi):
  - 1 part loi KHONG chan cac part sau no trong CUNG 1 luot chay (tru khi
    kich hoat circuit breaker do loi lien tiep).
  - Moi lan chay dinh ky vua (a) mo rong tam nhin them 1-5 part (mo phong
    "part moi sinh ra"), vua (b) TU DONG retry lai cac part 'failed' tu cac
    lan chay truoc do (khong can lam gi them - day la hanh vi mac dinh cua
    list_processable_parts(), da bao gom ca status='pending' va 'failed').
"""

from __future__ import annotations

import logging
import random
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable

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

# --- Cau hinh mo phong (xem giai thich chi tiet o docstring dau file) ---
SIM_MODE_VARIABLE = "alonhadat_simulation_mode"          # "true" / "false", mac dinh "true"
SIM_MAX_VISIBLE_VARIABLE = "alonhadat_sim_max_visible_part"  # so nguyen, mac dinh 50
SIM_INITIAL_MAX_VISIBLE = 50   # phai KHOP voi INITIAL_LOAD_MAX_PART trong DAG nap goc
SIM_INCREMENT_MIN = 1
SIM_INCREMENT_MAX = 5
SIM_DATASET_TRUE_MAX_PART = 77  # tran mo phong (== tong so part THAT cua dataset hien tai)


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
    dag_id="bronze_crawl_alonhadat_dataset",
    schedule="0 2 * * *",  # 2h sang moi ngay - production that. Van co the trigger thu cong bat ky luc nao de demo.
    start_date=datetime(2026, 8, 1, tzinfo=GMT7),
    catchup=False,
    max_active_runs=1,  # KHONG cho 2 lan chay chong nhau (dung chung 1 queue trong Postgres)
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    on_failure_callback=_on_dag_failure,
    tags=["bronze", "crawler", "alonhadat", "periodic"],
)
def bronze_crawl_alonhadat_dataset():

    @task
    def prepare_part_visibility(**context) -> Optional[int]:
        """
        CHE DO MO PHONG (mac dinh, Variable `alonhadat_simulation_mode` chua
        set hoac = "true"): doc nguong tam nhin hien tai tu Airflow Variable,
        TANG them 1-5 (ngau nhien, hoac lay tu dag_run.conf={"increment": N}
        neu trigger thu cong voi config cu the de demo co kiem soat), gioi
        han tran o SIM_DATASET_TRUE_MAX_PART, ghi lai Variable, tra ve nguong
        moi.

        CHE DO PRODUCTION THAT (Variable `alonhadat_simulation_mode` = "false"):
        tra ve None - bao hieu cho download_parts_sequential KHONG boc/gioi
        han part_exists_fn, dung thang ham that.
        """
        sim_mode = Variable.get(SIM_MODE_VARIABLE, default_var="true").strip().lower() == "true"
        if not sim_mode:
            logger.info(
                "[PRODUCTION] simulation_mode=false -> KHONG gioi han tam nhin, "
                "dung truc tiep tinh trang that cua CDN."
            )
            return None

        dag_run = context.get("dag_run")
        conf = (dag_run.conf if dag_run else None) or {}
        increment = conf.get("increment")
        if increment is None:
            increment = random.randint(SIM_INCREMENT_MIN, SIM_INCREMENT_MAX)

        current = int(Variable.get(SIM_MAX_VISIBLE_VARIABLE, default_var=SIM_INITIAL_MAX_VISIBLE))
        new_max = min(current + int(increment), SIM_DATASET_TRUE_MAX_PART)
        Variable.set(SIM_MAX_VISIBLE_VARIABLE, new_max)

        logger.info(
            "[MO PHONG] max_visible_part: %s -> %s (tang %s, tran mo phong=%s). "
            "De chuyen production that: set Airflow Variable '%s' = 'false'.",
            current, new_max, increment, SIM_DATASET_TRUE_MAX_PART, SIM_MODE_VARIABLE,
        )
        return new_max

    @task
    def start_run() -> dict:
        run_info = start_crawl_run()
        logger.info(
            "Bat dau crawl_run id=%s run_date=%s crawl-%s",
            run_info["run_id"], run_info["run_date"], run_info["run_no"],
        )
        return run_info

    @task
    def download_parts_sequential(run_info: dict, max_visible_part: Optional[int]) -> dict:
        """
        Task DUY NHAT xu ly toan bo vong doi: doi chieu S3 -> quet lo hong ->
        discover part moi -> tai TUAN TU tung part -> verify -> upload -> cap
        nhat status. Dung SOM neu circuit breaker kich hoat.
        """
        store = PostgresPartQueueStore()
        load_date = run_info["run_date"]
        crawl_no = run_info["run_no"]

        if max_visible_part is not None:
            effective_part_exists_fn = make_capped_part_exists_fn(
                part_exists_on_source, max_visible_part
            )
        else:
            effective_part_exists_fn = part_exists_on_source  # PRODUCTION THAT

        result = run_crawl(
            store=store,
            part_exists_fn=effective_part_exists_fn,
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
            # dung khi muon test an toan tren vai part truoc khi tha chay full.
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
        # khong xu ly duoc part nao -> day la 1 lan chay VO ICH, PHAI bao loi
        # ro rang thay vi de task "thanh cong" voi 0 part.
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
    def finalize(summary: dict) -> None:
        """
        Cap nhat crawl_run voi ket qua cuoi cung. Neu con part 'failed' sau
        het retry -> RAISE de task nay that bai, keo theo DAG that bai va
        kich hoat on_failure_callback.
        """
        status = finalize_crawl_run(
            summary["run_id"], summary["success_count"], summary["failed_count"], dsn=DW_DSN
        )
        logger.info("Crawl-run hoan tat voi status=%s", status)

        if summary["failed_count"] > 0:
            raise AirflowException(
                f"[CANH BAO] Con {summary['failed_count']} part loi sau het retry: "
                f"{summary['failed_parts']}. Xem chi tiet trong "
                f"crawl.dataset_part_queue (status='failed', cot last_error)."
            )

    max_visible_part = prepare_part_visibility()
    run_info = start_run()
    summary = download_parts_sequential(run_info, max_visible_part)
    finalize(summary)


bronze_crawl_alonhadat_dataset()
