"""
Module: bronze_crawler_core.py
Core logic cua Bronze crawler cho dataset alonhadat (partN.parquet).

Module nay KHONG phu thuoc truc tiep vao psycopg2/boto3/requests -> co the
unit-test bang cach "tiem" (dependency injection) cac ham gia lap CDN/S3/DB,
khong can may that / mang / Postgres / AWS khi chay test.

Nguyen tac thiet ke (theo dung cac quyet dinh da chot voi hoc vien):
- part_number la khoa duy nhat -> tu chong down lap (idempotent theo thiet ke).
- Thu tu xu ly LUON tang dan theo part_number (part10 luon dung sau part2).
- 1 part loi KHONG chan cac part sau (mac dinh) -> nhung co circuit breaker:
  qua N loi LIEN TIEP thi dung ca luot chay, de lai cho lan sau retry tiep
  (day la GIA DINH HIEN TAI, co the doi neu yeu cau thay doi).
- Lan chay sau se tu dong: (a) resume nhung part con 'pending'/'failed' cua
  lan truoc, (b) do tim part MOI xuat hien (part78, part79, ...) TRUOC KHI
  xu ly, vi dataset co the duoc bo sung them part moi trong tuong lai.

GHI CHU: co 1 tham so `max_parts_per_run` trong ham run_crawl() duoc danh dau
"[CHI DUNG DE TEST]" - dung de gioi han so part xu ly trong 1 lan chay (test
an toan qua Airflow ma khong can seed/xoa tay Postgres). Mac dinh la None
(khong gioi han), khong anh huong hanh vi production. Xem chi tiet comment
tai vi tri khai bao tham so nay trong ham run_crawl().
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional, Protocol


# -----------------------------------------------------------------------------
# Interface cho noi luu state (Postgres o production, in-memory o test)
# -----------------------------------------------------------------------------
class PartQueueStore(Protocol):
    def get_max_known_part(self) -> int:
        """So part_number lon nhat da tung biet den (0 neu chua co gi)."""
        ...

    def insert_new_parts(self, part_numbers: list) -> None:
        """Them cac part_number moi vao queue voi status='pending'. Idempotent."""
        ...

    def reset_stuck_downloading(self, older_than_minutes: int) -> int:
        """Dua cac part dang ket o 'downloading' qua lau (crash truoc do) ve 'pending'."""
        ...

    def list_processable_parts(self) -> list:
        """Danh sach part_number co status pending/failed, sap xep TANG DAN."""
        ...

    def claim_part(self, part_number: int, run_id) -> bool:
        """Danh dau 1 part la 'downloading'. Tra False neu khong claim duoc."""
        ...

    def mark_success(self, part_number: int, *, s3_key: str, sha256: str,
                      file_size_bytes: int, actual_rows: Optional[int]) -> None:
        ...

    def mark_failed(self, part_number: int, error: str) -> None:
        ...


# -----------------------------------------------------------------------------
# Retry + exponential backoff (dung chung cho download)
# -----------------------------------------------------------------------------
def retry_with_backoff(
    fn: Callable[[], bytes],
    *,
    retries: int = 3,
    base_delay: float = 2.0,
    max_delay: float = 30.0,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> bytes:
    """
    Goi fn() toi da (retries + 1) lan. Delay tang dan theo cap so nhan (2, 4, 8, ...),
    gioi han o max_delay. Neu het luot ma van loi -> nem lai exception cuoi cung.
    sleep_fn co the thay bang ham "gia" (khong sleep that) khi chay unit test,
    de test chay nhanh thay vi phai cho that.
    """
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            last_exc = e
            if attempt < retries:
                delay = min(base_delay * (2 ** attempt), max_delay)
                sleep_fn(delay)
    raise last_exc  # type: ignore[misc]


# -----------------------------------------------------------------------------
# Kham pha part moi: do tu max_known_part + 1 tro di cho toi khi khong con
# -----------------------------------------------------------------------------
def discover_new_parts(
    max_known_part: int,
    part_exists_fn: Callable[[int], bool],
    *,
    max_probe: int = 200,
) -> list:
    """
    Do lien tuc tu max_known_part + 1. Dung ngay khi gap 1 so KHONG ton tai
    (part_exists_fn tra False) - vi nguon danh so lien tuc, khong co "lo hong".
    max_probe la gioi han an toan de tranh vong lap vo han neu nguon loi/doi hanh vi.
    """
    new_parts = []
    candidate = max_known_part + 1
    probed = 0
    while probed < max_probe:
        if part_exists_fn(candidate):
            new_parts.append(candidate)
            candidate += 1
            probed += 1
        else:
            break
    return new_parts


# -----------------------------------------------------------------------------
# Ket qua 1 lan chay crawl, de log/kiem tra/unit-test
# -----------------------------------------------------------------------------
@dataclass
class CrawlRunResult:
    new_parts_discovered: list = field(default_factory=list)
    processed: list = field(default_factory=list)
    succeeded: list = field(default_factory=list)
    failed: list = field(default_factory=list)
    circuit_breaker_tripped: bool = False
    stopped_at_part: Optional[int] = None
    discovery_error: Optional[str] = None  # loi khi kham pha part moi (VD: mang, DNS...)


# -----------------------------------------------------------------------------
# Ham dieu phoi chinh - duoc goi boi 1 Airflow task DUY NHAT (download_parts_sequential)
# -----------------------------------------------------------------------------
def run_crawl(
    *,
    store: PartQueueStore,
    part_exists_fn: Callable[[int], bool],
    download_fn: Callable[[int], bytes],
    verify_fn: Callable[[int, bytes], dict],
    upload_fn: Callable[[int, bytes], dict],
    run_id,
    batch_size: int = 10,
    consecutive_failure_limit: int = 3,
    per_part_retries: int = 3,
    base_delay: float = 2.0,
    delay_between_downloads: float = 0.0,
    stuck_reset_minutes: int = 30,
    sleep_fn: Callable[[float], None] = time.sleep,
    # === [CHI DUNG DE TEST] ===================================================
    # Gioi han SO PART TOI DA xu ly trong 1 lan goi run_crawl(), bat ke queue
    # con bao nhieu part 'pending'/'failed'. Muc dich: test an toan qua Airflow
    # (VD: chi thu 2-3 part) MA KHONG can seed/xoa tay du lieu trong Postgres.
    #
    # Mac dinh None = KHONG gioi han (dung production, xu ly het queue nhu binh
    # thuong). De BO HAN tham so nay: xoa tham so nay trong signature + xoa
    # khoi code ngay duoi dong "parts_to_process = store.list_processable_parts()"
    # (tim dong co danh dau "[CHI DUNG DE TEST]" o duoi).
    max_parts_per_run: Optional[int] = None,
    # ===========================================================================
) -> CrawlRunResult:
    """
    Quy trinh 1 lan chay:
      1) Reset cac part ket o 'downloading' qua lau (nghi la crash truoc do).
      2) Kham pha part MOI xuat hien tren nguon (78, 79, ...) va them vao queue
         (chi INSERT neu chua co - idempotent, khong dong lai part da 'success').
      3) Lay danh sach part can xu ly (pending/failed), da sap xep TANG DAN.
      4) Xu ly THEO BATCH (mac dinh 10 part/batch - chi de gom log, KHONG anh
         huong toi thu tu xu ly). Voi tung part: claim -> download (co retry +
         backoff) -> verify -> upload -> mark_success/mark_failed.
      5) Dem SO LOI LIEN TIEP (KHONG reset lai theo tung batch) - qua nguong
         thi DUNG NGAY (circuit breaker), de lai cac part con lai o trang thai
         cu (pending/failed) cho lan chay sau tu resume tiep.
    """
    result = CrawlRunResult()

    store.reset_stuck_downloading(stuck_reset_minutes)

    max_known = store.get_max_known_part()
    try:
        new_parts = discover_new_parts(max_known, part_exists_fn)
        if new_parts:
            store.insert_new_parts(new_parts)
        result.new_parts_discovered = new_parts
    except Exception as e:  # noqa: BLE001
        # QUAN TRONG: KHONG duoc coi loi discovery (VD: mang/DNS) la "khong co
        # part moi" - phai ghi nhan RO RANG vao discovery_error de nguoi goi
        # (Airflow task) biet ma bao loi, thay vi "thanh cong" ma xu ly 0 part.
        result.discovery_error = str(e)

    # Van tiep tuc xu ly cac part CU con 'pending'/'failed' trong queue (neu co),
    # ke ca khi buoc discovery loi - vi 2 viec nay doc lap nhau.
    parts_to_process = store.list_processable_parts()

    # === [CHI DUNG DE TEST] =====================================================
    # Cat bot danh sach part can xu ly neu co gioi han max_parts_per_run.
    # Cac part BI CAT VAN GIU NGUYEN trang thai cu trong Postgres (khong dong,
    # khong bi anh huong gi) - lan chay sau (khong truyen max_parts_per_run,
    # hoac truyen so lon hon) se tu xu ly tiep phan con lai binh thuong.
    if max_parts_per_run is not None:
        parts_to_process = parts_to_process[:max_parts_per_run]
    # =============================================================================

    consecutive_failures = 0

    for batch_start in range(0, len(parts_to_process), batch_size):
        batch = parts_to_process[batch_start: batch_start + batch_size]

        for part_number in batch:
            if not store.claim_part(part_number, run_id):
                continue  # bi claim boi tien trinh khac (an toan cho multi-worker sau nay)

            result.processed.append(part_number)

            try:
                content = retry_with_backoff(
                    lambda pn=part_number: download_fn(pn),
                    retries=per_part_retries,
                    base_delay=base_delay,
                    sleep_fn=sleep_fn,
                )
                verify_info = verify_fn(part_number, content)
                upload_info = upload_fn(part_number, content)

                store.mark_success(
                    part_number,
                    s3_key=upload_info["s3_key"],
                    sha256=upload_info["sha256"],
                    file_size_bytes=len(content),
                    actual_rows=verify_info.get("actual_rows"),
                )
                result.succeeded.append(part_number)
                consecutive_failures = 0

            except Exception as e:  # noqa: BLE001
                store.mark_failed(part_number, str(e))
                result.failed.append(part_number)
                consecutive_failures += 1

                if consecutive_failures >= consecutive_failure_limit:
                    result.circuit_breaker_tripped = True
                    result.stopped_at_part = part_number
                    return result  # dung ngay, KHONG xu ly tiep cac part con lai

            if delay_between_downloads > 0:
                sleep_fn(delay_between_downloads)

    return result
