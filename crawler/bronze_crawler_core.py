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
- NGUON CO THE CO "LO HONG" (VD: part3, part23, part24, part27-29 bi thieu
  du part1..part77 da duoc biet den) - KHONG duoc gia dinh nguon danh so
  lien tuc tuyet doi.
- DU LIEU CO THE BI MAT SAU KHI DA "success" theo 2 CACH DOC LAP NHAU, moi
  cach can 1 co che phat hien RIENG (khong the dung chung 1 logic):
    (a) AI DO XOA DONG trong Postgres (crawl.dataset_part_queue) -> dong do
        bien mat hoan toan -> scan_and_fill_gaps() phat hien duoc, vi no
        chi dua vao "co dong trong Postgres hay khong".
    (b) AI DO XOA FILE tren S3, NHUNG dong Postgres VAN CON status='success'
        -> scan_and_fill_gaps() KHONG phat hien duoc (vi dong van "co" trong
        Postgres) -> can 1 co che RIENG: reconcile_missing_storage_objects()
        - kiem tra TUNG part dang 'success' xem file S3 phia sau co THAT SU
        con ton tai khong (khong chi tin vao trang thai ghi trong Postgres).
  Vi vay co 3 co che kham pha/doi chieu DOC LAP nhau, deu chay o MOI lan crawl:
    (a) scan_and_fill_gaps()               - dong Postgres bi xoa
    (b) reconcile_missing_storage_objects() - file S3 bi xoa
    (c) discover_new_parts()               - phan vuot bien (78, 79, ...),
        dung khi gap DU SO LAN LIEN TIEP khong ton tai (miss_tolerance),
        KHONG dung ngay o lan thieu dau tien (lo hong gan bien co the khien
        dung qua som, bo lo cac part that su moi o xa hon).
- Lan chay sau se tu dong resume nhung part con 'pending'/'failed' cua lan truoc.

GHI CHU: co 1 tham so `max_parts_per_run` trong ham run_crawl() duoc danh dau
"[CHI DUNG DE TEST]" - dung de gioi han so part xu ly trong 1 lan chay (test
an toan qua Airflow ma khong can seed/xoa tay Postgres). Mac dinh la None
(khong gioi han), khong anh huong hanh vi production. Xem chi tiet comment
tai vi tri khai bao tham so nay trong ham run_crawl().

BUG THUC TE DA GAP VA SUA (17/08/2026): buoc "kiem tra ton tai" (dung boi ca
3 co che scan/discover/reconcile) truoc day KHONG duoc retry - 1 lan goi
mang bi loi TAM THOI (VD Cloudflare tra HTTP 530 "khong ket noi duoc toi
origin", KHONG lien quan gi den viec part do co ton tai hay khong) se lam
that bai ngay ca crawl-run, DU hang doi hoan toan khong co gi ton dong. Da
them retry_with_backoff cho rieng buoc kiem tra ton tai (xem _retrying_predicate,
tham so probe_retries/probe_base_delay trong run_crawl()) - tach biet voi
retry cua buoc TAI FILE (per_part_retries/base_delay) vi 2 loai thao tac co
dac tinh khac nhau (kiem tra ton tai nhe & nhanh, khong can retry nhieu nhu tai file).
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

    def get_known_part_numbers(self) -> set:
        """
        TOAN BO part_number da tung duoc ghi nhan trong queue (bat ke status).
        Dung de tinh cac "lo hong" - so nam trong [1, max_known_part] nhung
        CHUA TUNG duoc dua vao queue (khac voi status='failed' - failed la
        DA BIET nhung tai loi, con lo hong la CHUA TUNG duoc biet den).
        """
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

    def list_success_parts_with_keys(self) -> list:
        """
        Danh sach (part_number, s3_key) cua TAT CA part dang status='success'.
        Dung cho reconcile_missing_storage_objects() - can s3_key de kiem tra
        file THAT SU con ton tai tren S3 hay khong, khong chi tin vao trang
        thai da ghi san trong Postgres.
        """
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


def _retrying_predicate(
    fn: Callable[[int], bool],
    *,
    retries: int,
    base_delay: float,
    sleep_fn: Callable[[float], None],
) -> Callable[[int], bool]:
    """
    Boc 1 ham kiem tra ton tai (nhan 1 candidate, tra bool) bang
    retry_with_backoff - de LOI HTTP TAM THOI tren 1 lan goi don le (VD
    Cloudflare tra 530 "khong ket noi duoc toi origin", DNS timeout thoang
    qua...) KHONG lam that bai ngay ca qua trinh discover/scan/reconcile, ma
    duoc thu lai vai lan (delay tang dan) truoc khi thuc su bao loi.

    BUG THUC TE DA GAP (16/08/2026): discover_new_parts() probe candidate
    (VD part 78) gap Cloudflare 530 (loi ha tang tam thoi, KHONG lien quan
    gi den viec part 78 co ton tai hay khong) -> truoc day loi nay lan truyen
    NGAY LAP TUC ra ngoai (khong retry) -> ca crawl-run that bai chi vi 1 lan
    goi mang bi trung dung luc mang chap chon, du hang doi hoan toan khong co
    gi ton dong. Them retry o DIEM GOI thay vi o tung ham rieng le
    (part_exists_on_source/s3_object_exists) de giu bronze_crawler_io.py la
    IO THUAN TUY, khong tu quyet dinh chinh sach retry (nhat quan voi cach
    download_fn cung duoc boc retry o run_crawl(), khong tu retry ben trong).

    LUU Y: neu fn(candidate) tra ve False THAT (khong phai raise loi), day la
    1 CAU TRA LOI HOP LE (candidate khong ton tai) - retry_with_backoff chi
    bat Exception, gia tri False duoc tra thang ra ngay lan goi dau, KHONG bi
    retry oan uong.
    """
    def wrapped(candidate: int) -> bool:
        return retry_with_backoff(
            lambda: fn(candidate), retries=retries, base_delay=base_delay, sleep_fn=sleep_fn,
        )

    return wrapped


# -----------------------------------------------------------------------------
# Kham pha part moi: do tu max_known_part + 1 tro di cho toi khi khong con
# -----------------------------------------------------------------------------
def discover_new_parts(
    max_known_part: int,
    part_exists_fn: Callable[[int], bool],
    *,
    max_probe: int = 200,
    miss_tolerance: int = 5,
) -> list:
    """
    Do lien tuc tu max_known_part + 1. KHONG dung ngay khi gap 1 so khong
    ton tai nua (vi nguon co the co "lo hong" ngay ca o phan chua kham pha,
    khong chi o phan da biet) - ma CHI dung khi gap DU SO LAN LIEN TIEP
    khong ton tai (miss_tolerance, mac dinh 5), coi nhu da den cuoi du lieu
    hien co.

    LUU Y QUAN TRONG: cach nay van co the bo sot neu 1 khoang trong that su
    dai hon miss_tolerance (VD 10 so lien tiep bi thieu ngay sau max_known_part).
    Day la ly do BAT BUOC phai ket hop voi scan_and_fill_gaps() - ham do se
    tu dong quet lai va bo sung o CAC LAN CHAY SAU, vi luc do cac so nay da
    "da biet" (nam duoi max_known_part moi) nen se duoc quet toan bo, khong
    con phu thuoc vao mien_tolerance nua.

    max_probe la gioi han an toan tong so lan kiem tra, tranh vong lap qua
    dai neu nguon loi/doi hanh vi bat thuong.
    """
    new_parts = []
    candidate = max_known_part + 1
    consecutive_misses = 0
    probed = 0
    while probed < max_probe and consecutive_misses < miss_tolerance:
        if part_exists_fn(candidate):
            new_parts.append(candidate)
            consecutive_misses = 0
        else:
            consecutive_misses += 1
        candidate += 1
        probed += 1
    return new_parts


# -----------------------------------------------------------------------------
# Quet "lo hong": nhung so trong khoang [1, max_known_part] CHUA TUNG duoc
# dua vao queue - co the do lan quet truoc dung som (miss_tolerance khong du),
# hoac do loi mang TAM THOI ngay luc kiem tra ton tai truoc do.
# -----------------------------------------------------------------------------
def find_gap_candidates(known_part_numbers: set, max_known_part: int) -> list:
    """Tra ve cac so trong [1, max_known_part] CHUA co trong known_part_numbers."""
    return [pn for pn in range(1, max_known_part + 1) if pn not in known_part_numbers]


def scan_and_fill_gaps(
    store: "PartQueueStore",
    part_exists_fn: Callable[[int], bool],
) -> list:
    """
    Quet TOAN BO khoang [1, max_known_part] de tim cac so BI BO SOT (chua
    tung duoc dua vao queue). Voi moi so bi bo sot, kiem tra lai xem co that
    su ton tai tren nguon khong; neu co thi them vao queue voi status='pending'.

    Day la buoc "TU CHUA LANH" (self-healing) - NEN CHAY O MOI LAN CRAWL,
    khong chi 1 lan duy nhat, vi 1 lo hong hom nay co the do loi mang TAM
    THOI (VD CDN timeout dung luc kiem tra) ma lan chay sau kiem tra lai se
    thay ton tai binh thuong.

    Khac voi discover_new_parts() (chi do PHAN VUOT BIEN, ap dung
    miss_tolerance de tranh probe vo han), ham nay kiem tra TUNG SO MOT
    trong khoang DA BIET, khong co gioi han "so lan lien tiep" - vi khoang
    nay huu han (da biet max_known_part) nen chi phi kiem tra la co the
    chap nhan duoc (vai tram request nhieu nhat o quy mo hien tai).
    """
    max_known = store.get_max_known_part()
    if max_known == 0:
        return []

    known_numbers = store.get_known_part_numbers()
    gap_candidates = find_gap_candidates(known_numbers, max_known)

    found_parts = [pn for pn in gap_candidates if part_exists_fn(pn)]
    if found_parts:
        store.insert_new_parts(found_parts)
    return found_parts


# -----------------------------------------------------------------------------
# Doi chieu voi storage that: phat hien truong hop Postgres van ghi 'success'
# NHUNG file S3 phia sau da bi xoa (do con nguoi thao tac truc tiep, khong
# phai do crawler gay ra) - day la truong hop NGUOC voi scan_and_fill_gaps()
# o tren (Postgres bi xoa dong), nen can 1 co che kiem tra RIENG.
# -----------------------------------------------------------------------------
def reconcile_missing_storage_objects(
    store: "PartQueueStore",
    s3_object_exists_fn: Callable[[str], bool],
) -> list:
    """
    Voi MOI part dang o status='success', kiem tra file S3 (theo s3_key da
    luu) co THAT SU con ton tai khong - KHONG chi tin vao trang thai da ghi
    san trong Postgres. Neu file da bi xoa (VD: ai do xoa thu cong tren S3
    console, hoac loi vong doi luu tru/lifecycle policy xoa nham) -> reset
    lai thanh trang thai can xu ly lai (dung chung mark_failed - ve mat ngu
    nghia day khong phai "loi tai" nhung se duoc list_processable_parts()
    nhat lai va tai lai binh thuong o ngay trong lan chay nay).

    LUU Y: nguoc lai voi scan_and_fill_gaps() (xu ly truong hop DONG POSTGRES
    bi xoa), ham nay xu ly truong hop FILE S3 bi xoa nhung dong Postgres van
    con - 2 truong hop nay KHONG the dung chung 1 logic vi diem xuat phat
    kiem tra khac nhau (mot ben nhin tu Postgres, mot ben nhin tu S3 that).
    """
    missing = []
    for part_number, s3_key in store.list_success_parts_with_keys():
        if not s3_key:
            continue  # du lieu cu/thieu s3_key, bo qua de tranh false positive
        if not s3_object_exists_fn(s3_key):
            store.mark_failed(
                part_number,
                "s3_object_missing_reconcile: file da bi xoa khoi S3 (Postgres van ghi success)",
            )
            missing.append(part_number)
    return missing


# -----------------------------------------------------------------------------
# Ket qua 1 lan chay crawl, de log/kiem tra/unit-test
# -----------------------------------------------------------------------------
@dataclass
class CrawlRunResult:
    reconciled_missing_parts: list = field(default_factory=list)  # 'success' nhung file S3 da bi xoa
    gap_parts_found: list = field(default_factory=list)  # so bi thieu O GIUA duoc quet lai va bo sung
    new_parts_discovered: list = field(default_factory=list)  # so MOI o phan vuot bien
    processed: list = field(default_factory=list)
    succeeded: list = field(default_factory=list)
    failed: list = field(default_factory=list)
    circuit_breaker_tripped: bool = False
    stopped_at_part: Optional[int] = None
    discovery_error: Optional[str] = None  # loi khi kham pha part moi (VD: mang, DNS...)
    reconcile_error: Optional[str] = None  # loi khi doi chieu storage (VD: mat quyen truy cap S3...)


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
    gap_scan_enabled: bool = True,
    miss_tolerance: int = 5,
    s3_object_exists_fn: Optional[Callable[[str], bool]] = None,
    reconcile_enabled: bool = True,
    probe_retries: int = 2,       # so lan retry rieng cho BUOC KIEM TRA TON TAI (nhe hon download)
    probe_base_delay: float = 1.0,
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
      2) DOI CHIEU STORAGE: voi cac part dang 'success', kiem tra file S3 co
         THAT SU con ton tai khong (phong truong hop ai do xoa file S3 truc
         tiep nhung Postgres van con ghi 'success') - xem
         reconcile_missing_storage_objects(). Bo qua neu khong truyen
         s3_object_exists_fn (VD: unit test khong can kiem tra S3 that).
      3) QUET LO HONG: tim va bo sung cac so trong [1, max_known_part] CHUA
         TUNG duoc dua vao queue (xem scan_and_fill_gaps). Chay o MOI lan,
         khong chi 1 lan - vi lo hong co the do loi mang tam thoi HOAC do ai
         do xoa dong Postgres truc tiep.
      4) Kham pha part MOI o PHAN VUOT BIEN (78, 79, ...), dung khi gap du
         so lan LIEN TIEP khong ton tai (miss_tolerance) - xem discover_new_parts.
      5) Lay danh sach part can xu ly (pending/failed), da sap xep TANG DAN -
         danh sach nay da bao gom CA cac part vua duoc doi chieu/quet gap o
         buoc 2-3, vi chung duoc dua ve 'pending'/'failed' truoc khi query.
      6) Xu ly THEO BATCH (mac dinh 10 part/batch - chi de gom log, KHONG anh
         huong toi thu tu xu ly). Voi tung part: claim -> download (co retry +
         backoff) -> verify -> upload -> mark_success/mark_failed.
      7) Dem SO LOI LIEN TIEP (KHONG reset lai theo tung batch) - qua nguong
         thi DUNG NGAY (circuit breaker), de lai cac part con lai o trang thai
         cu (pending/failed) cho lan chay sau tu resume tiep.
    """
    result = CrawlRunResult()

    store.reset_stuck_downloading(stuck_reset_minutes)

    # Boc retry cho CA 2 ham kiem tra ton tai - ap dung 1 lan duy nhat o day,
    # dung chung cho ca 3 co che ben duoi (reconcile/gap-scan/discover), thay
    # vi phai sua rieng tung ham. Xem docstring _retrying_predicate() de biet
    # ly do (bug Cloudflare 530 thuc te da gap).
    resilient_part_exists_fn = _retrying_predicate(
        part_exists_fn, retries=probe_retries, base_delay=probe_base_delay, sleep_fn=sleep_fn,
    )
    resilient_s3_object_exists_fn = (
        _retrying_predicate(
            s3_object_exists_fn, retries=probe_retries, base_delay=probe_base_delay, sleep_fn=sleep_fn,
        )
        if s3_object_exists_fn is not None else None
    )

    if reconcile_enabled and resilient_s3_object_exists_fn is not None:
        try:
            result.reconciled_missing_parts = reconcile_missing_storage_objects(
                store, resilient_s3_object_exists_fn
            )
        except Exception as e:  # noqa: BLE001
            # Loi doi chieu storage (VD: mat quyen truy cap S3 tam thoi) KHONG
            # duoc lam dung ca crawl - ghi nhan va tiep tuc, lan sau thu lai.
            result.reconcile_error = f"reconcile_error: {e}"

    if gap_scan_enabled:
        try:
            result.gap_parts_found = scan_and_fill_gaps(store, resilient_part_exists_fn)
        except Exception as e:  # noqa: BLE001
            # Loi quet gap KHONG duoc lam dung ca crawl - ghi nhan va tiep tuc,
            # vi day la co che "tu chua lanh" chay dinh ky, lan sau se thu lai.
            result.discovery_error = f"gap_scan_error: {e}"

    max_known = store.get_max_known_part()
    try:
        new_parts = discover_new_parts(
            max_known, resilient_part_exists_fn, miss_tolerance=miss_tolerance
        )
        if new_parts:
            store.insert_new_parts(new_parts)
        result.new_parts_discovered = new_parts
    except Exception as e:  # noqa: BLE001
        # QUAN TRONG: KHONG duoc coi loi discovery (VD: mang/DNS) la "khong co
        # part moi" - phai ghi nhan RO RANG vao discovery_error de nguoi goi
        # (Airflow task) biet ma bao loi, thay vi "thanh cong" ma xu ly 0 part.
        # Noi tiep (khong ghi de) neu buoc gap_scan o tren cung da bi loi.
        prev = f"{result.discovery_error}; " if result.discovery_error else ""
        result.discovery_error = f"{prev}discover_new_parts_error: {e}"

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
