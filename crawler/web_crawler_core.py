"""
crawler/web_crawler_core.py

Thành phần 2 (Web Crawler) — logic thuần cho vòng lặp crawl chính của DAG 2.
Mọi I/O (DB/HTTP/S3/Proxy) inject qua Protocol, implement ở web_crawler_io.py.

Vòng đời status 1 URL trong detail_queue:
    pending -> processing -> fetched -> flushed -> done
                    |            |         |
                    +------------+---------+--> failed (hết retry/proxy)
- fetched: đã có HTML, còn trong RAM.
- flushed: đã ghi an toàn lên S3 (.inprogress), chưa chắc có bản final.
- done:    đã trong file .parquet final -> Silver đọc được.

Bảo vệ dữ liệu khi crash giữa chừng — 3 lớp:
  1. try/finally quanh vòng lặp chính -> luôn cố flush final + finalize run_state.
  2. update_run_progress() ghi incremental sau mỗi flush trung gian.
  3. _reconcile_crashed_runs() (Bước 7) chạy đầu mỗi run kế tiếp -> promote
     .inprogress của run đã chết thành final nếu đạt min_success_pages.
"""


from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Optional, Protocol, Sequence
from urllib.parse import urljoin

from bs4 import BeautifulSoup

logger = logging.getLogger("web_crawler_core")


# ============================================================
# 1. Hằng số nghiệp vụ
# ============================================================

BASE_URL = "https://alonhadat.com.vn"

LISTING_TYPES: tuple[str, ...] = ("can-ban", "cho-thue")

PROPERTY_TYPES: tuple[str, ...] = (
    "nha-mat-tien",
    "nha-trong-hem",
    "biet-thu-nha-lien-ke",
    "can-ho-chung-cu",
    "phong-tro-nha-tro",
)

# Địa giới CŨ dùng để build URL nguồn web (site chưa cập nhật địa giới mới).
PROVINCES: tuple[str, ...] = ("ho-chi-minh", "binh-duong", "ba-ria-vung-tau")

# Cụm từ dò CAPTCHA (site trả HTTP 200 kèm trang xác minh, không có status riêng).
CAPTCHA_MARKERS: tuple[str, ...] = (
    "Tôi không phải người máy",
)


def all_listing_combinations() -> list[tuple[str, str, str]]:
    """Sinh 30 tổ hợp (province_old, listing_type, property_type) cố định."""
    return [(pv, lt, pt) for pv in PROVINCES for lt in LISTING_TYPES for pt in PROPERTY_TYPES]


# ============================================================
# 2. Config & kiểu dữ liệu dùng chung
# ============================================================

@dataclass(frozen=True)
class CrawlerConfig:
    """Cấu hình cho 1 lần chạy DAG 2."""

    max_detail_pages_per_run: int = 1000
    time_box_seconds: int = 45 * 60        # ~45 phút, tránh đè run hourly
    delay_min_seconds: float = 5.0
    delay_max_seconds: float = 10.0

    # Retry cùng proxy khi FETCH_ERROR; hết lượt -> dừng run.
    # PROXY_ISSUE đổi proxy ngay, lặp tới khi hết pool.
    max_fetch_error_retries: int = 3

    flush_interval_seconds: int = 10 * 60
    flush_page_threshold: int = 100

    min_success_pages: int = 10


class StopReason(str, Enum):
    """Lý do dừng 1 run (ghi vào run_state.stopped_reason)."""

    MAX_PAGES = "max_pages"
    TIME_BOX = "time_box"
    NO_MORE_DATA = "no_more_data"
    FETCH_ERROR = "fetch_error"           # hết retry cùng proxy
    PROXY_EXHAUSTED = "proxy_exhausted"   # hết proxy pool kể cả sau refill
    CRASHED = "crashed"                   # exception Python bắt được qua try/finally
    RECOVERED = "recovered"               # Bước 7 khôi phục thành công run chết trước đó
    INCOMPLETE = "incomplete"             # Bước 7 không đủ điều kiện khôi phục, đóng sổ


class ErrorKind(str, Enum):
    """Phân loại kết quả fetch — quyết định retry hay đổi proxy ngay.

    - PROXY_ISSUE: lỗi do proxy hoặc site chặn (429/CAPTCHA) -> đổi proxy ngay.
    - FETCH_ERROR: lỗi mạng/server chung -> retry cùng proxy tối đa N lần.
    """

    OK = "ok"
    FETCH_ERROR = "fetch_error"
    PROXY_ISSUE = "proxy_issue"


@dataclass(frozen=True)
class RunResult:
    """Kết quả WebCrawlerCore.run()."""

    stop_reason: StopReason
    detail_pages_done: int

@dataclass(frozen=True)
class ListingTask:
    """1 tổ hợp listing_progress đã được claim."""

    progress_id: int
    province_old: str
    listing_type: str
    property_type: str
    page_to_crawl: int

@dataclass(frozen=True)
class DetailTask:
    """1 dòng detail_queue đã được claim, ưu tiên FIFO theo discovered_at."""

    queue_id: int
    url: str

@dataclass(frozen=True)
class FetchResult:
    """Kết quả 1 lần gọi HTTP — do web_crawler_io.py tạo, core chỉ phân loại."""

    status_code: Optional[int]              # None nếu timeout/connect-fail
    html: Optional[str] = None
    error: Optional[str] = None
    is_proxy_error: bool = False
    # True nếu ProxyError/SSLError/ConnectTimeout/ReadTimeout — lỗi bản chất
    # proxy, đổi proxy ngay thay vì retry cùng proxy.

@dataclass
class BronzeRecord:
    """1 dòng trang chi tiết đã fetch OK, chờ vào buffer.
    Khớp schema chung với Dataset Loader: url / crawl_date / html."""

    url: str
    crawl_date: datetime
    html: bytes   # luôn raw bytes, không base64

@dataclass(frozen=True)
class PromotedFile:
    """Kết quả promote .inprogress -> final (Bước 7)."""

    final_key: str
    urls: list[str]

@dataclass(frozen=True)
class IncompleteRun:
    """1 dòng run_state có ended_at IS NULL — ứng viên cho Bước 7."""

    run_id: str
    started_at: datetime
    detail_pages_done: int


# ============================================================
# 3. Hàm thuần — parse HTML, không I/O thật
# ============================================================

def compute_listing_page_url(province_old: str, listing_type: str, property_type: str, page: int) -> str:
    """Tính URL trang danh sách bằng số học, không dùng link phân trang."""
    base = f"{BASE_URL}/{listing_type}-{property_type}/{province_old}"
    return base if page <= 1 else f"{base}/trang-{page}"


def _normalize_url(href: str) -> str:
    """Chuẩn hoá URL relative -> absolute, tránh trùng khoá."""
    return urljoin(BASE_URL + "/", href)


def extract_detail_urls(listing_html: str) -> list[str]:
    """Trích URL chi tiết từ trang danh sách (Bước 4)."""
    soup = BeautifulSoup(listing_html, "lxml")
    urls: list[str] = []
    for article in soup.select("article.property-item"):
        link = article.select_one("a[itemprop='url']")
        if link and link.get("href"):
            urls.append(_normalize_url(link["href"]))
    return urls


def is_pagination_end(listing_html: str) -> bool:
    """Không còn article.property-item -> hết trang (Bước 4)."""
    soup = BeautifulSoup(listing_html, "lxml")
    return len(soup.select("article.property-item")) == 0


def detect_captcha(html: str) -> bool:
    """Dò dấu hiệu CAPTCHA trong HTML (status vẫn 200)."""
    lowered = html.lower()
    return any(marker in lowered for marker in CAPTCHA_MARKERS)


def classify_fetch_result(result: FetchResult) -> ErrorKind:
    """Phân loại kết quả fetch (Bước 3):
    - Proxy lỗi rõ ràng, 429, hoặc CAPTCHA -> PROXY_ISSUE.
    - Lỗi mạng/server chung (None, error, 5xx) -> FETCH_ERROR.
    - 2xx không phải CAPTCHA -> OK. 4xx khác (VD 404) -> FETCH_ERROR.
    """
    if result.is_proxy_error:
        return ErrorKind.PROXY_ISSUE
    if result.error is not None or result.status_code is None:
        return ErrorKind.FETCH_ERROR
    if result.status_code == 429:
        return ErrorKind.PROXY_ISSUE
    if result.status_code >= 500:
        return ErrorKind.FETCH_ERROR
    if result.html and detect_captcha(result.html):
        return ErrorKind.PROXY_ISSUE
    if 200 <= result.status_code < 300:
        return ErrorKind.OK
    return ErrorKind.FETCH_ERROR


# ============================================================
# 4. Protocol cho các thành phần I/O thật (implement ở web_crawler_io.py)
# ============================================================

class ControlPlaneRepo(Protocol):
    """Thao tác pipeline.listing_progress / detail_queue / run_state."""

    def apply_daily_reset_if_needed(self, today: date) -> None: ...
    def reclaim_stale_detail_queue(self) -> int: ...
    def claim_listing_task(self, crawl_date: date) -> Optional[ListingTask]: ...
    def mark_listing_exhausted(self, progress_id: int) -> None: ...
    def enqueue_detail_urls(
        self, urls: Sequence[str], discovered_page_id: int, crawl_date: date
    ) -> None: ...
    def claim_detail_task(self) -> Optional[DetailTask]: ...

    def mark_detail_fetched(self, queue_id: int) -> None:
        """processing -> fetched, gọi ngay sau buffer.add()."""
        ...

    def mark_details_flushed(self, queue_ids: Sequence[int]) -> None:
        """fetched -> flushed, sau buffer.flush(final=False) thành công."""
        ...

    def mark_details_done(self, queue_ids: Sequence[int]) -> None:
        """fetched/flushed -> done, sau buffer.flush(final=True) thành công."""
        ...

    def mark_urls_done(self, urls: Sequence[str]) -> None:
        """Như mark_details_done() nhưng theo url — dùng ở Bước 7
        (reconciliation không còn giữ queue_id gốc, chỉ đọc lại url từ parquet)."""
        ...

    def mark_detail_failed(self, queue_id: int) -> None: ...
    def init_run_state(self, run_id: str) -> None: ...

    def update_run_progress(self, run_id: str, detail_pages_done: int) -> None:
        """Cập nhật incremental sau mỗi flush trung gian — lưới an toàn khi bị kill cứng."""
        ...

    def list_incomplete_runs(self, older_than_seconds: int) -> list[IncompleteRun]:
        """Bước 7 — run_state có ended_at IS NULL và started_at đã đủ cũ."""
        ...

    def finalize_run_state(
        self,
        run_id: str,
        stopped_reason: StopReason,
        detail_pages_done: int,
        output_s3_key: Optional[str],
    ) -> None: ...


class ProxyPool(Protocol):
    """Quản lý proxy hiện tại và xoay vòng khi bị chặn."""

    def current(self) -> Optional[str]: ...
    def rotate(self) -> Optional[str]: ...
    def mark_failed(self, proxy_url: str) -> None: ...

    def refill(self) -> int:
        """Fetch + health-check proxy mới khi pool cạn. Core chỉ gọi 1 lần
        mỗi lần phát hiện cạn. Trả về số proxy sống lấy được."""
        ...


class PageFetcher(Protocol):
    """HTTP GET qua proxy — luôn trả FetchResult, không raise exception."""

    def fetch(self, url: str, proxy_url: Optional[str]) -> FetchResult: ...


class BufferWriter(Protocol):
    """Buffer tích luỹ trong bộ nhớ + flush lên S3 (Bước 5)."""

    def add(self, record: BronzeRecord) -> None: ...

    def flush(self, run_id: str, crawl_date: date, final: bool = False) -> Optional[str]:
        """Flush lên S3 key `.inprogress` (final=False) hoặc đổi tên thành
        key chính thức (final=True). Trả None nếu buffer rỗng."""
        ...

    def promote_inprogress_to_final(
        self, run_id: str, crawl_date: date
    ) -> Optional[PromotedFile]:
        """Bước 7 — đổi .inprogress của run đã chết thành final, đọc url
        trong đó để repo cập nhật detail_queue. None nếu không có gì để promote."""
        ...

    def delete_inprogress(self, run_id: str, crawl_date: date) -> None:
        """Bước 7 — dọn .inprogress của run chết nhưng không đủ điều kiện
        promote. Không tồn tại thì bỏ qua êm."""
        ...


class Clock(Protocol):
    """Bọc datetime.now()/monotonic()/sleep() để test không phụ thuộc thời gian thật."""

    def now(self) -> datetime: ...
    def monotonic(self) -> float: ...
    def sleep(self, seconds: float) -> None: ...


# ============================================================
# 5. Core orchestrator
# ============================================================

class WebCrawlerCore:
    """Điều phối toàn bộ vòng lặp crawl DAG 2. Mọi phụ thuộc inject qua
    constructor để dễ unit test."""

    # Ngưỡng coi 1 run là "chắc chắn đã chết" cho Bước 7 — 2 giờ, chừa dư
    # buffer cho Celery/Redis redeliver task hoặc retry/đổi proxy kéo dài hợp lệ.
    RECONCILE_STALE_RUN_AFTER_SECONDS = 2 * 60 * 60

    def __init__(
        self,
        repo: ControlPlaneRepo,
        proxy_pool: ProxyPool,
        fetcher: PageFetcher,
        buffer: BufferWriter,
        clock: Clock,
        config: CrawlerConfig = CrawlerConfig(),
        rng: Optional[random.Random] = None,
    ) -> None:
        self.repo = repo
        self.proxy_pool = proxy_pool
        self.fetcher = fetcher
        self.buffer = buffer
        self.clock = clock
        self.config = config
        self.rng = rng or random.Random()

    # -------- entrypoint gọi từ Airflow PythonOperator --------

    def run(self, run_id: str) -> RunResult:
        """Chạy 1 lần DAG 2. 'done' trong detail_queue chỉ ghi SAU KHI dữ
        liệu đã flush thành công lên S3 (fetched -> flushed -> done)."""
        today = self.clock.now().date()

        # Bước 7 chạy TRƯỚC KHI động tới detail_queue của run hiện tại — để
        # các dòng 'flushed' thuộc run vừa khôi phục không bị reclaim oan.
        self._reconcile_crashed_runs()

        self.repo.apply_daily_reset_if_needed(today)
        self.repo.reclaim_stale_detail_queue()
        self.repo.init_run_state(run_id)

        start_monotonic = self.clock.monotonic()
        last_flush_monotonic = start_monotonic
        detail_pages_done = 0
        pages_since_flush = 0
        early_flush_done = False
        # Không bao giờ clear — buffer.flush() mỗi lần re-serialize toàn bộ
        # buffer tích luỹ từ đầu run.
        all_fetched_queue_ids: list[int] = []

        stop_reason: Optional[StopReason] = None

        try:
            while stop_reason is None:
                if detail_pages_done >= self.config.max_detail_pages_per_run:
                    stop_reason = StopReason.MAX_PAGES
                    break

                if self.clock.monotonic() - start_monotonic >= self.config.time_box_seconds:
                    stop_reason = StopReason.TIME_BOX
                    break

                detail_task = self.repo.claim_detail_task()

                if detail_task is not None:
                    stop_reason = self._process_detail_task(detail_task)
                    if stop_reason is None:
                        detail_pages_done += 1
                        pages_since_flush += 1
                        all_fetched_queue_ids.append(detail_task.queue_id)
                else:
                    listing_task = self.repo.claim_listing_task(today)
                    if listing_task is None:
                        stop_reason = StopReason.NO_MORE_DATA
                        break
                    stop_reason = self._process_listing_task(listing_task, today)

                if stop_reason is not None:
                    break

                self._sleep_between_requests()

                if not early_flush_done and detail_pages_done >= self.config.min_success_pages:
                    # Flush sớm ngay khi đạt min_success_pages, bảo vệ dữ liệu nếu run dừng bất thường.
                    self._flush_and_mark(
                        run_id, today, all_fetched_queue_ids, detail_pages_done, final=False
                    )
                    last_flush_monotonic = self.clock.monotonic()
                    pages_since_flush = 0
                    early_flush_done = True
                else:
                    since_flush = self.clock.monotonic() - last_flush_monotonic
                    if (
                        since_flush >= self.config.flush_interval_seconds
                        or pages_since_flush >= self.config.flush_page_threshold
                    ):
                        self._flush_and_mark(
                            run_id, today, all_fetched_queue_ids, detail_pages_done, final=False
                        )
                        last_flush_monotonic = self.clock.monotonic()
                        pages_since_flush = 0
        except Exception:
            stop_reason = StopReason.CRASHED
            raise
        finally:
            # Bọc riêng try/except để lỗi ở đây không che mất exception gốc.
            try:
                output_key = self._flush_and_mark(
                    run_id, today, all_fetched_queue_ids, detail_pages_done, final=True
                )
                self.repo.finalize_run_state(run_id, stop_reason, detail_pages_done, output_key)
            except Exception:
                logger.exception(
                    "Lỗi khi flush/finalize trong finally — bỏ qua, không che exception gốc"
                )

        return RunResult(stop_reason=stop_reason, detail_pages_done=detail_pages_done)

    # -------- flush + mark status tương ứng (Bước 5) --------

    def _flush_and_mark(
        self,
        run_id: str,
        crawl_date: date,
        all_fetched_queue_ids: list[int],
        detail_pages_done: int,
        final: bool,
    ) -> Optional[str]:
        """final=False -> mark 'flushed'; final=True -> mark 'done'.
        Luôn mark toàn bộ all_fetched_queue_ids vì buffer.flush() mỗi lần
        re-serialize toàn bộ buffer tích luỹ từ đầu run."""
        output_key = self.buffer.flush(run_id, crawl_date, final=final)
        if output_key is not None and all_fetched_queue_ids:
            if final:
                self.repo.mark_details_done(all_fetched_queue_ids)
            else:
                self.repo.mark_details_flushed(all_fetched_queue_ids)
            self.repo.update_run_progress(run_id, detail_pages_done)
        return output_key

    # -------- Bước 7: phục hồi run bị SIGKILL/OOM --------

    def _reconcile_crashed_runs(self) -> None:
        """Run đạt min_success_pages -> promote .inprogress thành final +
        mark URL 'done'. Không đủ -> đóng sổ INCOMPLETE, dữ liệu coi như
        mất, detail_queue liên quan tự về 'pending' qua reclaim_stale_detail_queue()."""
        incomplete_runs = self.repo.list_incomplete_runs(
            older_than_seconds=self.RECONCILE_STALE_RUN_AFTER_SECONDS
        )
        for incomplete in incomplete_runs:
            crawl_date = incomplete.started_at.date()
            promoted: Optional[PromotedFile] = None
            if incomplete.detail_pages_done >= self.config.min_success_pages:
                promoted = self.buffer.promote_inprogress_to_final(incomplete.run_id, crawl_date)

            if promoted is not None:
                self.repo.mark_urls_done(promoted.urls)
                self.repo.finalize_run_state(
                    incomplete.run_id, StopReason.RECOVERED,
                    incomplete.detail_pages_done, promoted.final_key,
                )
                logger.info(
                    "Reconciliation: khôi phục run_id=%s (%d trang, %d URL promote)",
                    incomplete.run_id, incomplete.detail_pages_done, len(promoted.urls),
                )
            else:
                self.buffer.delete_inprogress(incomplete.run_id, crawl_date)
                self.repo.finalize_run_state(
                    incomplete.run_id, StopReason.INCOMPLETE,
                    incomplete.detail_pages_done, None,
                )
                logger.warning(
                    "Reconciliation: run_id=%s không đủ điều kiện khôi phục "
                    "(%d trang < %d tối thiểu) -> coi là mất, đã dọn .inprogress mồ côi",
                    incomplete.run_id, incomplete.detail_pages_done, self.config.min_success_pages,
                )

    # -------- Bước 5: xử lý 1 trang chi tiết --------

    def _process_detail_task(self, task: DetailTask) -> Optional[StopReason]:
        result, stop_reason = self._fetch_with_retry(task.url)

        if stop_reason is not None:
            self.repo.mark_detail_failed(task.queue_id)
            return stop_reason

        assert result.html is not None  # OK -> luôn có html (xem classify_fetch_result)
        record = BronzeRecord(
            url=task.url,
            crawl_date=self.clock.now(),
            html=result.html.encode("utf-8"),
        )
        self.buffer.add(record)
        # Chỉ mark 'fetched' ở đây — 'done' dời sang _flush_and_mark(),
        # sau khi dữ liệu đã flush thành công lên S3.
        self.repo.mark_detail_fetched(task.queue_id)
        return None

    # -------- Bước 4: xử lý 1 trang danh sách --------

    def _process_listing_task(
        self, task: ListingTask, crawl_date: date
    ) -> Optional[StopReason]:
        page_url = compute_listing_page_url(
            task.province_old, task.listing_type, task.property_type, task.page_to_crawl
        )
        result, stop_reason = self._fetch_with_retry(page_url)

        if stop_reason is not None:
            return stop_reason

        assert result.html is not None
        if is_pagination_end(result.html):
            self.repo.mark_listing_exhausted(task.progress_id)
            return None

        urls = extract_detail_urls(result.html)
        self.repo.enqueue_detail_urls(urls, task.progress_id, crawl_date)
        return None

    # -------- Bước 2-3: fetch dùng chung listing/detail, kèm retry --------

    def _fetch_with_retry(self, url: str) -> tuple[FetchResult, Optional[StopReason]]:
        """stop_reason=None nghĩa là fetch OK; khác None thì dừng run.

        Luật retry:
          - PROXY_ISSUE: đổi proxy ngay, lặp tới khi hết proxy (-> PROXY_EXHAUSTED).
          - FETCH_ERROR: retry cùng proxy tối đa max_fetch_error_retries; hết lượt -> dừng run.

        Quy tắc proxy: không bao giờ fetch bằng IP thật. Pool cạn -> refill
        đúng 1 lần; vẫn cạn -> PROXY_EXHAUSTED.
        """
        same_proxy_attempts = 0
        already_refilled = False

        while True:
            proxy = self.proxy_pool.current()

            if proxy is None:
                stop_reason = self._handle_pool_exhausted(already_refilled)
                if stop_reason is not None:
                    return (
                        FetchResult(status_code=None, error="proxy pool cạn, không thể tiếp tục"),
                        stop_reason,
                    )
                already_refilled = True
                same_proxy_attempts = 0
                continue

            result = self.fetcher.fetch(url, proxy)
            kind = classify_fetch_result(result)

            if kind is ErrorKind.OK:
                return result, None

            if kind is ErrorKind.FETCH_ERROR:
                same_proxy_attempts += 1
                if same_proxy_attempts < self.config.max_fetch_error_retries:
                    continue
                return result, StopReason.FETCH_ERROR

            # kind is ErrorKind.PROXY_ISSUE
            self.proxy_pool.mark_failed(proxy)
            same_proxy_attempts = 0

            new_proxy = self.proxy_pool.rotate()
            if new_proxy is None:
                stop_reason = self._handle_pool_exhausted(already_refilled)
                if stop_reason is not None:
                    return result, stop_reason
                already_refilled = True

    def _handle_pool_exhausted(self, already_refilled: bool) -> Optional[StopReason]:
        """Pool hết proxy: refill đúng 1 lần; vẫn cạn -> PROXY_EXHAUSTED."""
        if already_refilled:
            return StopReason.PROXY_EXHAUSTED
        new_count = self.proxy_pool.refill()
        if new_count <= 0:
            return StopReason.PROXY_EXHAUSTED
        return None

    def _sleep_between_requests(self) -> None:
        """Delay ngẫu nhiên giữa 2 request (concurrency=1, tôn trọng tải site)."""
        delay = self.rng.uniform(
            self.config.delay_min_seconds, self.config.delay_max_seconds
        )
        self.clock.sleep(delay)
