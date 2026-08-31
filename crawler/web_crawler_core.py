"""
crawler/web_crawler_core.py

Logic thuần Python cho vòng lặp crawl chính của DAG 2 (`crawl_alonhadat_web`).
Mọi I/O (DB/HTTP/S3/Proxy) được inject qua Protocol → dễ unit test bằng fake/mock,
không cần Airflow, Postgres hay mạng thật.

I/O thật nằm ở crawler/web_crawler_io.py.
"""


from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Optional, Protocol, Sequence
from urllib.parse import urljoin

from bs4 import BeautifulSoup


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

# Các cụm từ để dò CAPTCHA (site trả HTTP 200 kèm trang xác minh, không có status riêng — B9).
CAPTCHA_MARKERS: tuple[str, ...] = (
    "Tôi không phải người máy",
)


def all_listing_combinations() -> list[tuple[str, str]]:
    """Sinh 10 tổ hợp (listing_type, property_type) cố định — dùng khi seed
    hoặc daily-reset bảng listing_progress."""
    return [(lt, pt) for lt in LISTING_TYPES for pt in PROPERTY_TYPES]


# ============================================================
# 2. Config & kiểu dữ liệu dùng chung
# ============================================================

@dataclass(frozen=True)
class CrawlerConfig:
    """Cấu hình cho 1 lần chạy DAG 2. Có thể override khi test."""

    max_detail_pages_per_run: int = 1000   # số trang chi tiết tối đa
    time_box_seconds: int = 45 * 60        # ~45 phút, tránh đè run hourly
    delay_min_seconds: float = 5.0         # delay khi dùng proxy
    delay_max_seconds: float = 10.0

    # Retry cùng proxy khi FETCH_ERROR (lỗi mạng/server).
    # Hết retry → dừng run ngay. PROXY_ISSUE (proxy chết/429/CAPTCHA)
    # đổi proxy ngay, lặp tới khi hết pool.
    max_fetch_error_retries: int = 3

    flush_interval_seconds: int = 10 * 60  # flush mỗi 10 phút
    flush_page_threshold: int = 100        # hoặc ~100 trang

    min_success_pages: int = 10


class StopReason(str, Enum):
    """Lý do dừng 1 run DAG 2 (ghi vào run_state.stopped_reason).

    - FETCH_ERROR: hết retry cùng proxy cho lỗi mạng/server → dừng ngay.
    - PROXY_EXHAUSTED: hết proxy trong pool (kể cả sau 1 lần refill) khi gặp PROXY_ISSUE.
    Cả hai vẫn có thể coi là thành công nếu đã đạt min_success_pages.
    """

    MAX_PAGES = "max_pages"
    TIME_BOX = "time_box"
    NO_MORE_DATA = "no_more_data"
    FETCH_ERROR = "fetch_error"
    PROXY_EXHAUSTED = "proxy_exhausted"


class ErrorKind(str, Enum):
    """Phân loại kết quả fetch — quyết định retry hay đổi proxy ngay.

    - PROXY_ISSUE: lỗi do proxy (chết/treo/quá tải) hoặc site chặn (429/CAPTCHA).
      → đổi proxy ngay, không retry cùng proxy.
    - FETCH_ERROR: lỗi mạng/server chung chung (5xx, request lạ).
      → retry cùng proxy tối đa `max_fetch_error_retries`; hết lượt → dừng run (StopReason.FETCH_ERROR).
    """

    OK = "ok"
    FETCH_ERROR = "fetch_error"
    PROXY_ISSUE = "proxy_issue"


@dataclass(frozen=True)
class RunResult:
    """Kết quả của WebCrawlerCore.run() — gồm số trang đã crawl.
    Dùng cho run_dag2() quyết định thành công hay không,
    ngay cả khi stop_reason bất thường (xem min_success_pages)."""

    stop_reason: StopReason
    detail_pages_done: int


@dataclass(frozen=True)
class ListingTask:
    """1 tổ hợp listing_progress đã được claim."""

    progress_id: int
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
    error: Optional[str] = None             # mô tả lỗi kỹ thuật
    is_proxy_error: bool = False            # True nếu lỗi rõ ràng do proxy
    # ProxyError/SSLError/ConnectTimeout (không tunnel/handshake được)
    # hoặc ReadTimeout (proxy treo, quá tải/băng thông kém).
    # → coi là lỗi bản chất proxy, đổi proxy ngay (ErrorKind.PROXY_DEAD).


@dataclass
class BronzeRecord:
    """1 dòng dữ liệu trang chi tiết đã fetch OK, chờ vào buffer tích luỹ.
    Khớp schema chung với Nguồn 1 (CDN dataset): url / crawl_date / html."""

    url: str
    crawl_date: datetime
    html: bytes   # LUÔN raw bytes, KHÔNG base64


# ============================================================
# 3. Hàm thuần (pure function) — parse HTML, không I/O thật
# ============================================================

def compute_listing_page_url(listing_type: str, property_type: str, page: int) -> str:
    """Tính URL trang danh sách bằng số học (B6), không dùng link phân trang."""
    base = f"{BASE_URL}/{listing_type}-{property_type}/ho-chi-minh"
    return base if page <= 1 else f"{base}/trang-{page}"


def _normalize_url(href: str) -> str:
    """Chuẩn hoá URL relative → absolute, tránh trùng khoá."""
    return urljoin(BASE_URL + "/", href)


def extract_detail_urls(listing_html: str) -> list[str]:
    """Trích URL chi tiết từ trang danh sách (B5)."""
    soup = BeautifulSoup(listing_html, "lxml")
    urls: list[str] = []
    for article in soup.select("article.property-item"):
        link = article.select_one("a[itemprop='url']")
        if link and link.get("href"):
            urls.append(_normalize_url(link["href"]))
    return urls


def is_pagination_end(listing_html: str) -> bool:
    """Không còn article.property-item → hết trang (B7)."""
    soup = BeautifulSoup(listing_html, "lxml")
    return len(soup.select("article.property-item")) == 0


def detect_captcha(html: str) -> bool:
    """Dò dấu hiệu CAPTCHA trong HTML (status vẫn 200) (B9)."""
    lowered = html.lower()
    return any(marker in lowered for marker in CAPTCHA_MARKERS)


def classify_fetch_result(result: FetchResult) -> ErrorKind:
    """Phân loại kết quả fetch:
    - Proxy lỗi rõ ràng, 429, hoặc CAPTCHA → PROXY_ISSUE (đổi proxy ngay).
    - Lỗi mạng/server chung chung (None, error, 5xx) → FETCH_ERROR (retry cùng proxy).
    - 2xx không phải CAPTCHA → OK.
    - 4xx khác (VD 404) → FETCH_ERROR.
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
# 4. Protocol (interface) cho các thành phần I/O thật
#    -> implement ở web_crawler_io.py, ở đây chỉ cần fake để test.
# ============================================================

class ControlPlaneRepo(Protocol):
    """Thao tác các bảng pipeline.listing_progress / detail_queue / run_state."""

    def apply_daily_reset_if_needed(self, today: date) -> None: ...
    def reclaim_stale_detail_queue(self) -> int: ...
    def claim_listing_task(self, crawl_date: date) -> Optional[ListingTask]: ...
    def mark_listing_exhausted(self, progress_id: int) -> None: ...
    def enqueue_detail_urls(
        self, urls: Sequence[str], discovered_page_id: int, crawl_date: date
    ) -> None: ...
    def claim_detail_task(self) -> Optional[DetailTask]: ...
    def mark_detail_done(self, queue_id: int) -> None: ...
    def mark_detail_failed(self, queue_id: int) -> None: ...
    def init_run_state(self, run_id: str) -> None: ...
    def finalize_run_state(
        self,
        run_id: str,
        stopped_reason: StopReason,
        detail_pages_done: int,
        output_s3_key: Optional[str],
    ) -> None: ...


class ProxyPool(Protocol):
    """Quản lý proxy hiện tại và xoay vòng khi bị blocked (B12)."""

    def current(self) -> Optional[str]: ...
    def rotate(self) -> Optional[str]: ...
    def mark_failed(self, proxy_url: str) -> None: ...

    def refill(self) -> int:
        """Fetch + health-check proxy mới khi pool cạn.
        Core chỉ gọi đúng 1 lần mỗi lần phát hiện cạn (xem _fetch_with_retry),
        không gọi trong vòng lặp thường. Trả về số proxy sống lấy được (0 nếu không có)."""
        ...


class PageFetcher(Protocol):
    """Thực hiện HTTP GET qua proxy — LUÔN trả FetchResult, không raise
    exception (mọi lỗi kỹ thuật phải được bọc vào result.error)."""

    def fetch(self, url: str, proxy_url: Optional[str]) -> FetchResult: ...


class BufferWriter(Protocol):
    """Buffer tích luỹ trong bộ nhớ + flush lên S3 (B13/B14)."""

    def add(self, record: BronzeRecord) -> None: ...

    def flush(self, run_id: str, crawl_date: date, final: bool = False) -> Optional[str]:
        """Flush buffer hiện có lên S3 key `.inprogress` (final=False) hoặc
        đổi tên thành key chính thức (final=True). Trả về S3 key nếu có
        flush thật, None nếu buffer rỗng."""
        ...


class Clock(Protocol):
    """Bọc datetime.now()/time.monotonic()/sleep() để test không cần chờ
    thật và không phụ thuộc múi giờ hệ thống."""

    def now(self) -> datetime: ...
    def monotonic(self) -> float: ...
    def sleep(self, seconds: float) -> None: ...


# ============================================================
# 5. Core orchestrator
# ============================================================

class WebCrawlerCore:
    """Điều phối DAG 2 (mục 6). 
    Không tự tạo DB/HTTP/S3 — mọi phụ thuộc inject qua constructor để dễ unit test."""

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
        """Chạy 1 lần DAG 2, trả về RunResult (lý do dừng + số trang crawl)."""
        today = self.clock.now().date()

        self.repo.apply_daily_reset_if_needed(today)
        self.repo.reclaim_stale_detail_queue()
        self.repo.init_run_state(run_id)

        start_monotonic = self.clock.monotonic()
        last_flush_monotonic = start_monotonic
        detail_pages_done = 0
        pages_since_flush = 0
        early_flush_done = False  # đã flush sớm khi đạt min_success_pages lần đầu chưa

        stop_reason: Optional[StopReason] = None

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
                # Flush sớm ngay khi đạt min_success_pages để bảo vệ dữ liệu nếu run dừng bất thường.
                self.buffer.flush(run_id, today, final=False)
                last_flush_monotonic = self.clock.monotonic()
                pages_since_flush = 0
                early_flush_done = True
            else:
                # Flush định kỳ theo interval/page threshold.
                since_flush = self.clock.monotonic() - last_flush_monotonic
                if (
                    since_flush >= self.config.flush_interval_seconds
                    or pages_since_flush >= self.config.flush_page_threshold
                ):
                    self.buffer.flush(run_id, today, final=False)
                    last_flush_monotonic = self.clock.monotonic()
                    pages_since_flush = 0

        # Kết thúc run: flush cuối + rename. Luôn lưu toàn bộ trang đã crawl.
        output_key = self.buffer.flush(run_id, today, final=True)
        self.repo.finalize_run_state(run_id, stop_reason, detail_pages_done, output_key)
        return RunResult(stop_reason=stop_reason, detail_pages_done=detail_pages_done)

    # -------- xử lý 1 trang chi tiết --------

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
        self.repo.mark_detail_done(task.queue_id)
        return None

    # -------- xử lý 1 trang danh sách --------

    def _process_listing_task(
        self, task: ListingTask, crawl_date: date
    ) -> Optional[StopReason]:
        page_url = compute_listing_page_url(
            task.listing_type, task.property_type, task.page_to_crawl
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

    # -------- fetch dùng chung listing/detail, kèm retry (B4/B9-B12) --------

    def _fetch_with_retry(self, url: str) -> tuple[FetchResult, Optional[StopReason]]:
        """Trả (result, stop_reason). 
        stop_reason=None nghĩa là fetch OK; khác None thì dừng run.

        Luật retry:
          - PROXY_ISSUE: đổi proxy ngay, lặp tới khi hết proxy (-> PROXY_EXHAUSTED).
          - FETCH_ERROR: retry cùng proxy tối đa `max_fetch_error_retries`; hết lượt -> dừng run.

        Quy tắc proxy:
          - Không bao giờ fetch bằng IP thật (proxy=None).
          - Khi pool cạn: refill đúng 1 lần; nếu vẫn cạn -> PROXY_EXHAUSTED.
        """
        same_proxy_attempts = 0
        already_refilled = False  # chỉ refill 1 lần cho mỗi lần cạn trong lượt gọi này

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
                continue  # vừa refill() thành công -> lấy proxy mới ở vòng lặp kế

            result = self.fetcher.fetch(url, proxy)
            kind = classify_fetch_result(result)

            if kind is ErrorKind.OK:
                return result, None

            if kind is ErrorKind.FETCH_ERROR:
                same_proxy_attempts += 1
                if same_proxy_attempts < self.config.max_fetch_error_retries:
                    continue  # còn lượt -> retry CÙNG proxy
                # Hết ngân sách retry cùng proxy -> DỪNG RUN NGAY, không đổi proxy
                return result, StopReason.FETCH_ERROR

            # kind is ErrorKind.PROXY_ISSUE (proxy chết/treo/429/CAPTCHA)
            self.proxy_pool.mark_failed(proxy)
            same_proxy_attempts = 0

            new_proxy = self.proxy_pool.rotate()
            if new_proxy is None:
                stop_reason = self._handle_pool_exhausted(already_refilled)
                if stop_reason is not None:
                    return result, stop_reason
                already_refilled = True
            # lặp lại vòng while với proxy mới (từ rotate() hoặc từ refill())

    def _handle_pool_exhausted(self, already_refilled: bool) -> Optional[StopReason]:
        """Pool hết proxy: refill đúng 1 lần; nếu vẫn cạn -> PROXY_EXHAUSTED."""
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
