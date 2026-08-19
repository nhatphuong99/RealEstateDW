"""
crawler/bronze_crawler_core.py

Logic THUẦN Python (không thực hiện I/O thật) cho vòng lặp crawl chính của
DAG 2 (`crawl_alonhadat_web`). Toàn bộ tương tác DB / HTTP / S3 / Proxy được
inject qua các Protocol (Dependency Injection) — nhờ vậy có thể unit test
bằng fake/mock, không cần Airflow, Postgres hay mạng thật.

I/O thật (DB/S3/HTTP/Proxy) nằm ở module riêng: crawler/bronze_crawler_io.py

Tài liệu tham chiếu thiết kế: tong_hop_boi_canh_crawler_alonhadat.md
  - Mục 5: schema bảng crawl.listing_progress / detail_queue / proxy_pool / run_state
  - Mục 6: quy trình chi tiết vòng lặp chính (đã bám sát từng bước ở đây)
  - Mục 7: các quyết định thiết kế & lý do
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

# Các cụm từ dùng để dò trang CAPTCHA (site trả HTTP 200 kèm trang xác minh,
# KHÔNG có status code riêng biệt — B9).
# ⚠️ Đây là placeholder suy đoán, CHƯA đối chiếu với mẫu HTML CAPTCHA thật.
# Cần cập nhật lại danh sách này khi gặp CAPTCHA thật lần đầu khi crawl.
CAPTCHA_MARKERS: tuple[str, ...] = (
    "captcha",
    "xác minh bạn không phải",
    "verify you are human",
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
    """Tham số cấu hình 1 lần chạy DAG 2. Override khi test để chạy nhanh."""

    max_detail_pages_per_run: int = 1000   # tính theo trang CHI TIẾT (mục 7)
    time_box_seconds: int = 45 * 60         # ~45 phút, tránh đè lên run hourly kế tiếp
    delay_min_seconds: float = 5.0          # xem cảnh báo ở đầu file
    delay_max_seconds: float = 10.0
    max_fetch_error_retries: int = 3        # B11: retry CÙNG proxy (lỗi kỹ thuật chung)
    max_blocked_proxy_rotations: int = 3    # B12: đổi tối đa 3 proxy khi bị 429/CAPTCHA (site chặn thật)
    max_dead_proxy_rotations: int = 8       # đổi tối đa 8 proxy khi PROXY CHẾT (ProxyError) — ngân
                                             # sách cao hơn blocked vì tỷ lệ proxy free chết là BÌNH
                                             # THƯỜNG (~90%+), không phải hiện tượng hiếm như bị site chặn
    flush_interval_seconds: int = 10 * 60   # B13: 10 phút
    flush_page_threshold: int = 100         # B13: ~100 trang chi tiết


class StopReason(str, Enum):
    """Lý do dừng 1 run của DAG 2 — ghi vào run_state.stopped_reason."""

    MAX_PAGES = "max_pages"
    TIME_BOX = "time_box"
    NO_MORE_DATA = "no_more_data"
    FETCH_ERROR = "fetch_error"
    BLOCKED = "blocked"
    PROXY_EXHAUSTED = "proxy_exhausted"


class ErrorKind(str, Enum):
    """Phân loại kết quả fetch — quyết định retry cùng proxy hay đổi proxy."""

    OK = "ok"
    FETCH_ERROR = "fetch_error"   # timeout / connect-fail / 5xx thường (B11)
    BLOCKED = "blocked"            # 429 hoặc CAPTCHA, gộp chung (B12)


@dataclass(frozen=True)
class ListingTask:
    """1 tổ hợp listing_progress đã được claim (B3)."""

    progress_id: int
    listing_type: str
    property_type: str
    page_to_crawl: int


@dataclass(frozen=True)
class DetailTask:
    """1 dòng detail_queue đã được claim, ưu tiên FIFO theo discovered_at (B8)."""

    queue_id: int
    url: str


@dataclass(frozen=True)
class FetchResult:
    """Kết quả 1 lần gọi HTTP — do bronze_crawler_io.py tạo ra rồi truyền
    vào core. Core KHÔNG tự gọi HTTP, chỉ phân loại kết quả."""

    status_code: Optional[int]                  # None nếu timeout/connect-fail
    html: Optional[str] = None
    error: Optional[str] = None                  # mô tả lỗi kỹ thuật, nếu có
    retry_after_seconds: Optional[int] = None     # header Retry-After, nếu có
    is_proxy_error: bool = False                  # True nếu lỗi RÕ RÀNG do proxy chết
    # (ProxyError/SSLError/ConnectTimeout — không tunnel/handshake được qua
    # proxy) — phát hiện từ log thực tế 2026-08-19: retry mù cùng 1 proxy
    # chắc chắn hỏng 3 lần là vô ích, cần đổi proxy ngay (xem classify_fetch_result).


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
    """Tính URL trang danh sách bằng số học (B6) — KHÔNG dùng link phân
    trang ">>" vì đã xác nhận site có thể nhảy cóc số trang (VD 1→9→13)."""
    base = f"{BASE_URL}/{listing_type}-{property_type}/ho-chi-minh"
    if page <= 1:
        return base
    return f"{base}/trang-{page}"


def _normalize_url(href: str) -> str:
    """Chuẩn hoá URL relative -> absolute, thực hiện đúng 1 nơi duy nhất
    (C3), tránh phát sinh 2 URL khác nhau trỏ cùng 1 tin -> trùng khoá."""
    return urljoin(BASE_URL + "/", href)


def extract_detail_urls(listing_html: str) -> list[str]:
    """Trích URL trang chi tiết từ trang danh sách (B5).
    Selector: mỗi tin là <article class="property-item" itemscope
    itemtype="https://schema.org/RealEstateListing">, URL tại
    a[itemprop='url']::attr(href)."""
    soup = BeautifulSoup(listing_html, "lxml")
    urls: list[str] = []
    for article in soup.select("article.property-item"):
        link = article.select_one("a[itemprop='url']")
        href = link.get("href") if link else None
        if href:
            urls.append(_normalize_url(href))
    return urls


def is_pagination_end(listing_html: str) -> bool:
    """0 khối article.property-item -> hết trang, đánh dấu tổ hợp
    exhausted (B7)."""
    soup = BeautifulSoup(listing_html, "lxml")
    return len(soup.select("article.property-item")) == 0


def detect_captcha(html: str) -> bool:
    """Dò dấu hiệu trang CAPTCHA trong nội dung HTML (status vẫn 200) (B9)."""
    lowered = html.lower()
    return any(marker in lowered for marker in CAPTCHA_MARKERS)


def classify_fetch_result(result: FetchResult) -> ErrorKind:
    """Phân loại kết quả fetch theo B9-B11:
    - Lỗi RÕ RÀNG do proxy chết (`is_proxy_error=True`: ProxyError/SSLError/
      ConnectTimeout — không tunnel/handshake qua proxy được) -> BLOCKED
      NGAY, coi như đổi proxy y hệt 429/CAPTCHA. KHÔNG retry mù cùng 1 proxy
      chắc chắn hỏng (phát hiện từ log thực tế 2026-08-19 — proxy free chết
      giữa chừng rất phổ biến, retry 3 lần cùng proxy chết là vô ích).
    - Không có response nhưng KHÔNG rõ do proxy (VD ReadTimeout — proxy đã
      connect được, chỉ là chờ phản hồi lâu, có thể do site chậm) hoặc 5xx
      không do rate-limit -> FETCH_ERROR (retry cùng proxy, B11).
    - 429, hoặc HTML chứa dấu hiệu CAPTCHA (status vẫn 200) -> BLOCKED
      (đổi proxy).
    - 2xx và không phải CAPTCHA -> OK.
    - 4xx khác (VD 404 tin đã gỡ) -> coi là FETCH_ERROR kỹ thuật, không
      phải do bị chặn, không nên tốn lượt đổi proxy."""
    if result.is_proxy_error:
        return ErrorKind.BLOCKED

    if result.error is not None or result.status_code is None:
        return ErrorKind.FETCH_ERROR

    if result.status_code == 429:
        return ErrorKind.BLOCKED

    if result.status_code >= 500:
        return ErrorKind.FETCH_ERROR

    if result.html is not None and detect_captcha(result.html):
        return ErrorKind.BLOCKED

    if 200 <= result.status_code < 300:
        return ErrorKind.OK

    return ErrorKind.FETCH_ERROR


# ============================================================
# 4. Protocol (interface) cho các thành phần I/O thật
#    -> implement ở bronze_crawler_io.py, ở đây chỉ cần fake để test.
# ============================================================

class ControlPlaneRepo(Protocol):
    """Thao tác các bảng crawl.listing_progress / detail_queue / run_state."""

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
        """Lấy proxy MỚI khi pool cạn giữa run — network call thật (chậm),
        core chỉ gọi ĐÚNG 1 LẦN mỗi khi phát hiện cạn (xem _fetch_with_retry),
        KHÔNG gọi trong vòng lặp bình thường. Trả về số proxy mới lấy được
        (0 nếu không có gì mới)."""
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

class BronzeCrawlerCore:
    """Điều phối vòng lặp chính của DAG 2 (mục 6 trong tài liệu thiết kế).

    KHÔNG tự tạo kết nối DB/HTTP/S3 — mọi phụ thuộc được inject qua
    constructor (Dependency Injection), nên có thể unit test bằng fake
    implementation, không cần Airflow/Postgres/mạng thật."""

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

    def run(self, run_id: str) -> StopReason:
        """Chạy 1 lần trigger DAG 2, trả về lý do dừng để ghi vào run_state."""
        today = self.clock.now().date()

        self.repo.apply_daily_reset_if_needed(today)
        self.repo.reclaim_stale_detail_queue()
        self.repo.init_run_state(run_id)

        start_monotonic = self.clock.monotonic()
        last_flush_monotonic = start_monotonic
        detail_pages_done = 0
        pages_since_flush = 0

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

            # Flush định kỳ — độc lập với nhánh detail/listing ở trên (B13)
            since_flush = self.clock.monotonic() - last_flush_monotonic
            if (
                since_flush >= self.config.flush_interval_seconds
                or pages_since_flush >= self.config.flush_page_threshold
            ):
                self.buffer.flush(run_id, today, final=False)
                last_flush_monotonic = self.clock.monotonic()
                pages_since_flush = 0

        # Kết thúc run (bất kể lý do dừng nào) — flush lần cuối + rename (B14)
        output_key = self.buffer.flush(run_id, today, final=True)
        self.repo.finalize_run_state(run_id, stop_reason, detail_pages_done, output_key)
        return stop_reason

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
        """Trả (result, stop_reason). stop_reason=None nghĩa là fetch OK,
        có thể tiếp tục vòng lặp chính; khác None nghĩa là phải dừng run
        ngay, không xử lý task hiện tại.

        QUY TẮC PROXY (quyết định 2026-08-18): KHÔNG BAO GIỜ được gọi
        fetcher với proxy=None (tức chạy bằng IP thật) — mỗi khi pool cạn
        (`current()` trả None ngay từ đầu, hoặc `rotate()` hết proxy hợp lệ
        sau khi bị blocked), thử `refill()` ĐÚNG 1 LẦN. Nếu refill() lấy
        được proxy mới -> tiếp tục fetch bình thường với proxy mới. Nếu
        refill() vẫn không có gì -> dừng run ngay (PROXY_EXHAUSTED)."""
        fetch_error_attempts = 0
        blocked_proxy_attempts = 0
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
                continue  # vừa refill() thành công -> lấy proxy mới ở vòng lặp kế

            result = self.fetcher.fetch(url, proxy)
            kind = classify_fetch_result(result)

            if kind is ErrorKind.OK:
                return result, None

            if kind is ErrorKind.FETCH_ERROR:
                fetch_error_attempts += 1
                if fetch_error_attempts >= self.config.max_fetch_error_retries:
                    return result, StopReason.FETCH_ERROR
                # B11: lỗi kỹ thuật -> retry CÙNG proxy, không rotate
                continue

            # kind is ErrorKind.BLOCKED (429 hoặc CAPTCHA)
            self.proxy_pool.mark_failed(proxy)

            blocked_proxy_attempts += 1
            if blocked_proxy_attempts >= self.config.max_blocked_proxy_rotations:
                return result, StopReason.BLOCKED

            new_proxy = self.proxy_pool.rotate()
            if new_proxy is None:
                stop_reason = self._handle_pool_exhausted(already_refilled)
                if stop_reason is not None:
                    return result, stop_reason
                already_refilled = True
            # B12: retry CÙNG URL bằng proxy mới (từ rotate() hoặc từ refill()) -> lặp lại vòng while

    def _handle_pool_exhausted(self, already_refilled: bool) -> Optional[StopReason]:
        """Gọi khi pool hết proxy hợp lệ (current()==None hoặc rotate()
        trả None). Refill ĐÚNG 1 LẦN cho lượt gọi _fetch_with_retry hiện
        tại; nếu đã refill rồi mà vẫn cạn, hoặc refill() không lấy được
        proxy nào -> trả PROXY_EXHAUSTED để dừng run ngay."""
        if already_refilled:
            return StopReason.PROXY_EXHAUSTED
        new_count = self.proxy_pool.refill()
        if new_count <= 0:
            return StopReason.PROXY_EXHAUSTED
        return None

    def _sleep_between_requests(self) -> None:
        """Delay ngẫu nhiên giữa 2 request liên tiếp (concurrency luôn =1,
        tôn trọng tải của site — mục 7)."""
        delay = self.rng.uniform(
            self.config.delay_min_seconds, self.config.delay_max_seconds
        )
        self.clock.sleep(delay)
