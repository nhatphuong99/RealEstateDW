"""
crawler/web_crawler_core.py

Component 2 (Web Crawler) — pure logic for DAG 2's main crawl loop. All I/O
(DB/HTTP/S3/Proxy) is injected via Protocol, implemented in web_crawler_io.py.

Status lifecycle of a URL in detail_queue:
    pending -> processing -> fetched -> flushed -> done
                    |            |         |
                    +------------+---------+--> failed (retries/proxies exhausted)
- fetched: HTML fetched, still in RAM.
- flushed: safely written to S3 (.inprogress), not necessarily final yet.
- done:    inside a final .parquet file -> readable by Silver.

3 layers of protection against a mid-run crash:
  1. try/finally around the main loop -> always attempts a final flush +
     finalizes run_state.
  2. update_run_progress() writes incrementally after every intermediate flush.
  3. _reconcile_crashed_runs() (Step 7) runs at the start of the next run ->
     promotes a dead run's .inprogress file to final if it reached
     min_success_pages.
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

from listing_taxonomy import (
    PROVINCES_OLD,
    listing_type_slugs,
    property_type_slugs,
)

logger = logging.getLogger("web_crawler_core")


# ============================================================
# 1. Business constants
# ============================================================

BASE_URL = "https://alonhadat.com.vn"

# Sourced from listing_taxonomy.py — the single mapping shared with
# parser/bronze_to_silver_core.py's scope filter, so the two can never
# drift out of sync (see the module docstring there for the failure mode
# this prevents).
LISTING_TYPES: tuple[str, ...] = listing_type_slugs()

PROPERTY_TYPES: tuple[str, ...] = property_type_slugs()

# OLD administrative boundaries used to build source URLs (the site hasn't
# been updated to the new boundaries).
PROVINCES: tuple[str, ...] = PROVINCES_OLD

# CAPTCHA detection phrase (site returns HTTP 200 with a verification page,
# no distinct status code). Kept in Vietnamese — must match the real page text.
CAPTCHA_MARKERS: tuple[str, ...] = (
    "Tôi không phải người máy",
)


def all_listing_combinations() -> list[tuple[str, str, str]]:
    """Generate the 30 fixed (province_old, listing_type, property_type) combinations."""
    return [(pv, lt, pt) for pv in PROVINCES for lt in LISTING_TYPES for pt in PROPERTY_TYPES]


# ============================================================
# 2. Shared config & data types
# ============================================================

@dataclass(frozen=True)
class CrawlerConfig:
    """Config for a single DAG 2 run."""

    max_detail_pages_per_run: int = 1000
    time_box_seconds: int = 45 * 60        # ~45 min, avoids overlapping the next hourly run
    delay_min_seconds: float = 5.0
    delay_max_seconds: float = 10.0

    # Retry the same proxy on FETCH_ERROR; give up and stop the run once exhausted.
    # PROXY_ISSUE rotates immediately, repeating until the pool is exhausted.
    max_fetch_error_retries: int = 3

    flush_interval_seconds: int = 10 * 60
    flush_page_threshold: int = 100

    min_success_pages: int = 10
    reconcile_stale_run_after_seconds: int = 2 * 60 * 60


class StopReason(str, Enum):
    """Reason a run stopped (written to run_state.stopped_reason)."""

    MAX_PAGES = "max_pages"
    TIME_BOX = "time_box"
    NO_MORE_DATA = "no_more_data"
    FETCH_ERROR = "fetch_error"           # retries on the same proxy exhausted
    PROXY_EXHAUSTED = "proxy_exhausted"   # proxy pool exhausted even after refill
    CRASHED = "crashed"                   # Python exception caught via try/finally
    RECOVERED = "recovered"               # Step 7 successfully recovered a prior dead run
    INCOMPLETE = "incomplete"             # Step 7 couldn't recover it, closed out instead


class ErrorKind(str, Enum):
    """Fetch-result classification — decides retry vs. immediate proxy rotation.

    - PROXY_ISSUE: proxy-level failure or site block (429/CAPTCHA) -> rotate now.
    - FETCH_ERROR: generic network/server error -> retry the same proxy up to N times.
    """

    OK = "ok"
    FETCH_ERROR = "fetch_error"
    PROXY_ISSUE = "proxy_issue"


@dataclass(frozen=True)
class RunResult:
    """Result of WebCrawlerCore.run()."""

    stop_reason: StopReason
    detail_pages_done: int

@dataclass(frozen=True)
class ListingTask:
    """A claimed listing_progress combination."""

    progress_id: int
    province_old: str
    listing_type: str
    property_type: str
    page_to_crawl: int

@dataclass(frozen=True)
class DetailTask:
    """A claimed detail_queue row, FIFO by discovered_at."""

    queue_id: int
    url: str

@dataclass(frozen=True)
class FetchResult:
    """Result of one HTTP call — produced by web_crawler_io.py, the core
    only classifies it."""

    status_code: Optional[int]              # None on timeout/connect failure
    html: Optional[str] = None
    error: Optional[str] = None
    is_proxy_error: bool = False
    # True for ProxyError/SSLError/ConnectTimeout/ReadTimeout — treated as a
    # proxy-level failure, rotate immediately instead of retrying the same proxy.

@dataclass
class BronzeRecord:
    """A successfully fetched detail page, ready for the buffer.
    Matches the shared schema with the Dataset Loader: url / crawl_date / html."""

    url: str
    crawl_date: datetime
    html: bytes   # always raw bytes, never base64

@dataclass(frozen=True)
class PromotedFile:
    """Result of promoting .inprogress -> final (Step 7)."""

    final_key: str
    urls: list[str]

@dataclass(frozen=True)
class IncompleteRun:
    """A run_state row with ended_at IS NULL — a Step 7 candidate."""

    run_id: str
    started_at: datetime
    detail_pages_done: int


# ============================================================
# 3. Pure functions — HTML parsing, no real I/O
# ============================================================

def compute_listing_page_url(province_old: str, listing_type: str, property_type: str, page: int) -> str:
    """Compute the listing page URL arithmetically, not via pagination links."""
    base = f"{BASE_URL}/{listing_type}-{property_type}/{province_old}"
    return base if page <= 1 else f"{base}/trang-{page}"


def _normalize_url(href: str) -> str:
    """Normalize a relative URL to absolute, to avoid duplicate keys."""
    return urljoin(BASE_URL + "/", href)


def extract_detail_urls(listing_html: str) -> list[str]:
    """Extract detail-page URLs from a listing page (Step 4)."""
    soup = BeautifulSoup(listing_html, "lxml")
    urls: list[str] = []
    for article in soup.select("article.property-item"):
        link = article.select_one("a[itemprop='url']")
        if link and link.get("href"):
            urls.append(_normalize_url(link["href"]))
    return urls


def is_pagination_end(listing_html: str) -> bool:
    """No more article.property-item -> end of pagination (Step 4)."""
    soup = BeautifulSoup(listing_html, "lxml")
    return len(soup.select("article.property-item")) == 0


def detect_captcha(html: str) -> bool:
    """Detect a CAPTCHA page (status is still 200)."""
    lowered = html.lower()
    return any(marker in lowered for marker in CAPTCHA_MARKERS)


def classify_fetch_result(result: FetchResult) -> ErrorKind:
    """Classify a fetch result (Step 3):
    - Clear proxy failure, 429, or CAPTCHA -> PROXY_ISSUE.
    - Generic network/server error (None, error, 5xx) -> FETCH_ERROR.
    - 2xx without CAPTCHA -> OK. Other 4xx (e.g. 404) -> FETCH_ERROR.
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
# 4. Protocols for real I/O components (implemented in web_crawler_io.py)
# ============================================================

class ControlPlaneRepo(Protocol):
    """Operations on pipeline.listing_progress / detail_queue / run_state."""

    def apply_daily_reset_if_needed(self, today: date) -> None: ...
    def reclaim_stale_detail_queue(self, older_than_seconds: int) -> int: ...
    def claim_listing_task(self, crawl_date: date) -> Optional[ListingTask]: ...
    def mark_listing_exhausted(self, progress_id: int) -> None: ...
    def enqueue_detail_urls(
        self, urls: Sequence[str], discovered_page_id: int, crawl_date: date
    ) -> None: ...
    def claim_detail_task(self) -> Optional[DetailTask]: ...

    def mark_detail_fetched(self, queue_id: int) -> None:
        """processing -> fetched, called right after buffer.add()."""
        ...

    def mark_details_flushed(self, queue_ids: Sequence[int]) -> None:
        """fetched -> flushed, after a successful buffer.flush(final=False)."""
        ...

    def mark_details_done(self, queue_ids: Sequence[int]) -> None:
        """fetched/flushed -> done, after a successful buffer.flush(final=True)."""
        ...

    def mark_urls_done(self, urls: Sequence[str]) -> None:
        """Same as mark_details_done() but by url — used in Step 7
        (reconciliation no longer has the original queue_id, only the url
        read back from the parquet file)."""
        ...

    def mark_urls_pending(self, urls: Sequence[str]) -> None:
        """Put URLs from a discarded retry parquet back to pending for re-crawl."""
        ...

    def reset_run_progress(self, run_id: str) -> None:
        """Reset the counter after discarding the current run's own partial parquet."""
        ...

    def mark_detail_failed(self, queue_id: int) -> None: ...
    def init_run_state(self, run_id: str) -> None: ...

    def update_run_progress(self, run_id: str, detail_pages_done: int) -> None:
        """Incremental update after each intermediate flush — a safety net
        against a hard kill."""
        ...

    def list_incomplete_runs(self, older_than_seconds: int) -> list[IncompleteRun]:
        """Step 7 — run_state rows with ended_at IS NULL and started_at old enough."""
        ...

    def get_incomplete_run(self, run_id: str) -> Optional[IncompleteRun]:
        """Read the current run to resume after an Airflow retry."""
        ...

    def finalize_run_state(
        self,
        run_id: str,
        stopped_reason: StopReason,
        detail_pages_done: int,
        output_s3_key: Optional[str],
    ) -> None: ...


class ProxyPool(Protocol):
    """Manages the current proxy and rotates when blocked."""

    def current(self) -> Optional[str]: ...
    def rotate(self) -> Optional[str]: ...
    def mark_failed(self, proxy_url: str) -> None: ...

    def refill(self) -> int:
        """Fetch + health-check new proxies once the pool is exhausted. The
        core calls this at most once per detected exhaustion. Returns the
        number of live proxies obtained."""
        ...


class PageFetcher(Protocol):
    """HTTP GET via proxy — always returns a FetchResult, never raises."""

    def fetch(self, url: str, proxy_url: Optional[str]) -> FetchResult: ...


class BufferWriter(Protocol):
    """In-memory accumulation buffer + flush to S3 (Step 5)."""

    def add(self, record: BronzeRecord) -> None: ...

    def flush(self, run_id: str, crawl_date: date, final: bool = False) -> Optional[str]:
        """Flush to the S3 `.inprogress` key (final=False), or rename it to
        the final key (final=True). Returns None if the buffer is empty."""
        ...

    def promote_inprogress_to_final(
        self, run_id: str, crawl_date: date
    ) -> Optional[PromotedFile]:
        """Step 7 — turn a dead run's .inprogress into final, reading its
        urls so the repo can update detail_queue. None if there is nothing
        to promote."""
        ...

    def discard_inprogress(self, run_id: str, crawl_date: date) -> list[str]:
        """Read the urls then delete .inprogress so it can be crawled again from scratch."""
        ...


class Clock(Protocol):
    """Wraps datetime.now()/monotonic()/sleep() so tests don't depend on real time."""

    def now(self) -> datetime: ...
    def monotonic(self) -> float: ...
    def sleep(self, seconds: float) -> None: ...


# ============================================================
# 5. Core orchestrator
# ============================================================

class WebCrawlerCore:
    """Orchestrates the entire DAG 2 crawl loop. Every dependency is
    injected via the constructor for easy unit testing."""

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

    # -------- entry point called from the Airflow PythonOperator --------

    def run(self, run_id: str) -> RunResult:
        """Run DAG 2 once. A detail_queue row only becomes 'done' AFTER its
        data has been successfully flushed to S3 (fetched -> flushed -> done)."""
        today = self.clock.now().date()

        recovered = self._reconcile_crashed_runs(run_id)
        if recovered is not None:
            return recovered

        self.repo.apply_daily_reset_if_needed(today)
        self.repo.reclaim_stale_detail_queue(
            older_than_seconds=self.config.reconcile_stale_run_after_seconds
        )
        self.repo.init_run_state(run_id)

        start_monotonic = self.clock.monotonic()
        last_flush_monotonic = start_monotonic
        detail_pages_done = 0
        pages_since_flush = 0
        early_flush_done = False
        # Never cleared — buffer.flush() re-serializes the entire buffer
        # accumulated since the start of the run, every time.
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
                    # Flush early as soon as min_success_pages is reached, to
                    # protect data in case the run stops abnormally.
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
            # Wrapped in its own try/except so an error here doesn't mask
            # the original exception.
            try:
                output_key = self._flush_and_mark(
                    run_id, today, all_fetched_queue_ids, detail_pages_done, final=True
                )
                self.repo.finalize_run_state(run_id, stop_reason, detail_pages_done, output_key)
            except Exception:
                logger.exception(
                    "Error while flushing/finalizing in finally — ignored, does not mask the original exception"
                )

        return RunResult(stop_reason=stop_reason, detail_pages_done=detail_pages_done)

    # -------- flush + mark corresponding status (Step 5) --------

    def _flush_and_mark(
        self,
        run_id: str,
        crawl_date: date,
        all_fetched_queue_ids: list[int],
        detail_pages_done: int,
        final: bool,
    ) -> Optional[str]:
        """final=False -> mark 'flushed'; final=True -> mark 'done'.
        Always marks the full all_fetched_queue_ids list because each
        buffer.flush() call re-serializes the entire accumulated buffer."""
        output_key = self.buffer.flush(run_id, crawl_date, final=final)
        if output_key is not None and all_fetched_queue_ids:
            if final:
                self.repo.mark_details_done(all_fetched_queue_ids)
            else:
                self.repo.mark_details_flushed(all_fetched_queue_ids)
            self.repo.update_run_progress(run_id, detail_pages_done)
        return output_key

    # -------- Step 7: recover a run killed by SIGKILL/OOM --------

    def _reconcile_crashed_runs(self, current_run_id: str) -> Optional[RunResult]:
        """Resume the current retry, or reconcile older crashed runs.

        If the current retry hasn't reached the threshold, its old parquet
        is discarded and its urls requeued, to avoid overwriting the same
        `.inprogress` file with incomplete data.
        """
        current = self.repo.get_incomplete_run(current_run_id)
        if current is not None:
            crawl_date = current.started_at.date()
            if current.detail_pages_done >= self.config.min_success_pages:
                promoted = self.buffer.promote_inprogress_to_final(current_run_id, crawl_date)
                if promoted is not None:
                    self.repo.mark_urls_done(promoted.urls)
                    self.repo.finalize_run_state(
                        current_run_id, StopReason.RECOVERED,
                        current.detail_pages_done, promoted.final_key,
                    )
                    logger.info(
                        "Current retry: recovered run_id=%s (%d pages, %d urls promoted)",
                        current_run_id, current.detail_pages_done, len(promoted.urls),
                    )
                    return RunResult(
                        stop_reason=StopReason.RECOVERED,
                        detail_pages_done=current.detail_pages_done,
                    )
            else:
                urls = self.buffer.discard_inprogress(current_run_id, crawl_date)
                self.repo.mark_urls_pending(urls)
                self.repo.reset_run_progress(current_run_id)
                logger.info(
                    "Current retry: discarded parquet for run_id=%s, %d urls set back to pending",
                    current_run_id, len(urls),
                )

        incomplete_runs = self.repo.list_incomplete_runs(
            older_than_seconds=self.config.reconcile_stale_run_after_seconds
        )
        for incomplete in incomplete_runs:
            if incomplete.run_id == current_run_id:
                continue
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
                    "Reconciliation: recovered run_id=%s (%d pages, %d urls promoted)",
                    incomplete.run_id, incomplete.detail_pages_done, len(promoted.urls),
                )
            else:
                urls = self.buffer.discard_inprogress(incomplete.run_id, crawl_date)
                self.repo.mark_urls_pending(urls)
                self.repo.finalize_run_state(
                    incomplete.run_id, StopReason.INCOMPLETE,
                    incomplete.detail_pages_done, None,
                )
                logger.warning(
                    "Reconciliation: run_id=%s not eligible for recovery "
                    "(%d pages < %d minimum) -> %d urls set back to pending, parquet discarded",
                    incomplete.run_id, incomplete.detail_pages_done,
                    self.config.min_success_pages, len(urls),
                )
        return None

    # -------- Step 5: process one detail page --------

    def _process_detail_task(self, task: DetailTask) -> Optional[StopReason]:
        result, stop_reason = self._fetch_with_retry(task.url)

        if stop_reason is not None:
            self.repo.mark_detail_failed(task.queue_id)
            return stop_reason

        assert result.html is not None  # OK always has html (see classify_fetch_result)
        record = BronzeRecord(
            url=task.url,
            crawl_date=self.clock.now(),
            html=result.html.encode("utf-8"),
        )
        self.buffer.add(record)
        # Only mark 'fetched' here — 'done' happens later in
        # _flush_and_mark(), once the data has been flushed to S3 successfully.
        self.repo.mark_detail_fetched(task.queue_id)
        return None

    # -------- Step 4: process one listing page --------

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

    # -------- Steps 2-3: shared fetch for listing/detail, with retry --------

    def _fetch_with_retry(self, url: str) -> tuple[FetchResult, Optional[StopReason]]:
        """stop_reason=None means the fetch succeeded; otherwise the run stops.

        Retry rules:
          - PROXY_ISSUE: rotate immediately, repeat until proxies run out
            (-> PROXY_EXHAUSTED).
          - FETCH_ERROR: retry the same proxy up to max_fetch_error_retries
            times; give up and stop the run once exhausted.

        Proxy rule: never fetch using the real IP. If the pool is empty,
        refill exactly once; still empty -> PROXY_EXHAUSTED.
        """
        same_proxy_attempts = 0
        already_refilled = False

        while True:
            proxy = self.proxy_pool.current()

            if proxy is None:
                stop_reason = self._handle_pool_exhausted(already_refilled)
                if stop_reason is not None:
                    return (
                        FetchResult(status_code=None, error="proxy pool exhausted, cannot continue"),
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
        """Pool is empty: refill exactly once; still empty -> PROXY_EXHAUSTED."""
        if already_refilled:
            return StopReason.PROXY_EXHAUSTED
        new_count = self.proxy_pool.refill()
        if new_count <= 0:
            return StopReason.PROXY_EXHAUSTED
        return None

    def _sleep_between_requests(self) -> None:
        """Random delay between requests (concurrency=1, respectful of site load)."""
        delay = self.rng.uniform(
            self.config.delay_min_seconds, self.config.delay_max_seconds
        )
        self.clock.sleep(delay)
