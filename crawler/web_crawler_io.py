"""
crawler/web_crawler_io.py

Thành phần 2 (Web Crawler) — implement các Protocol từ web_crawler_core.py:
    - PsycopgControlPlaneRepo -> Postgres (pipeline.listing_progress/detail_queue/run_state)
    - RequestsPageFetcher     -> HTTP GET qua proxy
    - S3ParquetBufferWriter   -> buffer + flush S3 (boto3 + pyarrow)
    - SystemClock             -> thời gian thật (Asia/Ho_Chi_Minh)
    - run_dag2()              -> hàm wiring duy nhất gọi từ dags/web_crawler.py

Core không import psycopg2/boto3/requests — chỉ module này xử lý I/O thật.
Cấu hình đọc từ crawler/config.py, không tự đọc os.environ rải rác.
"""


from __future__ import annotations

import io
import logging
import random
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Sequence
from zoneinfo import ZoneInfo

import boto3
import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from crawler.web_crawler_core import (
    WebCrawlerCore,
    BronzeRecord,
    CrawlerConfig,
    DetailTask,
    FetchResult,
    IncompleteRun,
    ListingTask,
    PromotedFile,
    RunResult,
    StopReason,
    PROVINCES,       
    LISTING_TYPES,     
    PROPERTY_TYPES, 
)
from crawler import config
from crawler.proxy_manager import ProxyPool

logger = logging.getLogger("web_crawler_io")

HCM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# User-Agent xoay vòng — giảm rủi ro bị nhận diện bot qua header cố định.
USER_AGENTS: tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
)


# ============================================================
# Clock thật
# ============================================================

class SystemClock:
    """Clock thật, quy đổi về Asia/Ho_Chi_Minh (crawl_date/daily-reset theo
    ngày lịch HCMC, không phải ngày UTC)."""

    def now(self) -> datetime:
        return datetime.now(HCM_TZ)

    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


# ============================================================
# Bước 1: Control-plane repo (psycopg2)
# ============================================================

def _sanitize_run_id_for_key(run_id: str) -> str:
    """Làm sạch run_id trước khi dùng trong S3 key."""
    return run_id.replace(":", "-").replace("+00:00", "Z").replace("+", "-")


class PsycopgControlPlaneRepo:
    """Implement ControlPlaneRepo bằng psycopg2, thao tác schema `pipeline`.
    autocommit=True — mỗi method là 1 statement độc lập (kể cả UPDATE...RETURNING
    với FOR UPDATE SKIP LOCKED), không cần quản lý transaction thủ công."""

    def __init__(self, dsn: str) -> None:
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = True

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "PsycopgControlPlaneRepo":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _cursor(self):
        return self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # -------- daily reset & reclaim --------

    def apply_daily_reset_if_needed(self, today: date) -> None:
        """INSERT 30 tổ hợp mới cho `today` nếu chưa có. UNIQUE + ON CONFLICT
        DO NOTHING đảm bảo idempotent khi gọi nhiều lần."""
        rows = [
            (pv, lt, pt, today)
            for pv in PROVINCES for lt in LISTING_TYPES for pt in PROPERTY_TYPES
        ]
        with self._cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO pipeline.listing_progress
                    (province_old, listing_type, property_type, current_page, status, crawl_date)
                VALUES %s
                ON CONFLICT (province_old, listing_type, property_type, crawl_date) DO NOTHING
                """,
                [(pv, lt, pt, 1, "active", cd) for pv, lt, pt, cd in rows],
            )

    def reclaim_stale_detail_queue(self, older_than_seconds: int) -> int:
        """Reset dòng 'processing'/'fetched'/'flushed' treo quá
        ngưỡng stale về 'pending', giữ nguyên discovered_at."""
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.detail_queue
                SET status = 'pending', claimed_at = NULL
                WHERE status IN ('processing', 'fetched', 'flushed')
                  AND claimed_at < now() - (%s * interval '1 second')
                RETURNING id
                """,
                (older_than_seconds,),
            )
            return cur.rowcount

    # -------- listing_progress --------

    def claim_listing_task(self, crawl_date: date) -> Optional[ListingTask]:
        """Claim tổ hợp `active` có current_page nhỏ nhất (tie-break id ASC)."""
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.listing_progress
                SET current_page = current_page + 1, updated_at = now()
                WHERE id = (
                    SELECT id FROM pipeline.listing_progress
                    WHERE status = 'active' AND crawl_date = %s
                    ORDER BY current_page ASC, id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING id, province_old, listing_type, property_type, current_page - 1 AS page_to_crawl
                """,
                (crawl_date,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return ListingTask(
                progress_id=row["id"],
                province_old=row["province_old"],
                listing_type=row["listing_type"],
                property_type=row["property_type"],
                page_to_crawl=row["page_to_crawl"],
            )

    def mark_listing_exhausted(self, progress_id: int) -> None:
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.listing_progress
                SET status = 'exhausted', updated_at = now()
                WHERE id = %s
                """,
                (progress_id,),
            )

    # -------- detail_queue --------

    def enqueue_detail_urls(
        self, urls: Sequence[str], discovered_page_id: int, crawl_date: date
    ) -> None:
        """INSERT hàng loạt, dedup bằng UNIQUE(url)."""
        if not urls:
            return
        with self._cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO pipeline.detail_queue
                    (url, discovered_page_id, crawl_date)
                VALUES %s
                ON CONFLICT (url) DO NOTHING
                """,
                [(url, discovered_page_id, crawl_date) for url in urls],
            )

    def claim_detail_task(self) -> Optional[DetailTask]:
        """Claim URL `pending` có discovered_at nhỏ nhất — FIFO."""
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.detail_queue
                SET status = 'processing', claimed_at = now()
                WHERE id = (
                    SELECT id FROM pipeline.detail_queue
                    WHERE status = 'pending'
                    ORDER BY discovered_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING id, url
                """
            )
            row = cur.fetchone()
            if row is None:
                return None
            return DetailTask(queue_id=row["id"], url=row["url"])

    def mark_detail_fetched(self, queue_id: int) -> None:
        """processing -> fetched, gọi ngay sau buffer.add()."""
        with self._cursor() as cur:
            cur.execute(
                "UPDATE pipeline.detail_queue SET status = 'fetched' WHERE id = %s",
                (queue_id,),
            )

    def mark_details_flushed(self, queue_ids: Sequence[int]) -> None:
        """fetched -> flushed, sau buffer.flush(final=False) thành công."""
        if not queue_ids:
            return
        with self._cursor() as cur:
            cur.execute(
                "UPDATE pipeline.detail_queue SET status = 'flushed' "
                "WHERE id = ANY(%s) AND status = 'fetched'",
                (list(queue_ids),),
            )

    def mark_details_done(self, queue_ids: Sequence[int]) -> None:
        """fetched/flushed -> done, sau buffer.flush(final=True) thành công."""
        if not queue_ids:
            return
        with self._cursor() as cur:
            cur.execute(
                "UPDATE pipeline.detail_queue SET status = 'done' "
                "WHERE id = ANY(%s) AND status IN ('fetched', 'flushed')",
                (list(queue_ids),),
            )

    def mark_urls_done(self, urls: Sequence[str]) -> None:
        """Như mark_details_done() nhưng theo url — dùng ở Bước 7
        (reconciliation chỉ đọc lại được url từ file parquet vừa promote)."""
        if not urls:
            return
        with self._cursor() as cur:
            cur.execute(
                "UPDATE pipeline.detail_queue SET status = 'done' "
                "WHERE url = ANY(%s) AND status IN ('fetched', 'flushed')",
                (list(urls),),
            )

    def mark_urls_pending(self, urls: Sequence[str]) -> None:
        """Đưa URL của artifact retry dở về pending để crawl lại."""
        if not urls:
            return
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.detail_queue
                SET status = 'pending', claimed_at = NULL
                WHERE url = ANY(%s)
                  AND status IN ('processing', 'fetched', 'flushed')
                """,
                (list(urls),),
            )

    def mark_detail_failed(self, queue_id: int) -> None:
        with self._cursor() as cur:
            cur.execute(
                "UPDATE pipeline.detail_queue SET status = 'failed' WHERE id = %s",
                (queue_id,),
            )

    # -------- run_state --------

    def init_run_state(self, run_id: str) -> None:
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline.run_state (run_id, started_at)
                VALUES (%s, now())
                ON CONFLICT (run_id) DO NOTHING
                """,
                (run_id,),
            )

    def update_run_progress(self, run_id: str, detail_pages_done: int) -> None:
        """Cập nhật incremental, không đụng ended_at/stopped_reason — lưới
        an toàn khi bị kill cứng giữa 2 lần flush."""
        with self._cursor() as cur:
            cur.execute(
                "UPDATE pipeline.run_state SET detail_pages_done = %s WHERE run_id = %s",
                (detail_pages_done, run_id),
            )

    def reset_run_progress(self, run_id: str) -> None:
        """Reset bộ đếm sau khi bỏ artifact dở của chính run hiện tại."""
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.run_state
                SET detail_pages_done = 0,
                    stopped_reason = NULL,
                    ended_at = NULL,
                    output_s3_key = NULL
                WHERE run_id = %s
                """,
                (run_id,),
            )

    def list_incomplete_runs(self, older_than_seconds: int) -> list[IncompleteRun]:
        """Bước 7 — run_state có ended_at IS NULL và started_at đã đủ cũ.
        older_than_seconds do core.py truyền vào, không đọc lại
        STALE_RUN_THRESHOLD_SQL để tránh 2 nguồn sự thật lệch nhau."""
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT run_id, started_at, detail_pages_done
                FROM pipeline.run_state
                WHERE ended_at IS NULL
                  AND started_at < now() - (%s * interval '1 second')
                """,
                (older_than_seconds,),
            )
            rows = cur.fetchall()
        return [
            IncompleteRun(
                run_id=row["run_id"],
                started_at=row["started_at"],
                detail_pages_done=row["detail_pages_done"],
            )
            for row in rows
        ]

    def get_incomplete_run(self, run_id: str) -> Optional[IncompleteRun]:
        """Đọc run hiện tại để Airflow retry có thể resume artifact của nó."""
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT run_id, started_at, detail_pages_done
                FROM pipeline.run_state
                WHERE run_id = %s AND ended_at IS NULL
                """,
                (run_id,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return IncompleteRun(
            run_id=row["run_id"],
            started_at=row["started_at"],
            detail_pages_done=row["detail_pages_done"],
        )

    def finalize_run_state(
        self,
        run_id: str,
        stopped_reason: StopReason,
        detail_pages_done: int,
        output_s3_key: Optional[str],
    ) -> None:
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.run_state
                SET ended_at = now(),
                    stopped_reason = %s,
                    detail_pages_done = %s,
                    output_s3_key = %s
                WHERE run_id = %s
                """,
                (stopped_reason.value, detail_pages_done, output_s3_key, run_id),
            )


# ============================================================
# Bước 2-3: Page fetcher (requests)
# ============================================================

class RequestsPageFetcher:
    """Không raise exception — mọi lỗi (timeout, connect-fail, DNS...)
    được bọc vào FetchResult.error để core tự phân loại."""

    def __init__(
        self,
        connect_timeout: float = config.CONNECT_TIMEOUT_SECONDS,
        read_timeout: float = config.READ_TIMEOUT_SECONDS,
    ) -> None:
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout

    def fetch(self, url: str, proxy_url: Optional[str]):
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.5",
        }
        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None

        try:
            response = requests.get(
                url,
                headers=headers,
                proxies=proxies,
                timeout=(self._connect_timeout, self._read_timeout),
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            # Proxy không dùng được -> đổi proxy ngay.
            logger.warning("Proxy lỗi/treo url=%s proxy=%s: %s", url, proxy_url, exc)
            return FetchResult(status_code=None, error=str(exc), is_proxy_error=True)
        except requests.exceptions.RequestException as exc:
            # Lỗi khác (VD TooManyRedirects, InvalidURL) -> retry cùng proxy.
            logger.warning("Fetch lỗi kỹ thuật url=%s proxy=%s: %s", url, proxy_url, exc)
            return FetchResult(status_code=None, error=str(exc))

        return FetchResult(
            status_code=response.status_code,
            html=response.text,
        )


# ============================================================
# Bước 5: Buffer tích luỹ + flush S3 (boto3 + pyarrow)
# ============================================================

class S3ParquetBufferWriter:
    """Tích luỹ toàn bộ record trong bộ nhớ, mỗi flush ghi đè lên cùng
    1 S3 key `.inprogress` để mô phỏng append."""

    def __init__(self, bucket: str, s3_client=None) -> None:
        self._bucket = bucket
        self._s3 = s3_client or boto3.client("s3")
        self._records: list[BronzeRecord] = []

    def add(self, record: BronzeRecord) -> None:
        self._records.append(record)

    def inprogress_key(self, run_id: str, crawl_date: date) -> str:
        return f"bronze/web/date={crawl_date.isoformat()}/part-{_sanitize_run_id_for_key(run_id)}.parquet.inprogress"

    def final_key(self, run_id: str, crawl_date: date) -> str:
        return f"bronze/web/date={crawl_date.isoformat()}/part-{_sanitize_run_id_for_key(run_id)}.parquet"

    def _serialize_current_buffer(self) -> bytes:
        table = pa.table(
            {
                "url": pa.array([r.url for r in self._records], type=pa.string()),
                "crawl_date": pa.array(
                    [r.crawl_date for r in self._records], type=pa.timestamp("us")
                ),
                "html": pa.array([r.html for r in self._records], type=pa.binary()),
            }
        )
        sink = io.BytesIO()
        pq.write_table(table, sink)
        return sink.getvalue()

    def flush(self, run_id: str, crawl_date: date, final: bool = False) -> Optional[str]:
        if not self._records:
            return None

        inprogress_key = self.inprogress_key(run_id, crawl_date)
        body = self._serialize_current_buffer()
        self._s3.put_object(Bucket=self._bucket, Key=inprogress_key, Body=body)
        logger.info(
            "Flush %d record lên s3://%s/%s (final=%s)",
            len(self._records), self._bucket, inprogress_key, final,
        )

        if not final:
            return inprogress_key

        # final=True: copy sang key chính thức rồi xoá .inprogress.
        final_key = self.final_key(run_id, crawl_date)
        self._s3.copy_object(
            Bucket=self._bucket,
            CopySource={"Bucket": self._bucket, "Key": inprogress_key},
            Key=final_key,
        )

        try:
            self._s3.delete_object(Bucket=self._bucket, Key=inprogress_key)
        except Exception as exc:  # noqa: BLE001 - dọn dẹp best-effort, dữ liệu đã an toàn ở final_key
            logger.warning(
                "Copy thành công (%s) nhưng KHÔNG xoá được %s: %s",
                final_key, inprogress_key, exc,
            )
        else:
            logger.info("Rename hoàn tất -> s3://%s/%s", self._bucket, final_key)
        return final_key

    # -------- Bước 7: promote .inprogress của run đã chết thành final --------

    def _object_exists(self, key: str) -> bool:
        try:
            self._s3.head_object(Bucket=self._bucket, Key=key)
            return True
        except Exception:  # noqa: BLE001 - không tồn tại hoặc lỗi truy cập đều coi "chưa có"
            return False

    def promote_inprogress_to_final(
        self, run_id: str, crawl_date: date
    ) -> Optional[PromotedFile]:
        """Đổi .inprogress (từ run đã chết) thành final, đọc url trong đó để
        repo cập nhật detail_queue. Idempotent — nếu final đã tồn tại từ lần
        gọi trước, đọc thẳng từ final, không copy lại."""
        ikey = self.inprogress_key(run_id, crawl_date)
        fkey = self.final_key(run_id, crawl_date)

        already_promoted = self._object_exists(fkey)
        source_key = fkey if already_promoted else ikey
        if not already_promoted and not self._object_exists(ikey):
            return None  # run chết trước khi kịp flush lần nào

        body = self._s3.get_object(Bucket=self._bucket, Key=source_key)["Body"].read()
        table = pq.read_table(io.BytesIO(body), columns=["url"])
        urls = table.column("url").to_pylist()

        if not already_promoted:
            self._s3.copy_object(
                Bucket=self._bucket,
                CopySource={"Bucket": self._bucket, "Key": ikey},
                Key=fkey,
            )
            try:
                self._s3.delete_object(Bucket=self._bucket, Key=ikey)
            except Exception as exc:  # noqa: BLE001 - dữ liệu đã an toàn ở fkey
                logger.warning(
                    "Promote thành công (%s) nhưng KHÔNG xoá được %s: %s", fkey, ikey, exc
                )
            else:
                logger.info("Bước 7: promote hoàn tất -> s3://%s/%s", self._bucket, fkey)

        return PromotedFile(final_key=fkey, urls=urls)

    def discard_inprogress(self, run_id: str, crawl_date: date) -> list[str]:
        """Đọc URL rồi xóa .inprogress để retry crawl lại từ đầu."""
        ikey = self.inprogress_key(run_id, crawl_date)
        if not self._object_exists(ikey):
            return []

        body = self._s3.get_object(Bucket=self._bucket, Key=ikey)["Body"].read()
        table = pq.read_table(io.BytesIO(body), columns=["url"])
        urls = table.column("url").to_pylist()
        self._s3.delete_object(Bucket=self._bucket, Key=ikey)
        logger.info(
            "Recovery: đã xóa .inprogress %s để crawl lại %d URL",
            ikey, len(urls),
        )
        return urls

    # -------- Dọn .inprogress mồ côi (đầu mỗi run) --------

    def list_orphaned_inprogress(self, prefix: str = "bronze/") -> list[str]:
        """Liệt kê key `.inprogress` đã có bản final tương ứng."""
        paginator = self._s3.get_paginator("list_objects_v2")
        all_keys: set[str] = set()
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                all_keys.add(obj["Key"])

        return sorted(
            key for key in all_keys
            if key.endswith(".parquet.inprogress") and key[: -len(".inprogress")] in all_keys
        )

    def cleanup_orphaned_inprogress(self, prefix: str = "bronze/") -> int:
        """Xoá các `.inprogress` mồ côi. Không raise, chỉ log warning nếu lỗi."""
        try:
            orphaned = self.list_orphaned_inprogress(prefix)
        except Exception as exc:  # noqa: BLE001 - không được chặn crawl chính
            logger.warning("Không liệt kê được .inprogress mồ côi (bỏ qua): %s", exc)
            return 0

        deleted = 0
        for key in orphaned:
            try:
                self._s3.delete_object(Bucket=self._bucket, Key=key)
                deleted += 1
                logger.info("Đã dọn .inprogress mồ côi: %s", key)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Không xoá được %s (bỏ qua, thử lại ở run sau): %s", key, exc)

        if deleted:
            logger.info("Dọn dẹp đầu run: đã xoá %d file .inprogress mồ côi", deleted)
        return deleted


# ============================================================
# Factory — lắp ráp core từ biến môi trường (.env)
# ============================================================

def build_repo_from_env() -> PsycopgControlPlaneRepo:
    return PsycopgControlPlaneRepo(config.get_postgres_dsn())


def build_buffer_from_env() -> S3ParquetBufferWriter:
    return S3ParquetBufferWriter(bucket=config.get_s3_bucket())


def build_proxy_pool_from_env(auto_refill: bool = True) -> ProxyPool:
    """auto_refill=True -> refill ngay để có proxy sẵn khi run bắt đầu."""
    pool = ProxyPool()
    if auto_refill:
        pool.refill()
    return pool


# ============================================================
# Wiring — điểm gọi duy nhất cho PythonOperator (dags/web_crawler.py)
# ============================================================

# stop_reason nhóm này = hoàn thành bình thường (Airflow SUCCESS). Ngoài
# nhóm này vẫn coi thành công nếu crawl >= WEB_CRAWLER_MIN_SUCCESS_PAGES.
NORMAL_STOP_REASONS = frozenset({
    StopReason.MAX_PAGES,
    StopReason.TIME_BOX,
    StopReason.NO_MORE_DATA,
})


def is_success(result: RunResult) -> bool:
    """Thành công nếu stop_reason thuộc NORMAL_STOP_REASONS hoặc crawl đủ
    WEB_CRAWLER_MIN_SUCCESS_PAGES."""
    if result.stop_reason in NORMAL_STOP_REASONS:
        return True
    return result.detail_pages_done >= config.WEB_CRAWLER_MIN_SUCCESS_PAGES


def run_dag2(run_id: Optional[str] = None) -> str:
    """Điểm gọi duy nhất từ DAG 2. Lắp repo/proxy/buffer/fetcher/clock, dọn
    .inprogress mồ côi, chạy 1 lần (gồm cả Bước 7 cho run trước bị crash
    cứng), luôn đóng Postgres. Raise RuntimeError nếu is_success()=False."""
    if not run_id:
        run_id = f"web-{datetime.now(HCM_TZ):%Y%m%dT%H%M%S}"

    repo = build_repo_from_env()
    try:
        buffer = build_buffer_from_env()
        buffer.cleanup_orphaned_inprogress()

        core = WebCrawlerCore(
            repo=repo,
            proxy_pool=build_proxy_pool_from_env(),
            fetcher=RequestsPageFetcher(),
            buffer=buffer,
            clock=SystemClock(),
            config=CrawlerConfig(
                max_detail_pages_per_run=config.WEB_CRAWLER_MAX_DETAIL_PAGES_PER_RUN,
                time_box_seconds=config.WEB_CRAWLER_TIME_BOX_SECONDS,
                delay_min_seconds=config.WEB_CRAWLER_DELAY_MIN_SECONDS,
                delay_max_seconds=config.WEB_CRAWLER_DELAY_MAX_SECONDS,
                max_fetch_error_retries=config.WEB_CRAWLER_MAX_FETCH_ERROR_RETRIES,
                flush_interval_seconds=config.WEB_CRAWLER_FLUSH_INTERVAL_SECONDS,
                flush_page_threshold=config.WEB_CRAWLER_FLUSH_PAGE_THRESHOLD,
                min_success_pages=config.WEB_CRAWLER_MIN_SUCCESS_PAGES,
                reconcile_stale_run_after_seconds=(
                    config.WEB_CRAWLER_RECONCILE_STALE_RUN_AFTER_SECONDS
                ),
            ),
        )
        result = core.run(run_id)
    finally:
        repo.close()

    logger.info(
        "DAG2 run_id=%s kết thúc: stop_reason=%s, detail_pages_done=%d",
        run_id, result.stop_reason.value, result.detail_pages_done,
    )

    if not is_success(result):
        raise RuntimeError(
            f"DAG2 run_id={run_id} thất bại: stop_reason={result.stop_reason.value}, "
            f"chỉ crawl được {result.detail_pages_done} trang (< "
            f"{config.WEB_CRAWLER_MIN_SUCCESS_PAGES} trang tối thiểu). "
            f"Chi tiết: bảng pipeline.run_state hoặc log task phía trên."
        )

    if result.stop_reason not in NORMAL_STOP_REASONS:
        logger.info(
            "DAG2 run_id=%s: stop_reason bất thường (%s) nhưng đã crawl đủ "
            "%d trang (>= %d) -> vẫn tính là thành công.",
            run_id, result.stop_reason.value, result.detail_pages_done,
            config.WEB_CRAWLER_MIN_SUCCESS_PAGES,
        )

    return result.stop_reason.value
