"""
crawler/bronze_crawler_io.py

Implement THẬT các Protocol khai báo trong bronze_crawler_core.py:
    - PsycopgControlPlaneRepo -> crawl.listing_progress / detail_queue / run_state (psycopg2)
    - RequestsPageFetcher     -> HTTP GET qua proxy (requests)
    - S3ParquetBufferWriter   -> buffer tích luỹ + flush S3 (boto3 + pyarrow)
    - SystemClock             -> thời gian thật, múi giờ Asia/Ho_Chi_Minh
    - run_dag2()              -> hàm wiring DUY NHẤT mà dags/crawl_alonhadat_web.py gọi

Core (bronze_crawler_core.py) hoàn toàn không biết đến module này — mọi
import psycopg2/boto3/requests CHỈ nằm ở đây, đúng nguyên tắc tách biệt
logic thuần khỏi I/O thật.

VỀ PROXY: dùng thẳng `proxy_manager.ProxyPool` (đã implement đúng Protocol
`ProxyPool` của core: current/rotate/mark_failed) — xem `build_proxy_pool_from_env()`
bên dưới. `SimpleProxyPool` tạm thời trước đây ĐÃ BỊ XOÁ khỏi file này.
"""

from __future__ import annotations

import io
import logging
import os
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

from crawler.bronze_crawler_core import (
    BronzeCrawlerCore,
    BronzeRecord,
    DetailTask,
    FetchResult,
    ListingTask,
    StopReason,
)
from crawler.proxy_manager import ProxyPool

logger = logging.getLogger("bronze_crawler_io")

HCM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# User-Agent xoay vòng đơn giản — giảm rủi ro bị nhận diện bot qua header cố định.
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
# 1. Clock thật
# ============================================================

class SystemClock:
    """Implement Protocol Clock bằng thời gian hệ thống thật, quy đổi về
    múi giờ Asia/Ho_Chi_Minh (crawl_date/daily-reset phải theo ngày lịch
    HCMC, không phải ngày UTC)."""

    def now(self) -> datetime:
        return datetime.now(HCM_TZ)

    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


# ============================================================
# 2. Control-plane repo (psycopg2) — crawl.listing_progress / detail_queue / run_state
# ============================================================

LISTING_TYPES = ("can-ban", "cho-thue")
PROPERTY_TYPES = (
    "nha-mat-tien",
    "nha-trong-hem",
    "biet-thu-nha-lien-ke",
    "can-ho-chung-cu",
    "phong-tro-nha-tro",
)


class PsycopgControlPlaneRepo:
    """Implement Protocol ControlPlaneRepo bằng psycopg2, thao tác schema
    `crawl` trong `postgres-dw` (DDL: sql/002_crawl_schema.sql).

    Dùng `autocommit=True`: mỗi method là 1 statement SQL độc lập (kể cả
    claim pattern UPDATE...RETURNING vốn đã atomic trong 1 statement nhờ
    FOR UPDATE SKIP LOCKED), nên không cần quản lý transaction thủ công."""

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
        """INSERT 10 tổ hợp mới cho `today` nếu chưa có — giữ nguyên dòng
        ngày cũ (B1). UNIQUE(listing_type, property_type, crawl_date) +
        ON CONFLICT DO NOTHING đảm bảo idempotent nếu gọi lại nhiều lần."""
        rows = [
            (lt, pt, today) for lt in LISTING_TYPES for pt in PROPERTY_TYPES
        ]
        with self._cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO crawl.listing_progress
                    (listing_type, property_type, current_page, status, crawl_date)
                VALUES %s
                ON CONFLICT (listing_type, property_type, crawl_date) DO NOTHING
                """,
                [(lt, pt, 1, "active", cd) for lt, pt, cd in rows],
            )

    def reclaim_stale_detail_queue(self) -> int:
        """Reset các dòng `in_progress` còn treo từ run trước (crash) về
        `pending`, giữ nguyên `discovered_at` (B2)."""
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE crawl.detail_queue
                SET status = 'pending', claimed_at = NULL
                WHERE status = 'in_progress'
                RETURNING id
                """
            )
            return cur.rowcount

    # -------- listing_progress --------

    def claim_listing_task(self, crawl_date: date) -> Optional[ListingTask]:
        """Claim tổ hợp `active` có `current_page` nhỏ nhất (tie-break id
        ASC) — nguyên văn claim pattern đã thống nhất (mục 5)."""
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE crawl.listing_progress
                SET current_page = current_page + 1, updated_at = now()
                WHERE id = (
                    SELECT id FROM crawl.listing_progress
                    WHERE status = 'active' AND crawl_date = %s
                    ORDER BY current_page ASC, id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING id, listing_type, property_type, current_page - 1 AS page_to_crawl
                """,
                (crawl_date,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return ListingTask(
                progress_id=row["id"],
                listing_type=row["listing_type"],
                property_type=row["property_type"],
                page_to_crawl=row["page_to_crawl"],
            )

    def mark_listing_exhausted(self, progress_id: int) -> None:
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE crawl.listing_progress
                SET status = 'exhausted', updated_at = now()
                WHERE id = %s
                """,
                (progress_id,),
            )

    # -------- detail_queue --------

    def enqueue_detail_urls(
        self, urls: Sequence[str], discovered_page_id: int, crawl_date: date
    ) -> None:
        """INSERT hàng loạt, dedup qua UNIQUE(url) + ON CONFLICT DO NOTHING
        (B5). Không có gì để làm nếu danh sách rỗng (trang liệt kê không
        phải lúc nào cũng có tin mới)."""
        if not urls:
            return
        with self._cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO crawl.detail_queue
                    (url, discovered_page_id, crawl_date)
                VALUES %s
                ON CONFLICT (url) DO NOTHING
                """,
                [(url, discovered_page_id, crawl_date) for url in urls],
            )

    def claim_detail_task(self) -> Optional[DetailTask]:
        """Claim URL `pending` có `discovered_at` nhỏ nhất — FIFO (B8)."""
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE crawl.detail_queue
                SET status = 'in_progress', claimed_at = now()
                WHERE id = (
                    SELECT id FROM crawl.detail_queue
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

    def mark_detail_done(self, queue_id: int) -> None:
        with self._cursor() as cur:
            cur.execute(
                "UPDATE crawl.detail_queue SET status = 'done' WHERE id = %s",
                (queue_id,),
            )

    def mark_detail_failed(self, queue_id: int) -> None:
        with self._cursor() as cur:
            cur.execute(
                "UPDATE crawl.detail_queue SET status = 'failed' WHERE id = %s",
                (queue_id,),
            )

    # -------- run_state --------

    def init_run_state(self, run_id: str) -> None:
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO crawl.run_state (run_id, started_at)
                VALUES (%s, now())
                ON CONFLICT (run_id) DO NOTHING
                """,
                (run_id,),
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
                UPDATE crawl.run_state
                SET ended_at = now(),
                    stopped_reason = %s,
                    detail_pages_done = %s,
                    output_s3_key = %s
                WHERE run_id = %s
                """,
                (stopped_reason.value, detail_pages_done, output_s3_key, run_id),
            )


# ============================================================
# 3. Page fetcher (requests) — B4/B9/B10
# ============================================================

@dataclass(frozen=True)
class RequestsFetcherConfig:
    connect_timeout_seconds: float = 10.0
    read_timeout_seconds: float = 30.0


class RequestsPageFetcher:
    """Implement Protocol PageFetcher bằng `requests`. KHÔNG BAO GIỜ raise
    exception ra ngoài — mọi lỗi kỹ thuật (timeout, connect-fail, DNS...)
    được bọc vào FetchResult.error để core tự phân loại (classify_fetch_result)."""

    def __init__(self, config: RequestsFetcherConfig = RequestsFetcherConfig()) -> None:
        self._config = config

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
                timeout=(self._config.connect_timeout_seconds, self._config.read_timeout_seconds),
            )
        except requests.exceptions.ConnectionError as exc:
            # ProxyError / SSLError / ConnectTimeout đều là ConnectionError —
            # dấu hiệu RÕ RÀNG proxy chết (không tunnel/handshake được), khác
            # bản chất với timeout đọc response thông thường. Xác nhận từ log
            # thực tế 2026-08-19: retry mù cùng 1 proxy chết 3 lần là vô ích
            # -> đánh dấu is_proxy_error để core đổi proxy ngay (B12), không
            # đi qua nhánh retry-cùng-proxy (B11).
            logger.warning("Proxy lỗi kết nối url=%s proxy=%s: %s", url, proxy_url, exc)
            return FetchResult(status_code=None, error=str(exc), is_proxy_error=True)
        except requests.exceptions.RequestException as exc:
            # ReadTimeout và các lỗi kỹ thuật khác — proxy đã connect được,
            # chỉ là chờ phản hồi lâu (có thể do site chậm, chưa chắc do
            # proxy) -> vẫn giữ B11 (retry cùng proxy trước khi bỏ cuộc).
            logger.warning("Fetch lỗi kỹ thuật url=%s proxy=%s: %s", url, proxy_url, exc)
            return FetchResult(status_code=None, error=str(exc))

        retry_after = response.headers.get("Retry-After")
        return FetchResult(
            status_code=response.status_code,
            html=response.text,
            retry_after_seconds=int(retry_after) if retry_after and retry_after.isdigit() else None,
        )


# ============================================================
# 4. Buffer tích luỹ + flush S3 (boto3 + pyarrow) — B13/B14
# ============================================================

class S3ParquetBufferWriter:
    """Implement Protocol BufferWriter. Buffer tích luỹ TOÀN BỘ record
    trong bộ nhớ (không clear sau mỗi flush), mỗi lần flush() GHI ĐÈ toàn
    bộ buffer hiện tại lên cùng 1 S3 key `.inprogress` — mô phỏng "append"
    vì S3 object là immutable (quyết định đã thống nhất, mục 7)."""

    def __init__(self, bucket: str, s3_client=None) -> None:
        self._bucket = bucket
        self._s3 = s3_client or boto3.client("s3")
        self._records: list[BronzeRecord] = []

    def add(self, record: BronzeRecord) -> None:
        self._records.append(record)

    def _inprogress_key(self, run_id: str, crawl_date: date) -> str:
        return f"bronze/web/date={crawl_date.isoformat()}/part-{run_id}.parquet.inprogress"

    def _final_key(self, run_id: str, crawl_date: date) -> str:
        return f"bronze/web/date={crawl_date.isoformat()}/part-{run_id}.parquet"

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

        inprogress_key = self._inprogress_key(run_id, crawl_date)
        body = self._serialize_current_buffer()
        self._s3.put_object(Bucket=self._bucket, Key=inprogress_key, Body=body)
        logger.info(
            "Flush %d record lên s3://%s/%s (final=%s)",
            len(self._records), self._bucket, inprogress_key, final,
        )

        if not final:
            return inprogress_key

        # B14: copy .inprogress -> key chính thức, rồi xoá .inprogress
        # để Parser (chỉ quét key KHÔNG có đuôi .inprogress) không đọc nhầm.
        final_key = self._final_key(run_id, crawl_date)
        self._s3.copy_object(
            Bucket=self._bucket,
            CopySource={"Bucket": self._bucket, "Key": inprogress_key},
            Key=final_key,
        )
        # Dữ liệu đã AN TOÀN ở final_key ngay khi copy_object() xong — bước
        # xoá .inprogress chỉ là dọn dẹp. KHÔNG được để lỗi xoá (VD thiếu
        # quyền IAM s3:DeleteObject — đã gặp thực tế 2026-08-19) làm crash
        # cả run và khiến Airflow coi cả lần crawl là thất bại dù dữ liệu
        # đã lưu đúng. Log cảnh báo, để lại 1 bản .inprogress mồ côi vô hại
        # thay vì mất cả kết quả crawl.
        try:
            self._s3.delete_object(Bucket=self._bucket, Key=inprogress_key)
        except Exception as exc:  # noqa: BLE001 - cố ý bắt rộng, đây là bước dọn dẹp best-effort
            logger.warning(
                "Copy thành công (%s) nhưng KHÔNG xoá được %s — dữ liệu vẫn an "
                "toàn, chỉ còn 1 bản .inprogress thừa cần dọn thủ công sau: %s",
                final_key, inprogress_key, exc,
            )
        else:
            logger.info("Rename hoàn tất -> s3://%s/%s", self._bucket, final_key)
        return final_key


# ============================================================
# 5. Factory — lắp ráp core từ biến môi trường (.env)
# ============================================================

def build_repo_from_env() -> PsycopgControlPlaneRepo:
    """DSN ưu tiên POSTGRES_DW_DSN (bên trong container Airflow); fallback
    POSTGRES_DW_DSN_LOCAL khi chạy ngoài Docker (VD chạy script tay trên host)."""
    dsn = os.environ.get("POSTGRES_DW_DSN") or os.environ.get("POSTGRES_DW_DSN_LOCAL")
    if not dsn:
        raise RuntimeError(
            "Thiếu POSTGRES_DW_DSN / POSTGRES_DW_DSN_LOCAL trong biến môi trường."
        )
    return PsycopgControlPlaneRepo(dsn)


def build_buffer_from_env() -> S3ParquetBufferWriter:
    bucket = os.environ.get("S3_BRONZE_BUCKET")
    if not bucket:
        raise RuntimeError("Thiếu S3_BRONZE_BUCKET trong biến môi trường.")
    return S3ParquetBufferWriter(bucket=bucket)


def build_proxy_pool_from_env(auto_refill: bool = True) -> ProxyPool:
    """Tạo ProxyPool thật (proxy_manager.py). `auto_refill=True` (mặc định)
    -> gọi refill() ngay để pool có proxy sẵn sàng khi run() bắt đầu (tốn
    vài giây do health-check song song — chấp nhận được vì chỉ chạy 1 lần
    đầu mỗi run, tần suất hourly)."""
    pool = ProxyPool()
    if auto_refill:
        pool.refill()
    return pool


# ============================================================
# 6. Wiring — điểm gọi DUY NHẤT cho PythonOperator (dags/crawl_alonhadat_web.py)
# ============================================================

# stop_reason thuộc nhóm này = "hoàn thành bình thường" (task Airflow SUCCESS).
# Ngoài nhóm này (fetch_error/blocked/proxy_exhausted) = bất thường -> raise
# để Airflow đánh dấu task FAILED, kích hoạt retry theo default_args, và mở
# đường cho email alert bonus sau này (xem mục 9 tài liệu thiết kế).
NORMAL_STOP_REASONS = frozenset({
    StopReason.MAX_PAGES,
    StopReason.TIME_BOX,
    StopReason.NO_MORE_DATA,
})


def run_dag2(run_id: Optional[str] = None) -> str:
    """Điểm gọi DUY NHẤT cho PythonOperator của DAG 2. Lắp ráp toàn bộ
    factory (repo/proxy_pool/buffer/fetcher/clock) thành 1 BronzeCrawlerCore,
    chạy 1 lần, LUÔN đóng kết nối Postgres (finally) dù thành công hay lỗi.

    `run_id`: nếu không truyền, tự sinh theo timestamp HCMC. Khi gọi từ
    Airflow nên truyền `{{ run_id }}` (run_id của chính DAG run) qua
    `op_kwargs` để dễ truy vết `crawl.run_state` <-> Airflow UI.

    Raise RuntimeError nếu stop_reason KHÔNG thuộc NORMAL_STOP_REASONS —
    để Airflow coi task là FAILED (xem NORMAL_STOP_REASONS ở trên)."""
    if not run_id:
        run_id = f"web-{datetime.now(HCM_TZ):%Y%m%dT%H%M%S}"

    repo = build_repo_from_env()
    try:
        core = BronzeCrawlerCore(
            repo=repo,
            proxy_pool=build_proxy_pool_from_env(),
            fetcher=RequestsPageFetcher(),
            buffer=build_buffer_from_env(),
            clock=SystemClock(),
        )
        stop_reason = core.run(run_id)
    finally:
        repo.close()

    logger.info("DAG2 run_id=%s kết thúc với stop_reason=%s", run_id, stop_reason.value)

    if stop_reason not in NORMAL_STOP_REASONS:
        raise RuntimeError(
            f"DAG2 run_id={run_id} dừng bất thường: stop_reason={stop_reason.value}. "
            f"Chi tiết: bảng crawl.run_state hoặc log task phía trên."
        )

    return stop_reason.value
