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

VỀ CẤU HÌNH: mọi tham số (timeout, DSN, bucket, ...) đọc qua `crawler/config.py`
— KHÔNG tự đọc `os.environ` rải rác trong file này nữa (trừ `run_dag2`
đọc `run_id` là tham số hàm, không phải cấu hình môi trường).
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

from crawler.bronze_crawler_core import (
    BronzeCrawlerCore,
    BronzeRecord,
    CrawlerConfig,
    DetailTask,
    FetchResult,
    ListingTask,
    RunResult,
    StopReason,
)
from crawler import config
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

class RequestsPageFetcher:
    """Implement Protocol PageFetcher bằng `requests`. KHÔNG BAO GIỜ raise
    exception ra ngoài — mọi lỗi kỹ thuật (timeout, connect-fail, DNS...)
    được bọc vào FetchResult.error để core tự phân loại (classify_fetch_result)."""

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
            # Gộp 2 nhóm lỗi CHƯA BAO GIỜ nhận được response hợp lệ:
            #   - ConnectionError: ProxyError/SSLError/ConnectTimeout — proxy
            #     không tunnel/handshake được (xác nhận log thực tế 2026-08-19).
            #   - Timeout (bao gồm ReadTimeout) — proxy CÓ connect được nhưng
            #     treo tới hết timeout, dấu hiệu kinh điển của proxy free quá
            #     tải/băng thông kém (cũng xác nhận từ log thực tế cùng ngày:
            #     "Read timed out" lặp lại nhiều lần trên cùng 1 proxy).
            # Cả 2 đều coi là "proxy này không dùng được" -> đổi proxy NGAY
            # (B12) thay vì retry mù cùng 1 proxy nhiều khả năng vẫn tệ (B11).
            logger.warning("Proxy lỗi/treo url=%s proxy=%s: %s", url, proxy_url, exc)
            return FetchResult(status_code=None, error=str(exc), is_proxy_error=True)
        except requests.exceptions.RequestException as exc:
            # Các lỗi request khác không liên quan trực tiếp tới proxy (VD
            # TooManyRedirects, InvalidURL) -> vẫn giữ B11 (retry cùng proxy).
            logger.warning("Fetch lỗi kỹ thuật url=%s proxy=%s: %s", url, proxy_url, exc)
            return FetchResult(status_code=None, error=str(exc))

        return FetchResult(
            status_code=response.status_code,
            html=response.text,
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

    # -------- Dọn .inprogress mồ côi (quyết định 2026-08-19) --------
    #
    # Cơ chế dọn TỰ ĐỘNG (gọi ở đầu mỗi run qua
    # run_dag2()) làm lưới an toàn — phòng khi worker bị kill giữa lúc
    # copy_object() đã xong nhưng delete_object() chưa kịp chạy.

    def list_orphaned_inprogress(self, prefix: str = "bronze/") -> list[str]:
        """Liệt kê (KHÔNG xoá) mọi key `.parquet.inprogress` đã có key
        final (không đuôi `.inprogress`) tương ứng — an toàn tuyệt đối để
        xoá, KHÔNG BAO GIỜ đụng tới `.inprogress` chưa có bản final (có
        thể là run đang dở dang thật)."""
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
        """Xoá thật các `.inprogress` mồ côi tìm được qua
        `list_orphaned_inprogress()`. KHÔNG BAO GIỜ raise — đây là bước
        dọn dẹp hygiene, lỗi (VD IAM tạm thời trục trặc) chỉ log warning,
        không được phép chặn crawl chính."""
        try:
            orphaned = self.list_orphaned_inprogress(prefix)
        except Exception as exc:  # noqa: BLE001 - cố ý bắt rộng, không được chặn crawl chính
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
# 5. Factory — lắp ráp core từ biến môi trường (.env)
# ============================================================

def build_repo_from_env() -> PsycopgControlPlaneRepo:
    """DSN lấy qua `config.get_postgres_dsn()` — tự chọn đúng DSN theo
    ngữ cảnh đang chạy (trong/ngoài container Airflow), xem giải thích
    trong crawler/config.py."""
    return PsycopgControlPlaneRepo(config.get_postgres_dsn())


def build_buffer_from_env() -> S3ParquetBufferWriter:
    return S3ParquetBufferWriter(bucket=config.get_s3_bucket())


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

# stop_reason thuộc nhóm này = "hoàn thành bình thường" (task Airflow SUCCESS)
# BẤT KỂ số trang crawl được. Ngoài nhóm này (fetch_error/proxy_exhausted)
# vẫn được tính THÀNH CÔNG nếu đã crawl đủ config.CRAWLER_MIN_SUCCESS_PAGES
# trước khi dừng (quyết định 2026-08-19) — xem is_success() bên dưới.
NORMAL_STOP_REASONS = frozenset({
    StopReason.MAX_PAGES,
    StopReason.TIME_BOX,
    StopReason.NO_MORE_DATA,
})


def is_success(result: RunResult) -> bool:
    """1 run được coi là THÀNH CÔNG nếu:
      - stop_reason thuộc NORMAL_STOP_REASONS (hoàn thành bình thường), HOẶC
      - đã crawl đủ `CRAWLER_MIN_SUCCESS_PAGES` trang chi tiết trước khi
        dừng, DÙ stop_reason bất thường (fetch_error/proxy_exhausted) —
        quyết định 2026-08-19: có dữ liệu đủ dùng thì không đáng để Airflow
        retry lại từ đầu (retry chỉ tốn thêm 1 vòng refill() + crawl lại
        những gì gần như chắc chắn đã có trong queue) hoặc báo fail giả."""
    if result.stop_reason in NORMAL_STOP_REASONS:
        return True
    return result.detail_pages_done >= config.CRAWLER_MIN_SUCCESS_PAGES


def run_dag2(run_id: Optional[str] = None) -> str:
    """Điểm gọi DUY NHẤT cho PythonOperator của DAG 2. Lắp ráp toàn bộ
    factory (repo/proxy_pool/buffer/fetcher/clock) thành 1 BronzeCrawlerCore,
    dọn `.inprogress` mồ côi còn sót từ run trước (nếu có), chạy 1 lần,
    LUÔN đóng kết nối Postgres (finally) dù thành công hay lỗi.

    `run_id`: nếu không truyền, tự sinh theo timestamp HCMC. Khi gọi từ
    Airflow nên truyền `{{ run_id }}` (run_id của chính DAG run) qua
    `op_kwargs` để dễ truy vết `crawl.run_state` <-> Airflow UI.

    Raise RuntimeError nếu `is_success()` trả False — để Airflow coi task
    là FAILED (kích hoạt retry theo default_args)."""
    if not run_id:
        run_id = f"web-{datetime.now(HCM_TZ):%Y%m%dT%H%M%S}"

    repo = build_repo_from_env()
    try:
        buffer = build_buffer_from_env()
        buffer.cleanup_orphaned_inprogress()  # dọn rác từ (các) run trước, nếu có — xem mục lưu ý ở class

        core = BronzeCrawlerCore(
            repo=repo,
            proxy_pool=build_proxy_pool_from_env(),
            fetcher=RequestsPageFetcher(),
            buffer=buffer,
            clock=SystemClock(),
            config=CrawlerConfig(
                max_detail_pages_per_run=config.CRAWLER_MAX_DETAIL_PAGES_PER_RUN,
                time_box_seconds=config.CRAWLER_TIME_BOX_SECONDS,
                delay_min_seconds=config.CRAWLER_DELAY_MIN_SECONDS,
                delay_max_seconds=config.CRAWLER_DELAY_MAX_SECONDS,
                max_fetch_error_retries=config.CRAWLER_MAX_FETCH_ERROR_RETRIES,
                flush_interval_seconds=config.CRAWLER_FLUSH_INTERVAL_SECONDS,
                flush_page_threshold=config.CRAWLER_FLUSH_PAGE_THRESHOLD,
                min_success_pages=config.CRAWLER_MIN_SUCCESS_PAGES,
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
            f"{config.CRAWLER_MIN_SUCCESS_PAGES} trang tối thiểu). "
            f"Chi tiết: bảng crawl.run_state hoặc log task phía trên."
        )

    if result.stop_reason not in NORMAL_STOP_REASONS:
        logger.info(
            "DAG2 run_id=%s: stop_reason bất thường (%s) nhưng đã crawl đủ "
            "%d trang (>= %d) -> VẪN TÍNH LÀ THÀNH CÔNG.",
            run_id, result.stop_reason.value, result.detail_pages_done,
            config.CRAWLER_MIN_SUCCESS_PAGES,
        )

    return result.stop_reason.value
