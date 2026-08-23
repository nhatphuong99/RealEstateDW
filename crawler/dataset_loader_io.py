"""
crawler/dataset_loader_io.py

Implement THẬT các Protocol khai báo trong dataset_loader_core.py:
    - RequestsPartFetcher -> GET CDN part1..part77.parquet (requests)
    - S3PartUploader      -> put_object lên S3 (boto3)
    - PsycopgPartStateStore -> crawl.dataset_part_state (psycopg2)
    - compute_parts_to_process_task() / process_one_part_task() -> 2 điểm
      gọi DUY NHẤT mà dags/bronze_load_dataset.py dùng (task 1 / task 2 mapped)

Core (dataset_loader_core.py) hoàn toàn không biết đến module này — mọi
import psycopg2/boto3/requests CHỈ nằm ở đây, đúng nguyên tắc tách biệt
logic thuần khỏi I/O thật (giống web_crawler_io.py bên Nhóm B).

Khác với Nhóm B: KHÔNG tự viết retry loop ở đây — lỗi ở bất kỳ bước nào
(probe/download/upload) đều ghi nhận vào DB rồi `raise` ngay, để Airflow
tự retry đúng Task Instance đó (quyết định D1).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import boto3
import psycopg2
import psycopg2.extras
import requests

from crawler.dataset_loader_core import (
    DownloadResult,
    PartOutcome,
    PartState,
    ProbeResult,
    UploadResult,
    compute_parts_to_process,
    process_one_part,
)
from crawler import config

logger = logging.getLogger("dataset_loader_io")

HCM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# CDN có thể đứng sau CDN/WAF chặn User-Agent mặc định của thư viện
# requests (giống trường hợp GeoNode bên proxy_manager.py) -> set sẵn
# User-Agent giả trình duyệt cho phòng ngừa. LƯU Ý: đây KHÔNG phải
# nguyên nhân của lỗi HTTP 530 (Cloudflare Origin DNS Error - lỗi hạ
# tầng phía CDN, xảy ra SAU khi Cloudflare đã chấp nhận request).
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


# ============================================================
# 1. RequestsPartFetcher — implement Protocol PartFetcher
# ============================================================

class RequestsPartFetcher:
    """GET trực tiếp CDN, không qua proxy (khác hẳn Nhóm B) — CDN không
    chặn/rate-limit như alonhadat.com.vn."""

    def __init__(self, base_url: str, probe_timeout: float, download_timeout: float) -> None:
        self._base_url = base_url.rstrip("/")
        self._probe_timeout = probe_timeout
        self._download_timeout = download_timeout

    def _part_url(self, part_number: int) -> str:
        return f"{self._base_url}/part{part_number}.parquet"

    def probe(self, part_number: int) -> ProbeResult:
        """GET + Range: bytes=0-0 (KHÔNG dùng HEAD — CDN trả 401 sai chuẩn
        với HEAD, đã xác nhận thực tế). `stream=True` để không tải nguyên
        file chỉ để kiểm tra tồn tại."""
        url = self._part_url(part_number)
        try:
            response = requests.get(
                url,
                headers={**DEFAULT_HEADERS, "Range": "bytes=0-0"},
                timeout=self._probe_timeout,
                stream=True,
            )
        except requests.exceptions.RequestException as exc:
            return ProbeResult(exists=False, error=str(exc))

        with response:
            if response.status_code not in (200, 206):
                return ProbeResult(exists=False, error=f"HTTP {response.status_code}")
            content_length = self._parse_total_size(response.headers)
            return ProbeResult(exists=True, content_length=content_length)

    @staticmethod
    def _parse_total_size(headers) -> Optional[int]:
        """Ưu tiên đọc tổng kích thước thật từ Content-Range (VD:
        "bytes 0-0/12345" -> 12345) — Content-Length khi có Range chỉ là
        kích thước phần trả về (1 byte), không phải kích thước file."""
        content_range = headers.get("Content-Range")
        if content_range and "/" in content_range:
            total = content_range.rsplit("/", 1)[-1]
            if total.isdigit():
                return int(total)
        content_length = headers.get("Content-Length")
        return int(content_length) if content_length and content_length.isdigit() else None

    def download(self, part_number: int) -> DownloadResult:
        """GET full file 1 lần — part lớn nhất ~10.000 dòng, không cần
        streaming (quyết định D4)."""
        url = self._part_url(part_number)
        try:
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=self._download_timeout)
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            return DownloadResult(success=False, error=str(exc))
        return DownloadResult(success=True, data=response.content)


# ============================================================
# 2. S3PartUploader — implement Protocol PartUploader
# ============================================================

class S3PartUploader:
    def __init__(self, bucket: str, prefix: str, s3_client=None) -> None:
        self._bucket = bucket
        self._prefix = prefix
        self._s3 = s3_client or boto3.client("s3")

    def upload(self, part_number: int, data: bytes) -> UploadResult:
        key = f"{self._prefix}part={part_number}.parquet"
        try:
            self._s3.put_object(Bucket=self._bucket, Key=key, Body=data)
        except Exception as exc:  # noqa: BLE001 - mọi lỗi boto3 đều coi là upload fail
            return UploadResult(success=False, error=str(exc))
        return UploadResult(success=True, s3_key=key)


def list_existing_s3_keys(bucket: str, prefix: str, s3_client=None) -> set[str]:
    """Liệt kê TOÀN BỘ key thật đang có trên S3 dưới `prefix` — nguồn sự
    thật để đối chiếu với `crawl.dataset_part_state` (A3 reconcile)."""
    s3 = s3_client or boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    keys: set[str] = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.add(obj["Key"])
    return keys


# ============================================================
# 3. PsycopgPartStateStore — implement Protocol PartStateStore
# ============================================================

class PsycopgPartStateStore:
    """Thao tác bảng crawl.dataset_part_state (DDL: sql/002_dataset_part_state.sql).
    `autocommit=True` — mỗi method là 1 statement độc lập, giống
    PsycopgControlPlaneRepo bên Nhóm B."""

    def __init__(self, dsn: str) -> None:
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = True

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "PsycopgPartStateStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def list_states(self) -> list[PartState]:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT part_number, status, s3_key, probed_at, downloaded_at, last_error
                FROM crawl.dataset_part_state
                ORDER BY part_number
                """
            )
            rows = cur.fetchall()
        return [PartState(**row) for row in rows]

    def mark_done(self, part_number: int, s3_key: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE crawl.dataset_part_state
                SET status = 'done', s3_key = %s, probed_at = now(),
                    downloaded_at = now(), last_error = NULL, updated_at = now()
                WHERE part_number = %s
                """,
                (s3_key, part_number),
            )

    def mark_failed(self, part_number: int, error: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE crawl.dataset_part_state
                SET status = 'failed', probed_at = now(), last_error = %s, updated_at = now()
                WHERE part_number = %s
                """,
                (error, part_number),
            )


# ============================================================
# 4. Factory — đọc cấu hình qua crawler/config.py
# ============================================================

def build_part_fetcher_from_env() -> RequestsPartFetcher:
    return RequestsPartFetcher(
        base_url=config.DATASET_CDN_BASE_URL,
        probe_timeout=config.DATASET_PROBE_TIMEOUT_SECONDS,
        download_timeout=config.DATASET_DOWNLOAD_TIMEOUT_SECONDS,
    )


def build_part_uploader_from_env() -> S3PartUploader:
    return S3PartUploader(bucket=config.get_s3_bucket(), prefix=config.DATASET_S3_PREFIX)


def build_state_store_from_env() -> PsycopgPartStateStore:
    return PsycopgPartStateStore(config.get_postgres_dsn())


# ============================================================
# 5. Wiring — 2 điểm gọi DUY NHẤT cho dags/bronze_load_dataset.py
# ============================================================

def compute_parts_to_process_task() -> list[int]:
    """Điểm gọi cho Task 1 (không mapped) — trả về danh sách part_number
    cần xử lý, dùng trực tiếp làm input cho `.expand()` của Task 2."""
    with build_state_store_from_env() as store:
        states = store.list_states()
    existing_keys = list_existing_s3_keys(config.get_s3_bucket(), config.DATASET_S3_PREFIX)
    parts = compute_parts_to_process(states, existing_keys)
    logger.info(
        "compute_parts_to_process_task: %d/%d part cần xử lý -> %s",
        len(parts), len(states), parts,
    )
    return parts


def process_one_part_task(part_number: int) -> None:
    """Điểm gọi cho Task 2 (mapped, 1 Task Instance / part). Lỗi ở bất kỳ
    bước nào -> ghi crawl.dataset_part_state rồi `raise` ngay để Airflow
    tự retry đúng Task Instance này (KHÔNG tự viết retry loop — quyết định D1)."""
    outcome: PartOutcome = process_one_part(
        part_number,
        fetcher=build_part_fetcher_from_env(),
        uploader=build_part_uploader_from_env(),
    )

    with build_state_store_from_env() as store:
        if outcome.success:
            store.mark_done(part_number, outcome.s3_key)
        else:
            store.mark_failed(part_number, outcome.error or "Lỗi không rõ nguyên nhân")

    if not outcome.success:
        raise RuntimeError(f"Xử lý part {part_number} thất bại: {outcome.error}")

    logger.info("Part %d xử lý xong -> %s", part_number, outcome.s3_key)
