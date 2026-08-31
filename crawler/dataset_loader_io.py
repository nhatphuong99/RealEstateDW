"""
crawler/dataset_loader_io.py

Thực hiện các Protocol trong dataset_loader_core.py:
    - RequestsPartFetcher -> tải CDN part1..77.parquet
    - S3PartUploader      -> upload lên S3
    - PsycopgPartStateStore -> lưu trạng thái dataset (Postgres)
    - compute_parts_to_process_task() / process_one_part_task() -> 2 task duy nhất
      được dags/bronze_load_dataset.py gọi

Core không biết module này — mọi import psycopg2/boto3/requests chỉ nằm ở đây,
đảm bảo tách biệt logic khỏi I/O (giống web_crawler_io.py Nhóm B).

Khác Nhóm B: không viết retry loop — lỗi probe/download/upload ghi vào DB rồi raise,
để Airflow tự retry đúng Task Instance (quyết định D1).
"""


from __future__ import annotations

import logging
import time
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

# CDN/WAF có thể chặn User-Agent mặc định của requests
# (như GeoNode trong proxy_manager.py) → đặt sẵn UA giả trình duyệt.
# Lưu ý: không liên quan lỗi HTTP 530 (Cloudflare Origin DNS Error).
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
    """GET trực tiếp CDN, không qua proxy — CDN không chặn/rate-limit."""

    def __init__(
        self,
        base_url: str,
        probe_timeout: float,
        download_timeout: float,
        request_delay: float = 2.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._probe_timeout = probe_timeout
        self._download_timeout = download_timeout
        self._request_delay = max(0.0, request_delay)

    def _part_url(self, part_number: int) -> str:
        return f"{self._base_url}/part{part_number}.parquet"

    def probe(self, part_number: int) -> ProbeResult:
        """GET + Range: bytes=0-0 (không dùng HEAD — CDN trả 401 sai chuẩn).
        Dùng `stream=True` để chỉ kiểm tra tồn tại, không tải toàn bộ file."""
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
        """Ưu tiên lấy kích thước file từ **Content-Range** (ví dụ "bytes 0-0/12345" → 12345).
        **Content-Length** khi dùng Range chỉ là kích thước phần trả về, không phải tổng."""

        content_range = headers.get("Content-Range")
        if content_range and "/" in content_range:
            total = content_range.rsplit("/", 1)[-1]
            if total.isdigit():
                return int(total)
        content_length = headers.get("Content-Length")
        return int(content_length) if content_length and content_length.isdigit() else None

    def download(self, part_number: int) -> DownloadResult:
        """GET full file 1 lần — part lớn nhất ~10.000 dòng, không cần streaming."""
        if self._request_delay:
            time.sleep(self._request_delay)
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
    """Liệt kê tất cả key thực trên S3 dưới `prefix` để đối chiếu với pipeline.dataset_part_state."""
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
    """Thao tác bảng pipeline.dataset_part_state (DDL: sql/002_dataset_part_state.sql).
    `autocommit=True` — mỗi method là 1 statement độc lập, giống PsycopgControlPlaneRepo Nhóm B."""

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
                FROM pipeline.dataset_part_state
                ORDER BY part_number
                """
            )
            rows = cur.fetchall()
        return [PartState(**row) for row in rows]

    def mark_done(self, part_number: int, s3_key: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline.dataset_part_state
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
                UPDATE pipeline.dataset_part_state
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
        request_delay=config.DATASET_REQUEST_DELAY_SECONDS,
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
    bước nào -> ghi pipeline.dataset_part_state rồi `raise` ngay để Airflow
    tự retry đúng Task Instance này."""
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
