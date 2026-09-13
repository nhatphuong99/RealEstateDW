"""
crawler/dataset_loader_io.py

Component 1 (Dataset Loader) — real I/O for the Protocols defined in
dataset_loader_core.py: RequestsPartFetcher (CDN), S3PartUploader,
PsycopgPartStateStore. The last two functions are the only entry points
called from dags/dataset_loader.py.

No retry loop here — errors are written to the DB then raised, letting
Airflow handle the retry.
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
    TOTAL_PARTS,          
    compute_parts_to_process,
    is_fully_seeded,     
    process_one_part,
)
from crawler import config

logger = logging.getLogger("dataset_loader_io")

HCM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# The CDN/WAF may block the default requests User-Agent -> spoof a browser UA.
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


# ============================================================
# Steps 2-3: RequestsPartFetcher — probe + download
# ============================================================

class RequestsPartFetcher:
    """GET straight from the CDN, no proxy."""

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
        """GET with a `Range: bytes=0-0` header to check existence (not HEAD
        — the CDN returns a non-standard 401 for HEAD requests)."""
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
        """Prefer Content-Range (e.g. "bytes 0-0/12345" -> 12345) since
        Content-Length under a Range request only reflects the returned chunk."""
        content_range = headers.get("Content-Range")
        if content_range and "/" in content_range:
            total = content_range.rsplit("/", 1)[-1]
            if total.isdigit():
                return int(total)
        content_length = headers.get("Content-Length")
        return int(content_length) if content_length and content_length.isdigit() else None

    def download(self, part_number: int) -> DownloadResult:
        """GET the whole file in one shot — the largest part is ~10,000 rows,
        small enough to not need streaming."""
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
# Step 4: S3PartUploader
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
        except Exception as exc:  # noqa: BLE001
            return UploadResult(success=False, error=str(exc))
        return UploadResult(success=True, s3_key=key)


def list_existing_s3_keys(bucket: str, prefix: str, s3_client=None) -> set[str]:
    """List real S3 keys to reconcile against pipeline.dataset_part_state."""
    s3 = s3_client or boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    keys: set[str] = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.add(obj["Key"])
    return keys


# ============================================================
# PsycopgPartStateStore — Postgres pipeline.dataset_part_state
# ============================================================

class PsycopgPartStateStore:
    """autocommit=True — each method is an independent statement."""

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
# Factory — build from env via crawler/config.py
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
# Wiring — the 2 entry points for dags/dataset_loader.py
# ============================================================

def compute_parts_to_process_task() -> list[int]:
    """Non-mapped task — returns the list of part_numbers to process, used
    as input for process_one_part_task's .expand()."""
    with build_state_store_from_env() as store:
        states = store.list_states()

    if not is_fully_seeded(states):
        logger.warning(
            "pipeline.dataset_part_state only has %d/%d rows — does not match the expected dataset",
            len(states), TOTAL_PARTS,
        )

    existing_keys = list_existing_s3_keys(config.get_s3_bucket(), config.DATASET_S3_PREFIX)
    parts = compute_parts_to_process(states, existing_keys)
    logger.info(
        "compute_parts_to_process_task: %d/%d parts need processing -> %s",
        len(parts), len(states), parts,
    )
    return parts


def process_one_part_task(part_number: int) -> None:
    """Mapped task, one Task Instance per part. On failure -> write to DB
    then raise so Airflow retries this exact instance."""
    outcome: PartOutcome = process_one_part(
        part_number,
        fetcher=build_part_fetcher_from_env(),
        uploader=build_part_uploader_from_env(),
    )

    with build_state_store_from_env() as store:
        if outcome.success:
            store.mark_done(part_number, outcome.s3_key)
        else:
            store.mark_failed(part_number, outcome.error or "Unknown error")

    if not outcome.success:
        raise RuntimeError(f"Failed to process part {part_number}: {outcome.error}")

    logger.info("Part %d processed -> %s", part_number, outcome.s3_key)
