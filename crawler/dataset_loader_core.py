"""
crawler/dataset_loader_core.py

Component 1 (Dataset Loader) — pure logic for DAG 1: fetch 77 fixed parts
from a CDN into S3. Real I/O is injected via Protocol, implemented in
dataset_loader_io.py.
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Protocol

TOTAL_PARTS = 77


@dataclass(frozen=True)
class PartState:
    """DB state of a single part."""
    part_number: int
    status: str              # pending / done / failed
    s3_key: Optional[str] = None
    probed_at: Optional[datetime] = None
    downloaded_at: Optional[datetime] = None
    last_error: Optional[str] = None

@dataclass(frozen=True)
class ProbeResult:
    """Step 2 result — CDN probe."""
    exists: bool
    content_length: Optional[int] = None
    error: Optional[str] = None

@dataclass(frozen=True)
class DownloadResult:
    """Step 3 result — file download."""
    success: bool
    data: Optional[bytes] = None
    error: Optional[str] = None

@dataclass(frozen=True)
class UploadResult:
    """Step 4 result — S3 upload."""
    success: bool
    s3_key: Optional[str] = None
    error: Optional[str] = None

@dataclass(frozen=True)
class PartOutcome:
    """Final outcome for the Airflow task."""
    part_number: int
    success: bool
    s3_key: Optional[str] = None
    error: Optional[str] = None


class PartFetcher(Protocol):
    """Fetch data from the CDN."""
    def probe(self, part_number: int) -> ProbeResult: ...
    def download(self, part_number: int) -> DownloadResult: ...

class PartUploader(Protocol):
    """Upload to S3."""
    def upload(self, part_number: int, data: bytes) -> UploadResult: ...

class PartStateStore(Protocol):
    """Read/write part state in the DB."""
    def list_states(self) -> list[PartState]: ...
    def mark_done(self, part_number: int, s3_key: str) -> None: ...
    def mark_failed(self, part_number: int, error: str) -> None: ...


# ---- Step 1: determine which parts need processing ----

def scan_and_fill_gaps(states: list[PartState]) -> list[int]:
    """Parts not yet done (pending/failed)."""
    return [s.part_number for s in states if s.status in ("pending", "failed")]

def is_fully_seeded(states: list[PartState]) -> bool:
    """Check whether pipeline.dataset_part_state has been seeded with all TOTAL_PARTS rows."""
    return len(states) == TOTAL_PARTS

def reconcile_missing_storage_objects(
    states: list[PartState], existing_s3_keys: set[str]
) -> list[int]:
    """Parts marked 'done' in the DB but actually missing on S3."""
    return [
        s.part_number
        for s in states
        if s.status == "done" and (s.s3_key is None or s.s3_key not in existing_s3_keys)
    ]

def compute_parts_to_process(
    states: list[PartState], existing_s3_keys: set[str]
) -> list[int]:
    """Union of not-yet-done parts + parts missing on S3, deduped and sorted."""
    gaps = scan_and_fill_gaps(states)
    missing_on_s3 = reconcile_missing_storage_objects(states, existing_s3_keys)
    return sorted(set(gaps) | set(missing_on_s3))


# ---- Steps 2-4: probe -> download -> upload ----

def process_one_part(
    part_number: int, fetcher: PartFetcher, uploader: PartUploader
) -> PartOutcome:
    """No retry, does not update the DB itself — the caller handles that."""
    probe_result = fetcher.probe(part_number)
    if not probe_result.exists:
        return PartOutcome(part_number, False, error=probe_result.error or f"Part {part_number} does not exist")

    download_result = fetcher.download(part_number)
    if not download_result.success or download_result.data is None:
        return PartOutcome(part_number, False, error=download_result.error or f"Failed to download part {part_number}")

    upload_result = uploader.upload(part_number, download_result.data)
    if not upload_result.success or upload_result.s3_key is None:
        return PartOutcome(part_number, False, error=upload_result.error or f"Failed to upload part {part_number}")

    return PartOutcome(part_number, True, s3_key=upload_result.s3_key)
