"""
crawler/dataset_loader_core.py

Thành phần 1 (Dataset Loader) — logic thuần cho DAG 1: tải 77 part cố định
từ CDN lên S3. I/O thật inject qua Protocol, implement ở dataset_loader_io.py.
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Protocol

TOTAL_PARTS = 77


@dataclass(frozen=True)
class PartState:
    """Trạng thái 1 part trong DB."""
    part_number: int
    status: str              # pending / done / failed
    s3_key: Optional[str] = None
    probed_at: Optional[datetime] = None
    downloaded_at: Optional[datetime] = None
    last_error: Optional[str] = None

@dataclass(frozen=True)
class ProbeResult:
    """Kết quả Bước 2 - probe CDN."""
    exists: bool
    content_length: Optional[int] = None
    error: Optional[str] = None

@dataclass(frozen=True)
class DownloadResult:
    """Kết quả Bước 3 - download file."""
    success: bool
    data: Optional[bytes] = None
    error: Optional[str] = None

@dataclass(frozen=True)
class UploadResult:
    """Kết quả Bước 4 - upload S3."""
    success: bool
    s3_key: Optional[str] = None
    error: Optional[str] = None

@dataclass(frozen=True)
class PartOutcome:
    """Kết quả cuối cho Airflow task."""
    part_number: int
    success: bool
    s3_key: Optional[str] = None
    error: Optional[str] = None


class PartFetcher(Protocol):
    """Lấy dữ liệu từ CDN."""
    def probe(self, part_number: int) -> ProbeResult: ...
    def download(self, part_number: int) -> DownloadResult: ...

class PartUploader(Protocol):
    """Upload lên S3."""
    def upload(self, part_number: int, data: bytes) -> UploadResult: ...

class PartStateStore(Protocol):
    """Đọc/ghi trạng thái part trong DB."""
    def list_states(self) -> list[PartState]: ...
    def mark_done(self, part_number: int, s3_key: str) -> None: ...
    def mark_failed(self, part_number: int, error: str) -> None: ...


# ---- Bước 1: xác định part cần xử lý ----

def scan_and_fill_gaps(states: list[PartState]) -> list[int]:
    """Part chưa xong (pending/failed)."""
    return [s.part_number for s in states if s.status in ("pending", "failed")]

def is_fully_seeded(states: list[PartState]) -> bool:
    """Kiểm tra pipeline.dataset_part_state đã seed đủ TOTAL_PARTS dòng chưa."""
    return len(states) == TOTAL_PARTS

def reconcile_missing_storage_objects(
    states: list[PartState], existing_s3_keys: set[str]
) -> list[int]:
    """Part DB ghi done nhưng S3 thực tế thiếu file."""
    return [
        s.part_number
        for s in states
        if s.status == "done" and (s.s3_key is None or s.s3_key not in existing_s3_keys)
    ]

def compute_parts_to_process(
    states: list[PartState], existing_s3_keys: set[str]
) -> list[int]:
    """Gộp part chưa xong + part thiếu trên S3, dedup, sort."""
    gaps = scan_and_fill_gaps(states)
    missing_on_s3 = reconcile_missing_storage_objects(states, existing_s3_keys)
    return sorted(set(gaps) | set(missing_on_s3))


# ---- Bước 2-4: probe -> download -> upload ----

def process_one_part(
    part_number: int, fetcher: PartFetcher, uploader: PartUploader
) -> PartOutcome:
    """Không retry, không tự cập nhật DB — caller lo phần đó."""
    probe_result = fetcher.probe(part_number)
    if not probe_result.exists:
        return PartOutcome(part_number, False, error=probe_result.error or f"Part {part_number} không tồn tại")

    download_result = fetcher.download(part_number)
    if not download_result.success or download_result.data is None:
        return PartOutcome(part_number, False, error=download_result.error or f"Tải part {part_number} thất bại")

    upload_result = uploader.upload(part_number, download_result.data)
    if not upload_result.success or upload_result.s3_key is None:
        return PartOutcome(part_number, False, error=upload_result.error or f"Upload part {part_number} thất bại")

    return PartOutcome(part_number, True, s3_key=upload_result.s3_key)
