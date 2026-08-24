"""
crawler/dataset_loader_core.py

Logic thuần Python cho DAG 1 (`bronze_load_dataset`) — tải 77 part cố định
từ CDN lên S3. I/O thật (HTTP/S3/DB) được inject qua Protocol.

I/O thật nằm ở dataset_loader_io.py. Nhóm A đơn giản hơn Nhóm B (không
proxy/CAPTCHA/rate-limit), chỉ có 4 chức năng A1–A4 gộp thành 4 hàm.
Retry lỗi dựa vào Airflow Task, không viết riêng. Số part cố định = 77,
không có hàm discover_new_parts.
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Protocol

# Hằng số
TOTAL_PARTS = 77

# Kiểu dữ liệu
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
    """Kết quả A1 - probe CDN."""
    exists: bool
    content_length: Optional[int] = None
    error: Optional[str] = None

@dataclass(frozen=True)
class DownloadResult:
    """Kết quả A2 - download file."""
    success: bool
    data: Optional[bytes] = None
    error: Optional[str] = None

@dataclass(frozen=True)
class UploadResult:
    """Kết quả A2 - upload S3."""
    success: bool
    s3_key: Optional[str] = None
    error: Optional[str] = None

@dataclass(frozen=True)
class PartOutcome:
    """Kết quả cuối cùng cho Airflow task."""
    part_number: int
    success: bool
    s3_key: Optional[str] = None
    error: Optional[str] = None

# Protocol (DI)
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

# Hàm nghiệp vụ
def scan_and_fill_gaps(states: list[PartState]) -> list[int]:
    """A4 - part chưa xong (pending/failed)."""
    return [s.part_number for s in states if s.status in ("pending", "failed")]

def reconcile_missing_storage_objects(
    states: list[PartState], existing_s3_keys: set[str]
) -> list[int]:
    """A3 - DB done nhưng S3 thiếu file."""
    return [
        s.part_number
        for s in states
        if s.status == "done" and (s.s3_key is None or s.s3_key not in existing_s3_keys)
    ]

def compute_parts_to_process(
    states: list[PartState], existing_s3_keys: set[str]
) -> list[int]:
    """Gộp A3 + A4, dedup, sort."""
    gaps = scan_and_fill_gaps(states)
    missing_on_s3 = reconcile_missing_storage_objects(states, existing_s3_keys)
    return sorted(set(gaps) | set(missing_on_s3))

def process_one_part(
    part_number: int, fetcher: PartFetcher, uploader: PartUploader
) -> PartOutcome:
    """A1 + A2: probe → download → upload. Không retry, không DB update."""
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
