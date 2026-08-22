"""
crawler/dataset_loader_core.py

Logic THUẦN Python (không thực hiện I/O thật) cho DAG 1 (`bronze_load_dataset`)
— tải 77 part cố định (part1..part77.parquet) từ CDN lên S3. Toàn bộ tương
tác HTTP / S3 / DB được inject qua Protocol (Dependency Injection), giống
pattern đã dùng ở web_crawler_core.py — nhờ vậy có thể unit test bằng
fake/mock, không cần mạng/S3/Postgres thật.

I/O thật (HTTP/S3/DB) nằm ở module riêng: crawler/dataset_loader_io.py

Khác với Nhóm B (crawl web trực tiếp), Nhóm A KHÔNG cần state machine phức
tạp (không proxy, không CAPTCHA, không rate-limit) — chỉ có 4 chức năng
A1-A4, gộp lại còn 4 hàm nghiệp vụ thực chất trong module này. Retry khi
lỗi KHÔNG tự viết ở đây — dựa vào cơ chế `retries` sẵn có của Airflow Task
(quyết định D1, xem sơ đồ vận hành DAG 1 đã thống nhất).

Số lượng part CỐ ĐỊNH = 77 (CDN đã xác nhận, không có part 78+) -> KHÔNG
có hàm `discover_new_parts` như bản nháp thiết kế cũ.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Protocol


# ============================================================
# 1. Hằng số nghiệp vụ
# ============================================================

TOTAL_PARTS = 77


# ============================================================
# 2. Kiểu dữ liệu dùng chung
# ============================================================

@dataclass(frozen=True)
class PartState:
    """Map 1-1 với 1 dòng trong bảng crawl.dataset_part_state."""

    part_number: int
    status: str                            # pending / done / failed
    s3_key: Optional[str] = None
    probed_at: Optional[datetime] = None
    downloaded_at: Optional[datetime] = None
    last_error: Optional[str] = None


@dataclass(frozen=True)
class ProbeResult:
    """Kết quả A1 - kiểm tra part có tồn tại thật trên CDN không
    (GET + Range: bytes=0-0, KHÔNG dùng HEAD vì CDN trả 401 sai chuẩn)."""

    exists: bool
    content_length: Optional[int] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class DownloadResult:
    """Kết quả A2 phần tải - GET full file 1 lần (part lớn nhất ~10.000
    dòng, file nhỏ nên không cần streaming - quyết định D4)."""

    success: bool
    data: Optional[bytes] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class UploadResult:
    """Kết quả A2 phần upload lên S3."""

    success: bool
    s3_key: Optional[str] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class PartOutcome:
    """Kết quả cuối cùng của việc xử lý 1 part - trả về cho task Airflow
    (io layer dựa vào đây để gọi mark_done()/mark_failed())."""

    part_number: int
    success: bool
    s3_key: Optional[str] = None
    error: Optional[str] = None


# ============================================================
# 3. Protocol (Dependency Injection)
# ============================================================

class PartFetcher(Protocol):
    """Lấy dữ liệu part từ CDN. Implement thật ở dataset_loader_io.py
    (RequestsPartFetcher, dùng requests)."""

    def probe(self, part_number: int) -> ProbeResult: ...
    def download(self, part_number: int) -> DownloadResult: ...


class PartUploader(Protocol):
    """Upload part lên S3. Implement thật ở dataset_loader_io.py
    (S3PartUploader, dùng boto3)."""

    def upload(self, part_number: int, data: bytes) -> UploadResult: ...


class PartStateStore(Protocol):
    """Đọc/ghi bảng crawl.dataset_part_state. Implement thật ở
    dataset_loader_io.py (PsycopgPartStateStore, dùng psycopg2)."""

    def list_states(self) -> list[PartState]: ...
    def mark_done(self, part_number: int, s3_key: str) -> None: ...
    def mark_failed(self, part_number: int, error: str) -> None: ...


# ============================================================
# 4. Hàm nghiệp vụ
# ============================================================

def scan_and_fill_gaps(states: list[PartState]) -> list[int]:
    """A4 - Trả về các part chưa xong (pending hoặc failed lần trước),
    cần tải lại."""
    return [s.part_number for s in states if s.status in ("pending", "failed")]


def reconcile_missing_storage_objects(
    states: list[PartState], existing_s3_keys: set[str]
) -> list[int]:
    """A3 - Trả về các part DB ghi 'done' nhưng file S3 thật lại KHÔNG có
    (lệch dữ liệu, VD: bị xoá nhầm trên S3) - cần tải lại dù DB nói đã xong."""
    return [
        s.part_number
        for s in states
        if s.status == "done" and (s.s3_key is None or s.s3_key not in existing_s3_keys)
    ]


def compute_parts_to_process(
    states: list[PartState], existing_s3_keys: set[str]
) -> list[int]:
    """Gộp A3 + A4, dedup, sắp xếp tăng dần - dùng làm input trực tiếp cho
    dynamic task mapping (.expand()) của Task 2 trong DAG 1."""
    gaps = scan_and_fill_gaps(states)
    missing_on_s3 = reconcile_missing_storage_objects(states, existing_s3_keys)
    return sorted(set(gaps) | set(missing_on_s3))


def process_one_part(
    part_number: int, fetcher: PartFetcher, uploader: PartUploader
) -> PartOutcome:
    """A1 + A2 gộp lại - orchestration xử lý 1 part: probe -> download ->
    upload. Dừng ngay ở bước đầu tiên bị lỗi, KHÔNG tự retry (dựa vào
    Airflow retry - quyết định D1). KHÔNG cập nhật DB ở đây (core không
    I/O) - io layer đọc PartOutcome trả về rồi tự gọi mark_done()/
    mark_failed()."""
    probe_result = fetcher.probe(part_number)
    if not probe_result.exists:
        error = probe_result.error or f"Part {part_number} không tồn tại trên CDN"
        return PartOutcome(part_number=part_number, success=False, error=error)

    download_result = fetcher.download(part_number)
    if not download_result.success or download_result.data is None:
        error = download_result.error or f"Tải part {part_number} thất bại"
        return PartOutcome(part_number=part_number, success=False, error=error)

    upload_result = uploader.upload(part_number, download_result.data)
    if not upload_result.success or upload_result.s3_key is None:
        error = upload_result.error or f"Upload part {part_number} lên S3 thất bại"
        return PartOutcome(part_number=part_number, success=False, error=error)

    return PartOutcome(part_number=part_number, success=True, s3_key=upload_result.s3_key)
