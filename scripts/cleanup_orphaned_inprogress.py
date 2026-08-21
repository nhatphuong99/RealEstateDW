"""
scripts/cleanup_orphaned_inprogress.py

Dọn THỦ CÔNG các file `.parquet.inprogress` mồ côi trên S3. Dùng chung
logic quét S3 với `S3ParquetBufferWriter` (bronze_crawler_io.py) — không
trùng lặp code. Từ 2026-08-19, `run_dag2()` đã TỰ ĐỘNG gọi cơ chế này ở
đầu mỗi run, nên script này giờ chỉ cần dùng khi muốn dọn NGAY (không đợi
tới run kế tiếp) hoặc kiểm tra thủ công.

AN TOÀN TUYỆT ĐỐI: chỉ xoá 1 `.inprogress` key khi đã có key final (không
đuôi `.inprogress`) tương ứng — KHÔNG BAO GIỜ đụng tới `.inprogress` chưa
có bản final (có thể là run đang dở dang thật).

Dry-run mặc định (chỉ liệt kê, KHÔNG xoá) — thêm --apply để xoá thật:
    python scripts/cleanup_orphaned_inprogress.py                # xem trước
    python scripts/cleanup_orphaned_inprogress.py --apply         # xoá thật
"""

from __future__ import annotations

import argparse

from crawler.bronze_crawler_io import build_buffer_from_env


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Xoá thật (mặc định chỉ liệt kê)")
    parser.add_argument("--prefix", default="bronze/")
    args = parser.parse_args()

    buffer = build_buffer_from_env()
    orphaned = buffer.list_orphaned_inprogress(prefix=args.prefix)

    if not orphaned:
        print("Không tìm thấy .inprogress mồ côi nào.")
        return

    print(f"Tìm thấy {len(orphaned)} file .inprogress mồ côi (đã có bản final tương ứng):")
    for key in orphaned:
        print(f"  - {key}")

    if not args.apply:
        print("\n(Chạy lại kèm --apply để xoá thật)")
        return

    n = buffer.cleanup_orphaned_inprogress(prefix=args.prefix)
    print(f"\nHoàn tất — đã xoá {n} file.")


if __name__ == "__main__":
    main()
