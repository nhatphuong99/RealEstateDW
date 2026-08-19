"""
scripts/cleanup_orphaned_inprogress.py

Dọn 1 lần các file `.parquet.inprogress` mồ côi trên S3 — hệ quả của lỗi
thiếu quyền IAM `s3:DeleteObject` (xem error_log.md, 2026-08-19).

AN TOÀN TUYỆT ĐỐI: chỉ xoá 1 `.inprogress` key khi đã xác nhận tồn tại
key chính thức (không đuôi `.inprogress`) TƯƠNG ỨNG — tức copy_object() ở
lần chạy trước ĐÃ thành công, dữ liệu đã có bản sao đầy đủ. KHÔNG BAO GIỜ
xoá 1 `.inprogress` chưa có bản final tương ứng (có thể là run đang dở
dang thật, xoá nhầm sẽ mất dữ liệu).

Chạy 1 lần sau khi đã sửa xong quyền IAM s3:DeleteObject (nếu chưa sửa,
script sẽ báo lỗi AccessDenied giống hệt lỗi gặp trong DAG — đó là dấu
hiệu cần sửa IAM trước, không phải lỗi của script này).

Dry-run mặc định (chỉ liệt kê, KHÔNG xoá) — thêm --apply để xoá thật:
    python scripts/cleanup_orphaned_inprogress.py                # xem trước
    python scripts/cleanup_orphaned_inprogress.py --apply         # xoá thật
"""

from __future__ import annotations

import argparse
import os

import boto3


def find_orphaned_inprogress(s3_client, bucket: str, prefix: str = "bronze/") -> list[str]:
    """Liệt kê mọi key `.parquet.inprogress` mà đã có key final (không đuôi
    `.inprogress`) tương ứng tồn tại — tức an toàn để xoá."""
    paginator = s3_client.get_paginator("list_objects_v2")
    all_keys: set[str] = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            all_keys.add(obj["Key"])

    orphaned = []
    for key in sorted(all_keys):
        if not key.endswith(".parquet.inprogress"):
            continue
        final_key = key[: -len(".inprogress")]
        if final_key in all_keys:
            orphaned.append(key)
    return orphaned


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Xoá thật (mặc định chỉ liệt kê)")
    parser.add_argument("--bucket", default=os.environ.get("S3_BRONZE_BUCKET"))
    parser.add_argument("--prefix", default="bronze/")
    args = parser.parse_args()

    if not args.bucket:
        raise SystemExit("Thiếu --bucket hoặc biến môi trường S3_BRONZE_BUCKET")

    s3 = boto3.client("s3")
    orphaned = find_orphaned_inprogress(s3, args.bucket, args.prefix)

    if not orphaned:
        print("Không tìm thấy .inprogress mồ côi nào.")
        return

    print(f"Tìm thấy {len(orphaned)} file .inprogress mồ côi (đã có bản final tương ứng):")
    for key in orphaned:
        print(f"  - {key}")

    if not args.apply:
        print("\n(Chạy lại kèm --apply để xoá thật)")
        return

    for key in orphaned:
        s3.delete_object(Bucket=args.bucket, Key=key)
        print(f"Đã xoá: {key}")
    print(f"\nHoàn tất — đã xoá {len(orphaned)} file.")


if __name__ == "__main__":
    main()
