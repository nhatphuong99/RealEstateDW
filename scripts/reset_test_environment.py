"""
scripts/reset_test_environment.py

Dọn dẹp môi trường TEST (KHÔNG dùng cho production) — dùng khi cần crawl
lại từ đầu sau khi đổi tên dag_id (crawl_alonhadat_web -> web_crawler,
bronze_load_dataset -> dataset_loader) hoặc đơn giản muốn xoá sạch dữ
liệu test cũ trước khi chạy lại.

Dọn 3 lớp, ĐỘC LẬP nhau (bật/tắt riêng qua flag):
    1. Airflow metadata  — xoá DAG cũ + lịch sử run (airflow CLI)
    2. Postgres          — reset control-plane (crawl.run_state/
                            detail_queue/listing_progress/dataset_part_state)
    3. S3                — xoá object bronze/web/ và bronze/dataset/

Mặc định DRY-RUN (chỉ in ra sẽ làm gì) — đúng convention đã dùng ở
scripts/cleanup_orphaned_inprogress.py. Phải truyền --execute mới thực sự
xoá/reset.

Cách chạy (từ máy host, có .env sẵn qua crawler/config.py):
    cd RealEstateDW/                                          # đứng ở project root
    python3 scripts/reset_test_environment.py                 # xem trước (dry-run)
    python3 scripts/reset_test_environment.py --execute        # thực thi cả 3 lớp
    python3 scripts/reset_test_environment.py --execute --skip-airflow  # chỉ SQL+S3
    python3 scripts/reset_test_environment.py --execute --skip-s3      # không đụng S3

Lưu ý: phải đứng ở PROJECT ROOT khi chạy (để import `crawler` package
thấy được) — nếu lỗi `ModuleNotFoundError: No module named 'crawler'`,
thêm `PYTHONPATH=.` phía trước lệnh.

Airflow CLI cần chạy TRONG container (không có trên host) — script tự
gọi qua `docker compose run --rm airflow-cli airflow ...` (service có
sẵn trong docker-compose.yaml, profile "debug").
"""

from __future__ import annotations

import argparse
import subprocess
import sys

import boto3
import psycopg2

from crawler import config

# dag_id CŨ (trước khi đổi tên) — mồ côi trong Airflow metadata, cần xoá
# hẳn để không còn hiển thị song song với dag_id mới trên UI.
OLD_DAG_IDS = ["crawl_alonhadat_web", "bronze_load_dataset"]

# dag_id MỚI — chỉ xoá lịch sử run (KHÔNG xoá định nghĩa DAG, vì file
# .py vẫn còn, xoá xong Airflow sẽ tự tạo lại DAG rỗng lịch sử ngay lần
# quét dag tiếp theo).
NEW_DAG_IDS = ["web_crawler", "dataset_loader"]

S3_PREFIXES_TO_CLEAR = ["bronze/web/", "bronze/dataset/"]


def reset_airflow(execute: bool, include_new: bool) -> None:
    dag_ids = OLD_DAG_IDS + (NEW_DAG_IDS if include_new else [])
    print(f"\n[Airflow] Sẽ xoá metadata + lịch sử run của: {dag_ids}")
    for dag_id in dag_ids:
        cmd = ["docker", "compose", "run", "--rm", "airflow-cli", "airflow", "dags", "delete", dag_id, "-y"]
        print(f"  $ {' '.join(cmd)}")
        if execute:
            # dag_id không tồn tại (chưa từng chạy) sẽ trả lỗi -> bỏ qua,
            # không phải lỗi thật cần dừng cả script.
            subprocess.run(cmd, check=False)


def reset_postgres(execute: bool) -> None:
    print("\n[Postgres] Sẽ reset control-plane về trạng thái sạch:")
    statements = [
        "TRUNCATE crawl.run_state;",
        "TRUNCATE crawl.detail_queue;",
        "TRUNCATE crawl.listing_progress;",
        (
            "UPDATE crawl.dataset_part_state "
            "SET status = 'pending', s3_key = NULL, probed_at = NULL, "
            "downloaded_at = NULL, last_error = NULL, updated_at = now();"
        ),
    ]
    for stmt in statements:
        print(f"  {stmt}")

    if not execute:
        return

    conn = psycopg2.connect(config.get_postgres_dsn())
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
    finally:
        conn.close()
    print("  -> Đã reset xong Postgres.")


def reset_s3(execute: bool) -> None:
    bucket = config.get_s3_bucket()
    print(f"\n[S3] Sẽ xoá toàn bộ object dưới các prefix (bucket={bucket}):")
    for prefix in S3_PREFIXES_TO_CLEAR:
        print(f"  s3://{bucket}/{prefix}*")

    if not execute:
        return

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    total_deleted = 0
    for prefix in S3_PREFIXES_TO_CLEAR:
        keys_to_delete = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys_to_delete.extend({"Key": obj["Key"]} for obj in page.get("Contents", []))

        # delete_objects giới hạn 1000 key/lần -> chia batch cho an toàn
        for i in range(0, len(keys_to_delete), 1000):
            batch = keys_to_delete[i : i + 1000]
            if batch:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
                total_deleted += len(batch)

    print(f"  -> Đã xoá {total_deleted} object trên S3.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="Thực thi thật (mặc định chỉ dry-run)")
    parser.add_argument("--skip-airflow", action="store_true", help="Bỏ qua bước dọn Airflow metadata")
    parser.add_argument("--skip-postgres", action="store_true", help="Bỏ qua bước reset Postgres")
    parser.add_argument("--skip-s3", action="store_true", help="Bỏ qua bước xoá object S3")
    parser.add_argument(
        "--include-new-dag-history",
        action="store_true",
        help="Xoá LUÔN lịch sử run của dag_id MỚI (web_crawler/dataset_loader), không chỉ dag_id cũ",
    )
    args = parser.parse_args()

    if not args.execute:
        print("=== DRY-RUN — chỉ hiển thị việc sẽ làm, KHÔNG thực thi gì cả ===")
        print("(thêm --execute để chạy thật)")

    if not args.skip_airflow:
        reset_airflow(args.execute, include_new=args.include_new_dag_history)
    if not args.skip_postgres:
        reset_postgres(args.execute)
    if not args.skip_s3:
        reset_s3(args.execute)

    if not args.execute:
        print("\n=== Dry-run xong. Chạy lại kèm --execute để thực thi thật. ===")
        sys.exit(0)

    print("\n=== Đã dọn dẹp xong toàn bộ (theo các flag đã chọn). ===")


if __name__ == "__main__":
    main()
