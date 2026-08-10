"""
Chạy thử crawler CỤC BỘ — KHÔNG cần Airflow.

Mục đích: xác nhận từng phần (kết nối DB, enqueue, claim, fetch thật,
lưu S3, parse) hoạt động đúng TRƯỚC KHI giao cho Airflow lịch chạy tự
động 6 lần/ngày — tránh debug một hệ thống tự động còn lỗi logic (dùng
nguyên tắc sắp xếp thứ tự đã thống nhất trong ke_hoach_do_an_6_tuan.md).

Cách chạy:
    1. Đảm bảo postgres-dw đang chạy và đã apply sql/schema.sql:
         docker compose up -d postgres-dw
         psql "postgresql://<user>:<pass>@localhost:5433/<db>" -f sql/schema.sql

    2. Chạy script này TỪ MÁY CÁ NHÂN (bên ngoài container), vì vậy DSN
       phải trỏ qua cổng 5433 đã expose ra host (KHÁC với DSN dùng bên
       trong container Airflow, trỏ qua tên service "postgres-dw"):

         export POSTGRES_DW_DSN="postgresql://<user>:<pass>@localhost:5433/<db>"
         export S3_BRONZE_BUCKET="ten-bucket-cua-ban"
         export AWS_ACCESS_KEY_ID="..."
         export AWS_SECRET_ACCESS_KEY="..."
         pip install -r crawler/requirements.txt
         python scripts/run_local_smoke_test.py

    3. Script CHỦ ĐỘNG giới hạn rất nhỏ (5 URL) — an toàn để chạy lại
       nhiều lần trong lúc debug, không sợ làm site bị rate-limit.
"""
import logging
import sys
from pathlib import Path

# Cho phép import package crawler khi chạy script từ thư mục scripts/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("smoke_test")

SAFE_TEST_BATCH_SIZE = 5   # CHỈ 5 URL cho lần chạy thử đầu tiên


def main():
    from crawler import config, queue_manager, pagination, crawl_runner, parser_runner

    logger.info("=== BƯỚC 1: Kiểm tra kết nối Postgres ===")
    pending_exists = queue_manager.has_pending()
    logger.info("Kết nối DB OK. Hàng đợi hiện có pending? %s", pending_exists)

    logger.info("=== BƯỚC 2: Enqueue seed (trang 1 của %d category) ===", len(config.CATEGORIES))
    # Chỉ enqueue seed khi hàng đợi hoàn toàn rỗng và chưa có dữ liệu đã crawl
    if not pending_exists and queue_manager.count_done() == 0:
        inserted = pagination.enqueue_category_seeds()
        logger.info("Đã thêm %d URL seed vào hàng đợi", inserted)
    else:
        logger.info("Hàng đợi đã có dữ liệu từ trước — bỏ qua enqueue seed")

    logger.info("=== BƯỚC 3: Crawl thử 1 batch nhỏ (%d URL) ===", SAFE_TEST_BATCH_SIZE)
    # target_total lớn để không chặn vì giới hạn tổng; chúng ta chỉ muốn chạy 1 batch nhỏ
    result = crawl_runner.run_batch(batch_size=SAFE_TEST_BATCH_SIZE, target_total=10**9)
    logger.info("Kết quả batch: %s", result)

    logger.info("=== BƯỚC 4: Parse thử các URL vừa crawl xong ===")
    parse_result = parser_runner.run(batch_limit=SAFE_TEST_BATCH_SIZE)
    logger.info("Kết quả parse: %s", parse_result)

    logger.info("=== BƯỚC 5: Kiểm tra 1 bản ghi mẫu trong staging.listings_raw ===")
    from crawler.db import get_conn, get_dict_cursor
    with get_conn() as conn:
        cur = get_dict_cursor(conn)
        cur.execute("SELECT url, payload FROM staging.listings_raw ORDER BY ingested_at DESC LIMIT 1")
        row = cur.fetchone()
        if row:
            logger.info("Mẫu dữ liệu đã parse: %s", dict(row))
        else:
            logger.warning(
                "Chưa có bản ghi nào trong staging.listings_raw - kiểm tra lại selector "
                "hoặc xem crawl_queue có URL nào status='blocked'/'failed' không."
            )

    logger.info("=== HOÀN TẤT SMOKE TEST ===")
    logger.info(
        "Nếu mọi thứ OK: tăng dần SAFE_TEST_BATCH_SIZE để test thêm, rồi mới bật DAG "
        "trong Airflow. Nếu có lỗi: xem log chi tiết ở trên, kiểm tra bảng crawl.crawl_queue "
        "(cột status, error_message) để biết chính xác URL nào thất bại và vì sao."
    )


if __name__ == "__main__":
    main()
