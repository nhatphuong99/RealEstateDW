"""
Chạy thử crawler CỤC BỘ - KHÔNG cần Airflow.

Mục đích: xác nhận từng phần (kết nối DB, enqueue, claim, fetch thật,
lưu S3, parse) hoạt động đúng TRƯỚC KHI giao cho Airflow lịch chạy tự
động 6 lần/ngày - tránh debug 1 hệ thống tự động còn lỗi logic (dùng
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

    3. Script CHỦ ĐỘNG giới hạn rất nhỏ (5 URL/lần). QUAN TRỌNG: điều đó
       chỉ kiểm soát SỐ LƯỢNG mỗi lần chạy, KHÔNG kiểm soát KHOẢNG CÁCH
       GIỮA CÁC LẦN CHẠY - nếu bạn chạy lại script này nhiều lần liên
       tiếp (vd: mỗi 30s trong lúc debug), tổng request vẫn có thể vượt
       ngưỡng ~15-20 req/phút đã xác nhận gây 429 (xem
       alonhadat_data_source_analysis.md mục 3). Nên CHỜ ÍT NHẤT 3-5
       PHÚT giữa các lần chạy thử, đặc biệt nếu lần trước đã thấy
       status='failed' với http_status=429 trong bảng crawl.crawl_queue.
"""
import argparse
import logging
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Cho phép import package crawler (hoặc "crawler" nếu bạn đã đổi tên
# thư mục) khi chạy script từ thư mục scripts/
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("smoke_test")


def _load_env_and_pick_local_dsn():
    """Tự đọc file .env ở gốc project (không cần tự tay 'export'/'$env:'
    trên terminal - tránh lỗi cú pháp khác nhau giữa bash/PowerShell/cmd
    đã gây nhầm lẫn ở lần chạy trước).

    Ưu tiên biến POSTGRES_DW_DSN_LOCAL (trỏ localhost:5433, dùng khi
    chạy script NGOÀI Docker) nếu có, không thì mới fallback về
    POSTGRES_DW_DSN (trỏ postgres-dw:5432, dùng BÊN TRONG container
    Airflow). Nhớ vậy bạn KHÔNG cần tự sửa qua sửa lại giá trị
    POSTGRES_DW_DSN mỗi lần chuyển đổi giữa 2 bối cảnh."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        logger.warning(
            "Chưa cài python-dotenv (pip install python-dotenv) - sẽ đọc "
            "trực tiếp biến môi trường đã có sẵn trong terminal hiện tại, "
            "không tự đọc file .env."
        )
    else:
        env_path = PROJECT_ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path)
            logger.info("Đã đọc file .env tại %s", env_path)
        else:
            logger.warning(".env không tồn tại tại %s - bỏ qua", env_path)

    local_dsn = os.environ.get("POSTGRES_DW_DSN_LOCAL")
    if local_dsn:
        os.environ["POSTGRES_DW_DSN"] = local_dsn
        logger.info("Dùng POSTGRES_DW_DSN_LOCAL (localhost:5433) cho lần chạy này")
    elif "POSTGRES_DW_DSN" in os.environ:
        logger.warning(
            "Không tìm thấy POSTGRES_DW_DSN_LOCAL - đang dùng POSTGRES_DW_DSN "
            "hiện có. Nếu giá trị này trỏ tới host 'postgres-dw', kết nối SẼ "
            "THẤT BẠI vì hostname đó chỉ resolve được BÊN TRONG mạng Docker."
        )
    else:
        raise RuntimeError(
            "Không tìm thấy POSTGRES_DW_DSN hay POSTGRES_DW_DSN_LOCAL nào trong "
            "môi trường/.env. Xem file env.additions.example để biết cần khai báo gì."
        )

SAFE_TEST_BATCH_SIZE = 3   # mặc định AN TOÀN hơn ban đầu (5 -> 3) - tăng dần thủ công qua --batch-size


def print_health_check():
    """In nhanh số lượng URL theo từng status - dùng trước/sau mỗi lần
    chạy thử để biết ngay có đang bị 429/blocked dồn dập không, không
    cần tự gõ SQL mỗi lần."""
    from crawler.db import get_conn, get_dict_cursor

    with get_conn() as conn:
        cur = get_dict_cursor(conn)
        cur.execute(
            """
            SELECT
                status,
                count(*) AS so_luong,
                count(*) FILTER (WHERE http_status = 429) AS trong_do_429,
                max(updated_at) AS cap_nhat_gan_nhat
            FROM crawl.crawl_queue
            GROUP BY status
            ORDER BY status
            """
        )
        rows = cur.fetchall()

    if not rows:
        logger.info("[HEALTH CHECK] Hàng đợi đang rỗng (chưa có bản ghi nào)")
        return

    logger.info("[HEALTH CHECK] Thống kê theo status:")
    for r in rows:
        logger.info(
            "  status=%-12s so_luong=%-4d trong_do_429=%-4d cap_nhat_gan_nhat=%s",
            r["status"], r["so_luong"], r["trong_do_429"], r["cap_nhat_gan_nhat"],
        )
    total_429 = sum(r["trong_do_429"] for r in rows)
    if total_429 > 0:
        logger.warning(
            "Phát hiện %d URL từng bị 429 - nếu vừa chạy xong mà thấy còn nhiều, "
            "NÊN ĐỢI ÍT NHẤT 5-10 PHÚT trước khi chạy lại, không nên chạy liên tục.",
            total_429,
        )


def parse_args():
    parser = argparse.ArgumentParser(description="Chạy thử crawler cục bộ, quy mô nhỏ")
    parser.add_argument(
        "--batch-size", type=int, default=SAFE_TEST_BATCH_SIZE,
        help=f"Số URL tối đa crawl trong lần chạy này (mặc định {SAFE_TEST_BATCH_SIZE}, "
             f"tăng dần từ từ: 3 -> 5 -> 10 -> 20...)",
    )
    parser.add_argument(
        "--skip-parse", action="store_true",
        help="Chỉ crawl, không chạy parser_runner (dùng khi chỉ muốn test riêng phần fetch)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    _load_env_and_pick_local_dsn()

    from crawler import config, queue_manager, pagination, crawl_runner, parser_runner

    logger.info("Đang dùng DSN: %s", config.DB_DSN.split("@")[-1])  # không log user/pass

    logger.info("=== HEALTH CHECK TRƯỚC KHI CHẠY ===")
    print_health_check()

    logger.info("=== BƯỚC 1: Kiểm tra kết nối Postgres ===")
    pending_exists = queue_manager.has_pending()
    logger.info("Kết nối DB OK. Hàng đợi hiện có pending? %s", pending_exists)

    logger.info("=== BƯỚC 2: Enqueue seed (trang 1 của %d category) ===", len(config.CATEGORIES))
    if not pending_exists and queue_manager.count_done() == 0:
        inserted = pagination.enqueue_category_seeds()
        logger.info("Đã thêm %d URL seed vào hàng đợi", inserted)
    else:
        logger.info("Hàng đợi đã có dữ liệu từ trước - bỏ qua enqueue seed")

    logger.info("=== BƯỚC 3: Crawl thử 1 batch nhỏ (%d URL) ===", args.batch_size)
    result = crawl_runner.run_batch(batch_size=args.batch_size, target_total=10**9)
    logger.info("Kết quả batch: %s", result)

    if not args.skip_parse:
        logger.info("=== BƯỚC 4: Parse thử các URL vừa crawl xong ===")
        parse_result = parser_runner.run(batch_limit=args.batch_size)
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

    logger.info("=== HEALTH CHECK SAU KHI CHẠY ===")
    print_health_check()

    logger.info("=== HOÀN TẤT SMOKE TEST ===")
    logger.info(
        "Nếu mọi thứ OK (không có status='failed' với http_status=429): tăng dần "
        "--batch-size cho lần sau. Nếu thấy 429: DỪNG lại, đợi ít nhất 5-10 phút, "
        "KHÔNG chạy lại ngay."
    )


if __name__ == "__main__":
    main()
