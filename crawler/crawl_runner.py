"""
Logic chính cho DAG "Fetch" (DAG 1 trong Note.md).

Mỗi lần Airflow trigger CHỈ xử lý tối đa config.MAX_PAGES_PER_RUN [URL-DS]
rồi kết thúc task ngay (không giữ worker slot bằng vòng lặp sleep dài) —
đúng anti-pattern đã ghi nhận trong error_log/tong_hop_boi_canh: nhiều
lần chạy ngắn trong ngày thay vì 1 lần chạy dài.
"""
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import List

from bs4 import BeautifulSoup

from crawler import config, db, fetcher, queue_manager, storage

logger = logging.getLogger(__name__)


@dataclass
class RunSummary:
    run_id: str
    claimed: int = 0
    success: int = 0
    failed_429: int = 0
    failed_other: int = 0
    circuit_breaker_triggered: bool = False
    next_pages_enqueued: int = 0
    errors: List[str] = field(default_factory=list)


def _count_listings(html_bytes: bytes) -> int:
    """Đếm số khối article.property-item trong trang danh sách."""
    soup = BeautifulSoup(html_bytes, "lxml")
    return len(soup.select("article.property-item"))


def run_batch(limit: int = None) -> RunSummary:
    """
    Entry point gọi từ Airflow PythonOperator (DAG Fetch).

    Trình tự:
      1. requeue_stale() — dọn các row kẹt 'in_progress' do crash trước đó.
      2. seed_category_start_pages() — đảm bảo trang 1 mỗi category có trong queue.
      3. claim_batch() — lấy tối đa `limit` [URL-DS] đến hạn xử lý.
      4. Với mỗi URL: delay -> fetch -> lưu S3 (nếu 200) -> cập nhật queue
         -> enqueue trang kế tiếp nếu còn tin.
      5. Circuit breaker: dừng NGAY vòng lặp nếu gặp 3 lần 429 liên tiếp
         (không cố crawl tiếp trong cùng lần chạy này).
    """
    limit = limit or config.MAX_PAGES_PER_RUN
    run_id = uuid.uuid4().hex[:8]
    summary = RunSummary(run_id=run_id)

    conn = db.get_conn()
    try:
        reclaimed = queue_manager.requeue_stale(conn)
        if reclaimed:
            logger.info("requeue_stale: đưa %d row kẹt 'in_progress' về 'pending'", reclaimed)

        queue_manager.seed_category_start_pages(conn)

        batch = queue_manager.claim_batch(conn, limit=limit)
        summary.claimed = len(batch)
        logger.info("run_id=%s claimed %d URL-DS", run_id, len(batch))

        consecutive_429 = 0

        for i, row in enumerate(batch):
            if consecutive_429 >= config.CIRCUIT_BREAKER_THRESHOLD:
                summary.circuit_breaker_triggered = True
                logger.warning(
                    "Circuit breaker kích hoạt (%d lần 429 liên tiếp) — dừng run_id=%s, "
                    "còn %d URL chưa xử lý sẽ đợi lần trigger kế tiếp",
                    consecutive_429,
                    run_id,
                    len(batch) - i,
                )
                break

            if i > 0:
                time.sleep(config.random_delay_seconds())

            result = fetcher.fetch(row["url"])

            if result.status_code == 200 and result.content is not None:
                consecutive_429 = 0
                listing_count = _count_listings(result.content)
                s3_key = storage.save_gz(row["category"], row["page_num"], result.content, run_id)
                queue_manager.mark_success(conn, row["id"], 200, s3_key, listing_count)
                summary.success += 1

                if listing_count > 0:
                    enqueued = queue_manager.enqueue_next_page(conn, row["category"], row["page_num"])
                    if enqueued:
                        summary.next_pages_enqueued += 1
                else:
                    logger.info(
                        "Category=%s dừng pagination ở trang %d (0 tin)",
                        row["category"],
                        row["page_num"],
                    )

            elif result.status_code == 429:
                consecutive_429 += 1
                queue_manager.mark_failed(conn, row["id"], 429, "Rate limited (HTTP 429)")
                summary.failed_429 += 1

            else:
                error_msg = result.error or f"HTTP {result.status_code}"
                queue_manager.mark_failed(conn, row["id"], result.status_code, error_msg)
                summary.failed_other += 1
                summary.errors.append(f"{row['url']}: {error_msg}")

        total_success = queue_manager.count_success_pages(conn)
        logger.info(
            "run_id=%s HOÀN TẤT: success=%d failed_429=%d failed_other=%d "
            "circuit_breaker=%s | tổng luỹ kế success=%d",
            run_id,
            summary.success,
            summary.failed_429,
            summary.failed_other,
            summary.circuit_breaker_triggered,
            total_success,
        )
        return summary
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_batch()
