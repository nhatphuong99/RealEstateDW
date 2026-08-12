"""
Quản lý hàng đợi crawl.crawl_queue — trái tim của cơ chế crash-resume
(thay thế JOBDIR của Scrapy bằng state persist trực tiếp vào Postgres
ngay tại thời điểm claim, đúng nguyên tắc "crash-resume requires explicit
state persistence").

Toàn bộ backoff/retry ở đây nằm Ở TẦNG QUEUE, tách biệt hoàn toàn khỏi
tầng request đơn lẻ trong fetcher.py — tránh lặp lại bug "retry storm"
đã gặp: URL bị 429 nếu đưa thẳng về 'pending' không delay sẽ bị
claim_batch() (ưu tiên created_at cũ nhất) lấy lại NGAY ở batch kế tiếp,
đúng lúc site đang nhạy cảm nhất.
"""
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from crawler import config


def seed_category_start_pages(conn) -> int:
    """
    Insert [URL-DS] gốc (trang 1) của từng category nếu chưa có.
    Idempotent (ON CONFLICT DO NOTHING) — an toàn gọi mỗi lần run_batch(),
    không cần cơ chế "chỉ chạy 1 lần lúc khởi tạo" riêng.
    """
    inserted = 0
    with conn.cursor() as cur:
        for category, base_url in config.CATEGORIES.items():
            cur.execute(
                """
                INSERT INTO crawl.crawl_queue (url, category, page_num, status)
                VALUES (%s, %s, 1, 'pending')
                ON CONFLICT (url) DO NOTHING
                """,
                (base_url, category),
            )
            inserted += cur.rowcount
    conn.commit()
    return inserted


def requeue_stale(conn, stale_minutes: int = config.STALE_IN_PROGRESS_MINUTES) -> int:
    """
    Crash recovery: nếu worker/task chết giữa chừng sau khi claim_batch()
    đã đánh dấu 'in_progress' nhưng chưa kịp mark_success/mark_failed, các
    row đó sẽ bị kẹt vĩnh viễn. Hàm này đưa chúng về lại 'pending' nếu đã
    quá `stale_minutes` mà không được cập nhật.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE crawl.crawl_queue
            SET status = 'pending', updated_at = now()
            WHERE status = 'in_progress'
              AND updated_at < now() - (%s || ' minutes')::interval
            """,
            (stale_minutes,),
        )
        n = cur.rowcount
    conn.commit()
    return n


def claim_batch(conn, limit: int = config.MAX_PAGES_PER_RUN) -> List[dict]:
    """
    Lấy tối đa `limit` [URL-DS] đang 'pending' và đến hạn retry, ưu tiên
    created_at cũ nhất, dùng UPDATE...RETURNING + FOR UPDATE SKIP LOCKED
    để tránh 2 worker/run claim trùng cùng 1 row (an toàn nếu sau này chạy
    song song nhiều task).
    """
    with db_dict_cursor(conn) as cur:
        cur.execute(
            """
            WITH claimed AS (
                SELECT id
                FROM crawl.crawl_queue
                WHERE status = 'pending'
                  AND (next_retry_after IS NULL OR next_retry_after <= now())
                ORDER BY created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            )
            UPDATE crawl.crawl_queue AS q
            SET status = 'in_progress', updated_at = now()
            FROM claimed
            WHERE q.id = claimed.id
            RETURNING q.id, q.url, q.category, q.page_num, q.attempt_count, q.max_attempts
            """,
            (limit,),
        )
        rows = cur.fetchall()
    conn.commit()
    return rows


def mark_success(conn, queue_id: int, http_status: int, s3_key: str, listing_count: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE crawl.crawl_queue
            SET status = 'success',
                http_status = %s,
                s3_key = %s,
                listing_count = %s,
                crawl_time = now(),
                error_message = NULL,
                updated_at = now()
            WHERE id = %s
            """,
            (http_status, s3_key, listing_count, queue_id),
        )
    conn.commit()


def mark_failed(
    conn, queue_id: int, http_status: Optional[int], error_message: str
) -> None:
    """
    Xử lý thất bại ở tầng queue:
      - Nếu đã hết lượt retry (attempt_count >= max_attempts sau khi +1)
        -> chuyển hẳn 'failed' (fix bug "zombie rows": trước đây URL hết
        max_attempts bị kẹt vĩnh viễn ở 'pending' vì claim_batch() lọc
        theo attempt_count < max_attempts nhưng không có nơi nào chuyển
        trạng thái sang 'failed').
      - Nếu còn lượt -> quay lại 'pending' NHƯNG với next_retry_after =
        now() + backoff tăng dần theo cấp số nhân (fix bug "retry storm").
    """
    with db_dict_cursor(conn) as cur:
        cur.execute(
            "SELECT attempt_count, max_attempts FROM crawl.crawl_queue WHERE id = %s",
            (queue_id,),
        )
        row = cur.fetchone()
        new_attempt_count = row["attempt_count"] + 1

        if new_attempt_count >= row["max_attempts"]:
            cur.execute(
                """
                UPDATE crawl.crawl_queue
                SET status = 'failed',
                    http_status = %s,
                    attempt_count = %s,
                    error_message = %s,
                    updated_at = now()
                WHERE id = %s
                """,
                (http_status, new_attempt_count, error_message, queue_id),
            )
        else:
            backoff_min = config.backoff_minutes(new_attempt_count)
            cur.execute(
                """
                UPDATE crawl.crawl_queue
                SET status = 'pending',
                    http_status = %s,
                    attempt_count = %s,
                    error_message = %s,
                    next_retry_after = now() + (%s || ' minutes')::interval,
                    updated_at = now()
                WHERE id = %s
                """,
                (http_status, new_attempt_count, error_message, backoff_min, queue_id),
            )
    conn.commit()


def enqueue_next_page(conn, category: str, current_page_num: int) -> bool:
    """
    Tính [URL-DS] kế tiếp CÙNG category bằng số học `/trang-{n+1}`
    (KHÔNG parse link phân trang ">>" trên trang — đã xác nhận link đó
    có thể nhảy cóc trang không liền kề). Chỉ gọi khi trang hiện tại có
    >= 1 khối article.property-item (điều kiện dừng: trang rỗng).
    """
    base_url = config.CATEGORIES[category]
    next_page_num = current_page_num + 1
    next_url = base_url if next_page_num == 1 else f"{base_url}/trang-{next_page_num}"

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO crawl.crawl_queue (url, category, page_num, status)
            VALUES (%s, %s, %s, 'pending')
            ON CONFLICT (url) DO NOTHING
            """,
            (next_url, category, next_page_num),
        )
        inserted = cur.rowcount > 0
    conn.commit()
    return inserted


def count_success_pages(conn) -> int:
    """Tổng số [URL-DS] đã crawl thành công — dùng để theo dõi tiến độ đạt mục tiêu record."""
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM crawl.crawl_queue WHERE status = 'success'")
        (n,) = cur.fetchone()
    return n


# ---------------------------------------------------------------------
# Helper nội bộ: tránh import vòng với db.py, chỉ cần cursor kiểu dict
# ---------------------------------------------------------------------
def db_dict_cursor(conn):
    import psycopg2.extras

    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
