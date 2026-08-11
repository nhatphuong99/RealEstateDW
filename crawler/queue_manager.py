"""
Quản lý hàng đợi crawl (crawl frontier) qua bảng crawl.crawl_queue.

ĐÂY LÀ PHẦN TRẢ LỜI TRỰC TIẾP CHO VẤN ĐỀ "Scrapy JOBDIR không đảm bảo
resume khi crash cùng": trạng thái được GHI XUỐNG DATABASE ngay tại thời
điểm "claim" (chuyển pending -> in_progress), không phải chỉ lưu khi
đóng spider "sạch" như JOBDIR. Nếu process bị kill cũng (container crash,
OOM, mất điện, Airflow worker bị restart giữa chừng...), các URL đang
ở trạng thái 'in_progress' sẽ được requeue_stale() đưa về 'pending' sau
khi qua STALE_IN_PROGRESS_MINUTES mà không thấy cập nhật - đảm bảo:
    - Không bao giờ MẤT URL (vẫn còn trong bảng, chỉ đợi requeue)
    - Không CRAWL LẶP (nhờ UNIQUE constraint trên cột url ở schema.sql)
"""
from datetime import datetime, timedelta, timezone
from typing import Optional

from . import config
from .db import get_conn, get_dict_cursor


def enqueue_urls(rows: list[dict]) -> int:
    """rows: list[dict] với key url, url_type, category, page_number(tùy chọn), parent_url(tùy chọn).
    Dùng ON CONFLICT DO NOTHING để INSERT idempotent - gọi lại nhiều lần
    với cùng 1 url không gây lỗi, không tạo bản ghi trùng."""
    if not rows:
        return 0
    with get_conn() as conn:
        cur = conn.cursor()
        inserted = 0
        for r in rows:
            cur.execute(
                """
                INSERT INTO crawl.crawl_queue (url, url_type, category, page_number, parent_url)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING
                """,
                (r["url"], r["url_type"], r["category"], r.get("page_number"), r.get("parent_url")),
            )
            inserted += cur.rowcount
        return inserted


def requeue_stale(timeout_minutes: int = config.STALE_IN_PROGRESS_MINUTES) -> int:
    """Gọi ở ĐẦU MỖI LẦN CHẠY batch (xem crawl_runner.run_batch). Đây là
    bước phát hiện crash: nếu 1 bản ghi bị 'claim' (in_progress) từ lâu
    mà chưa bao giờ 'done'/'failed', nghĩa là lần chạy trước đã chết
    giữa chừng (không kịp cập nhật trạng thái cuối) -> đưa về pending
    để lần này crawl lại."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=timeout_minutes)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE crawl.crawl_queue
            SET status = 'pending', claimed_at = NULL
            WHERE status = 'in_progress' AND claimed_at < %s
            """,
            (cutoff,),
        )
        return cur.rowcount


def claim_batch(batch_size: int = config.CRAWL_BATCH_SIZE) -> list[dict]:
    """Nhận 1 batch URL để fetch. UPDATE...RETURNING trong 1 câu lệnh
    duy nhất đảm bảo "claim" là thao tác ATOMIC - không có khoảng hở
    giữa "đọc thấy pending" và "đánh dấu in_progress" (khác với vòng lặp
    SELECT rồi UPDATE riêng trong Note.md, dễ bị race condition nếu sau
    này chạy nhiều worker song song). FOR UPDATE SKIP LOCKED giúp an
    toàn ngay cả khi có 2 worker chạy cùng lúc (không bắt buộc với rate-
    limit hiện tại chỉ cho 1 worker, nhưng thiết kế sẵn cho tương lai)."""
    with get_conn() as conn:
        cur = get_dict_cursor(conn)
        cur.execute(
            """
            UPDATE crawl.crawl_queue
            SET status = 'in_progress', claimed_at = now(), attempt_count = attempt_count + 1
            WHERE id IN (
                SELECT id FROM crawl.crawl_queue
                WHERE status = 'pending'
                  AND attempt_count < max_attempts
                  AND (next_retry_after IS NULL OR next_retry_after <= now())
                ORDER BY created_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, url, url_type, category, page_number, parent_url, attempt_count
            """,
            (batch_size,),
        )
        return cur.fetchall()


def mark_done(row_id: int, http_status: int, raw_html_s3_key: str) -> None:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE crawl.crawl_queue
            SET status = 'done', http_status = %s, raw_html_s3_key = %s, fetched_at = now()
            WHERE id = %s
            """,
            (http_status, raw_html_s3_key, row_id),
        )


def mark_failed(
    row_id: int,
    http_status: Optional[int],
    error_message: str,
    permanent: bool = False,
    retry_after_seconds: Optional[int] = None,
) -> None:
    """permanent=True (vd: 404 - tin đã bị gỡ) -> không retry nữa, chuyển thẳng 'failed'.

    permanent=False (vd: 429/5xx/lỗi mạng tạm thời):
        - Nếu attempt_count ĐÃ chạm max_attempts -> chuyển 'failed' (fix bug
          "zombie row" - xem lịch sử sửa đổi trước).
        - Nếu còn lượt retry -> về 'pending' kèm next_retry_after (backoff
          Ở TẦNG HÀNG ĐỢI). Backoff tính theo 2 nguồn, lấy giá trị LỚN HƠN:
            1. Công thức mũ tăng dần theo attempt_count (2, 4, 8... tối đa 60 phút)
            2. Header "Retry-After" của chính site trả về, nếu có (retry_after_seconds)
               - đây là thông tin CHÍNH XÁC NHẤT từ chính site, ưu tiên hơn
               số đoán của mình khi cả hai cùng tồn tại.
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT attempt_count, max_attempts FROM crawl.crawl_queue WHERE id = %s", (row_id,))
        row = cur.fetchone()
        attempt_count, max_attempts = row if row else (0, 5)

        if permanent or attempt_count >= max_attempts:
            cur.execute(
                """
                UPDATE crawl.crawl_queue
                SET status = 'failed', http_status = %s, error_message = %s,
                    claimed_at = NULL, next_retry_after = NULL
                WHERE id = %s
                """,
                (http_status, (error_message or "")[:2000], row_id),
            )
        else:
            backoff_minutes = min(2 ** attempt_count, 60)
            if retry_after_seconds:
                backoff_minutes = max(backoff_minutes, retry_after_seconds / 60)
            cur.execute(
                """
                UPDATE crawl.crawl_queue
                SET status = 'pending', http_status = %s, error_message = %s,
                    claimed_at = NULL, next_retry_after = now() + (%s || ' minutes')::interval
                WHERE id = %s
                """,
                (http_status, (error_message or "")[:2000], backoff_minutes, row_id),
            )


def cooldown_rows(row_ids: list, minutes: int) -> None:
    """Đẩy 1 nhóm URL về 'pending' với backoff dài, dùng bởi circuit
    breaker trong crawl_runner.run_batch() khi gặp quá nhiều 429 liên
    tiếp trong 1 batch - tránh tiếp tục thử các URL còn lại của batch
    (chắc chắn cũng sẽ bị 429, chỉ tốn thêm request vô ích)."""
    if not row_ids:
        return
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE crawl.crawl_queue
            SET status = 'pending', claimed_at = NULL,
                next_retry_after = now() + (%s || ' minutes')::interval
            WHERE id = ANY(%s)
            """,
            (minutes, row_ids),
        )


def mark_blocked(row_id: int, error_message: str) -> None:
    """Riêng cho trường hợp nghi bị chặn bot (dấu hiệu Cloudflare Turnstile...).
    KHÔNG tự động retry - cần người kiểm tra thủ công trước khi đổi trạng
    thái lại về pending, tránh lặp lại bug false-positive detection đã
    từng gặp ở batch crawler thế hệ 3."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE crawl.crawl_queue
            SET status = 'blocked', error_message = %s, claimed_at = NULL
            WHERE id = %s
            """,
            (error_message[:2000], row_id),
        )


def count_done() -> int:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM crawl.crawl_queue WHERE status = 'done'")
        return cur.fetchone()[0]


def has_pending() -> bool:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT EXISTS (SELECT 1 FROM crawl.crawl_queue WHERE status = 'pending')")
        return cur.fetchone()[0]
