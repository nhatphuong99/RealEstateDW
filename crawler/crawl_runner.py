"""
Logic chính cho DAG "Fetch" (DAG 1 trong Note.md).

Mỗi lần Airflow trigger CHỈ xử lý tối đa config.MAX_PAGES_PER_RUN [URL-DS]
rồi kết thúc task ngay (không giữ worker slot bằng vòng lặp sleep dài).

CẬP NHẬT 2026/08/13 (sau khi phát hiện 429 dai dẳng + CAPTCHA, xem
error_log.md):
  1. Tích hợp proxy_manager.ProxyPool: khi gặp 429/CAPTCHA, đổi proxy và
     thử lại CÙNG URL đó (tối đa config.PROXY_RETRY_PER_URL lần) trước
     khi coi là thất bại thật sự.
  2. Bronze giờ CHỈ lưu HTML của các khối <article class="property-item">
     (tin đăng thật) thay vì toàn bộ trang.

CẬP NHẬT 2026/08/13 (lần 3):
  3. Proxy giờ BẮT BUỘC (config.USE_PROXY_ROTATION=True mặc định) —
     KHÔNG còn fallback fetch trực tiếp khi hết proxy. Nếu
     get_or_refresh_pool() trả None (không tìm được proxy sau
     PROXY_SCAN_MAX_ATTEMPTS lần quét), toàn bộ batch DỪNG ngay từ đầu.
     Nếu pool cạn GIỮA batch (do report_failure loại dần), batch cũng
     dừng ngay tại đó thay vì crawl trực tiếp.
  4. Thêm queue_reset.reset_stale_categories() ở đầu mỗi run_batch():
     định kỳ đưa toàn bộ [URL-DS] của 1 category về 'pending' để crawl
     lại từ trang 1, tránh bỏ sót tin mới bị chôn ở các trang đầu do
     alonhadat không sort theo thời gian đăng.
"""
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import List, Optional, Set, Tuple

from bs4 import BeautifulSoup

from crawler import config, db, fetcher, proxy_manager, queue_manager, queue_reset, storage
from crawler.fetcher import FetchResult
from crawler.proxy_manager import ProxyEntry, ProxyPool

logger = logging.getLogger(__name__)


@dataclass
class RunSummary:
    run_id: str
    claimed: int = 0
    success: int = 0
    failed_429: int = 0
    failed_captcha: int = 0
    failed_other: int = 0
    circuit_breaker_triggered: bool = False
    next_pages_enqueued: int = 0
    proxies_used: int = 0
    aborted_no_proxy: bool = False
    stopped_no_proxy_mid_run: bool = False
    queue_reset_categories: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


def _extract_listings(html_bytes: bytes) -> Tuple[bytes, int]:
    """
    Parse 1 LẦN DUY NHẤT: trích xuất riêng phần HTML của các khối
    article.property-item (tin đăng), bỏ toàn bộ phần còn lại của trang
    (header/footer/nav/script/quảng cáo). Trả về (html_bytes, so_luong).
    """
    soup = BeautifulSoup(html_bytes, "lxml")
    articles = soup.select("article.property-item")
    if not articles:
        return b"", 0
    combined_html = "\n".join(str(a) for a in articles)
    return combined_html.encode("utf-8"), len(articles)


def _fetch_with_proxy_retry(
    url: str,
    pool: Optional[ProxyPool],
    max_proxy_switches: int,
) -> Tuple[FetchResult, Optional[ProxyEntry]]:
    """
    Fetch 1 URL, tự động ĐỔI PROXY khi gặp 429/CAPTCHA (tối đa
    max_proxy_switches lần) — vì đây là tín hiệu site chặn theo IP hiện
    tại. Lỗi kỹ thuật (mạng/404/500...) KHÔNG đổi proxy thử lại, chỉ báo
    proxy lỗi kỹ thuật rồi dừng.
    """
    last_result: Optional[FetchResult] = None
    attempts = max_proxy_switches + 1 if pool else 1

    for switch in range(attempts):
        current_proxy = pool.get_next() if pool else None
        result = fetcher.fetch(url, proxy=current_proxy.url if current_proxy else None)
        last_result = result

        if result.ok:
            if current_proxy:
                pool.report_success(current_proxy)
            return result, current_proxy

        if result.status_code == 429 or result.captcha_detected:
            reason = "CAPTCHA" if result.captcha_detected else "429"
            logger.info(
                "URL bị chặn (%s) qua proxy=%s — thử lại lần %d/%d với proxy khác",
                reason, current_proxy.url if current_proxy else "direct",
                switch + 1, max_proxy_switches,
            )
            continue

        if current_proxy and pool:
            pool.report_failure(current_proxy)
        return result, None

    return last_result, None


def run_batch(limit: int = None) -> RunSummary:
    """
    Trình tự: requeue_stale -> reset_stale_categories (định kỳ) ->
    seed_category_start_pages -> get_or_refresh_pool (DỪNG nếu None) ->
    claim_batch -> (delay -> fetch (tự đổi proxy nếu 429/CAPTCHA, DỪNG
    batch nếu hết proxy giữa chừng) -> trích HTML article -> lưu S3 ->
    cập nhật queue -> enqueue trang kế tiếp) -> record_proxy_outcome ->
    circuit breaker.
    """
    limit = limit or config.MAX_PAGES_PER_RUN
    run_id = uuid.uuid4().hex[:8]
    summary = RunSummary(run_id=run_id)

    conn = db.get_conn()
    try:
        reclaimed = queue_manager.requeue_stale(conn)
        if reclaimed:
            logger.info("requeue_stale: đưa %d row kẹt 'in_progress' về 'pending'", reclaimed)

        reset_categories = queue_reset.reset_stale_categories(conn)
        if reset_categories:
            summary.queue_reset_categories = reset_categories
            logger.warning(
                "run_id=%s RESET ĐỊNH KỲ %d category về 'pending': %s",
                run_id, len(reset_categories), ", ".join(reset_categories),
            )

        queue_manager.seed_category_start_pages(conn)

        pool: Optional[ProxyPool] = None
        if config.USE_PROXY_ROTATION:
            pool = proxy_manager.get_or_refresh_pool(conn)
            if pool is None:
                summary.aborted_no_proxy = True
                logger.error(
                    "run_id=%s KHÔNG có proxy khả dụng sau %d lần quét — DỪNG batch này "
                    "(bắt buộc dùng proxy, không crawl trực tiếp theo cấu hình hiện tại).",
                    run_id, config.PROXY_SCAN_MAX_ATTEMPTS,
                )
                return summary
            summary.proxies_used = len(pool)

        batch = queue_manager.claim_batch(conn, limit=limit)
        summary.claimed = len(batch)
        logger.info(
            "run_id=%s claimed %d URL-DS | proxy pool=%d",
            run_id, len(batch), len(pool) if pool else 0,
        )

        consecutive_blocked = 0
        successful_proxy_urls: Set[str] = set()

        for i, row in enumerate(batch):
            if consecutive_blocked >= config.CIRCUIT_BREAKER_THRESHOLD:
                summary.circuit_breaker_triggered = True
                logger.warning(
                    "Circuit breaker kích hoạt (%d lần 429/CAPTCHA liên tiếp) — dừng run_id=%s, "
                    "còn %d URL chưa xử lý sẽ đợi lần trigger kế tiếp",
                    consecutive_blocked, run_id, len(batch) - i,
                )
                break

            if pool is not None and len(pool) == 0:
                summary.stopped_no_proxy_mid_run = True
                logger.error(
                    "run_id=%s HẾT proxy giữa batch (đã xử lý %d/%d URL) — dừng ngay, "
                    "không crawl trực tiếp. %d URL còn lại vẫn ở 'in_progress', sẽ được "
                    "requeue_stale() đưa lại 'pending' sau %d phút để lần trigger sau xử lý tiếp.",
                    run_id, i, len(batch), len(batch) - i, config.STALE_IN_PROGRESS_MINUTES,
                )
                break

            if i > 0:
                time.sleep(config.random_delay_seconds())

            max_switches = config.PROXY_RETRY_PER_URL if pool else 0
            result, used_proxy = _fetch_with_proxy_retry(row["url"], pool, max_switches)

            if result.ok:
                consecutive_blocked = 0
                if used_proxy:
                    successful_proxy_urls.add(used_proxy.url)

                articles_html, listing_count = _extract_listings(result.content)
                s3_key = storage.save_gz(row["category"], row["page_num"], articles_html, run_id)
                queue_manager.mark_success(conn, row["id"], 200, s3_key, listing_count)
                summary.success += 1

                if listing_count > 0:
                    enqueued = queue_manager.enqueue_next_page(conn, row["category"], row["page_num"])
                    if enqueued:
                        summary.next_pages_enqueued += 1
                else:
                    logger.info(
                        "Category=%s dừng pagination ở trang %d (0 tin)",
                        row["category"], row["page_num"],
                    )

            elif result.status_code == 429 or result.captcha_detected:
                consecutive_blocked += 1
                if result.captcha_detected:
                    queue_manager.mark_failed(conn, row["id"], result.status_code, "CAPTCHA challenge")
                    summary.failed_captcha += 1
                else:
                    queue_manager.mark_failed(conn, row["id"], 429, "Rate limited (HTTP 429)")
                    summary.failed_429 += 1

            else:
                error_msg = result.error or f"HTTP {result.status_code}"
                queue_manager.mark_failed(conn, row["id"], result.status_code, error_msg)
                summary.failed_other += 1
                summary.errors.append(f"{row['url']}: {error_msg}")

        if pool is not None:
            proxy_manager.record_proxy_outcome(conn, successful_proxy_urls)

        total_success = queue_manager.count_success_pages(conn)
        logger.info(
            "run_id=%s HOÀN TẤT: success=%d failed_429=%d failed_captcha=%d failed_other=%d "
            "circuit_breaker=%s proxy_thanh_cong=%d | tổng luỹ kế success=%d",
            run_id, summary.success, summary.failed_429, summary.failed_captcha,
            summary.failed_other, summary.circuit_breaker_triggered,
            len(successful_proxy_urls), total_success,
        )
        return summary
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_batch()
