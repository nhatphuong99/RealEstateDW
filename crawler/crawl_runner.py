"""
Entry point cho Airflow task "crawl_batch".

ĐIỂM QUAN TRỌNG so với thiết kế vòng lặp trong Note.md: hàm run_batch()
XỬ LÝ 1 BATCH CÓ GIỚI HẠN RỒI RETURN, KHÔNG chạy vòng lặp "sleep 1-3s
rồi quay lại" bên trong 1 task. Giữ 1 Airflow task chạy liên tục nhiều
giờ bằng vòng lặp sleep là anti-pattern (giữ worker slot quá lâu, dễ bị
Airflow coi là treo/timeout, khó giám sát tiến độ qua UI).

Muốn đạt ~10.000-15.000 record: Airflow scheduler gọi run_batch() NHIỀU
LẦN/NGÀY (xem dags/dag_crawl_alonhadat.py, ví dụ 6 lần/ngày x ~120 url/lần
~ 720 url/ngày) - mỗi lần là 1 task run độc lập, tự kết thúc trong vài
phút. Dùng tinh thần "chia batch ngắn thay vì 1 lần chạy dài" đã rút ra
từ phát hiện rate-limit 429 (alonhadat_data_source_analysis.md mục 3).
"""
import logging
import time
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from . import config, fetcher, pagination, queue_manager, storage_s3

logger = logging.getLogger(__name__)


def _extract_list_page_info(html: str, page_url: str) -> tuple[int, list[str]]:
    """Trả về (số lượng tin đang trên trang, danh sách URL trang chi tiết
    ĐÃ CHUẨN HÓA TUYỆT ĐỐI). Dùng urljoin(page_url, href) - tương đương
    response.urljoin() của Scrapy trong alonhadat_spider_v2.py gốc, và
    dùng page_url THẬT (không phải root domain) làm base, giống chính
    xác cách Scrapy làm - quan trọng để cho ra CÙNG 1 giá trị URL như
    bên parser_runner.py (xem ghi chú trong parser_runner.parse_list_page)."""
    soup = BeautifulSoup(html, "lxml")
    items = soup.select("article.property-item")
    detail_urls = []
    for item in items:
        a_tag = item.select_one("a[itemprop='url']")
        if a_tag and a_tag.has_attr("href"):
            detail_urls.append(urljoin(page_url, a_tag["href"]))
    return len(items), detail_urls


def process_one(row: dict) -> bool:
    """Xử lý 1 URL. Trả về True nếu kết quả là 429 - dùng để run_batch()
    đếm số lần 429 LIÊN TIẾP (circuit breaker)."""
    url = row["url"]
    try:
        result = fetcher.fetch(url)
    except fetcher.BlockedError as exc:
        queue_manager.mark_blocked(row["id"], str(exc))
        logger.warning("BLOCKED: %s", exc)
        return False
    except Exception as exc:  # lỗi mạng sau khi đã retry nội bộ trong fetcher.fetch
        queue_manager.mark_failed(row["id"], None, str(exc), permanent=False)
        logger.warning("Fetch lỗi %s: %s", url, exc)
        return False

    if result.status_code == 429:
        queue_manager.mark_failed(
            row["id"], 429, "Bị rate-limit (429)", permanent=False,
            retry_after_seconds=result.retry_after_seconds,
        )
        logger.warning("429 tại %s (Retry-After header: %s giây)", url, result.retry_after_seconds)
        return True

    if result.status_code == 404:
        queue_manager.mark_failed(row["id"], 404, "Tin đã bị gỡ / không tồn tại", permanent=True)
        return False

    if result.status_code != 200 or not result.html:
        queue_manager.mark_failed(row["id"], result.status_code, "HTTP không phải 200", permanent=False)
        return False

    s3_key = storage_s3.save_raw_html(url, result.html, row["category"])
    queue_manager.mark_done(row["id"], result.status_code, s3_key)

    if row["url_type"] == "list":
        item_count, detail_urls = _extract_list_page_info(result.html, url)

        if item_count > 0:
            # Còn tin -> đưa trang danh sách kế tiếp vào hàng đợi (giai đoạn 1)
            pagination.enqueue_next_page(row["category"], row["page_number"])

            # Đưa các trang chi tiết vừa tìm thấy vào hàng đợi (giai đoạn 2,
            # tương đương response.follow(callback=parse_detail) trong
            # alonhadat_spider_v2.py, nhưng qua hàng đợi SQL thay vì Scrapy scheduler)
            detail_rows = [
                {
                    "url": u,
                    "url_type": "detail",
                    "category": row["category"],
                    "parent_url": url,
                }
                for u in detail_urls
            ]
            inserted = queue_manager.enqueue_urls(detail_rows)
            logger.info("Trang %s: %s tin, thêm %s URL chi tiết mới vào hàng đợi",
                        url, item_count, inserted)
        else:
            logger.info("Category %s đã hết trang ở trang %s", row["category"], row["page_number"])

    return False


def run_batch(batch_size: int = config.CRAWL_BATCH_SIZE, target_total: int = 15000) -> dict:
    """Gọi bởi Airflow PythonOperator/TaskFlow - 1 lần gọi = 1 batch có giới hạn.

    Có circuit breaker: nếu gặp CIRCUIT_BREAKER_CONSECUTIVE_429 lần 429
    LIÊN TIẾP, DỪNG xử lý các URL còn lại trong batch ngay lập tức - đẩy
    chúng về 'pending' với backoff dài (BATCH_COOLDOWN_MINUTES) thay vì
    tiếp tục thử (gần như chắc chắn cũng sẽ bị 429, chỉ kéo dài thời gian
    chạy và gửi thêm request vô ích vào site đang giới hạn).
    """
    requeued = queue_manager.requeue_stale()
    if requeued:
        logger.info("Requeue %s URL bị treo từ lần chạy trước (nghi crash giữa chừng)", requeued)

    done_so_far = queue_manager.count_done()
    if done_so_far >= target_total:
        logger.info("Đã đạt mục tiêu %s record, không crawl thêm", target_total)
        return {"processed": 0, "done_total": done_so_far}

    batch = queue_manager.claim_batch(batch_size)
    if not batch:
        logger.info("Hàng đợi rỗng - không còn URL pending (có thể cần enqueue_category_seeds() lại)")
        return {"processed": 0, "done_total": done_so_far}

    consecutive_429 = 0
    processed = 0
    circuit_broken = False

    for i, row in enumerate(batch):
        was_429 = process_one(row)
        processed += 1
        consecutive_429 = consecutive_429 + 1 if was_429 else 0

        if consecutive_429 >= config.CIRCUIT_BREAKER_CONSECUTIVE_429:
            remaining_ids = [r["id"] for r in batch[i + 1:]]
            if remaining_ids:
                queue_manager.cooldown_rows(remaining_ids, config.BATCH_COOLDOWN_MINUTES)
            logger.warning(
                "CIRCUIT BREAKER: %d lần 429 liên tiếp - dừng batch sớm (mới xử lý %d/%d URL), "
                "đẩy %d URL còn lại về pending với backoff %d phút",
                consecutive_429, processed, len(batch), len(remaining_ids), config.BATCH_COOLDOWN_MINUTES,
            )
            circuit_broken = True
            break

        if i < len(batch) - 1:
            if was_429:
                # Vừa bị 429 - nghỉ lâu hơn so với khoảng cách bình thường
                # trước khi thử URL TIẾP THEO trong batch
                time.sleep(config.MAX_DELAY_SECONDS * 3)
            else:
                fetcher.polite_sleep()

    return {
        "processed": processed,
        "circuit_broken": circuit_broken,
        "done_total": queue_manager.count_done(),
    }
