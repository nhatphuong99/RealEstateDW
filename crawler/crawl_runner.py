"""
Entry point cho Airflow task "crawl_batch".

ĐIỂM QUAN TRỌNG so với thiết kế vòng lặp trong Note.md: hàm run_batch()
XỬ LÝ 1 BATCH CÓ GIỚI HẠN RỒI RETURN, KHÔNG chạy vòng lặp "sleep 1-3s
rồi quay lại" bên trong 1 task. Giữ 1 Airflow task chạy liên tục nhiều
giờ bằng vòng lặp sleep là anti-pattern (giữ worker slot quá lâu, dễ bị
Airflow coi là treo/timeout, khó giám sát tiến độ qua UI).

Muốn đạt ~10.000-15.000 record: Airflow scheduler gọi run_batch() NHIỀU
LẦN/NGÀY (xem dags/dag_crawl_alonhadat.py, ví dụ 8 lần/ngày × ~150 url/lần
~ 1200 url/ngày) — mỗi lần là 1 task run độc lập, tự kết thúc trong vài
phút. Đúng tinh thần "chia batch ngắn thay vì 1 lần chạy dài" đã rút ra
từ phát hiện rate-limit 429 (alonhadat_data_source_analysis.md mục 3).
"""
import logging

from bs4 import BeautifulSoup

from . import config, fetcher, pagination, queue_manager, storage_s3

logger = logging.getLogger(__name__)


def _extract_list_page_info(html: str, base_url: str) -> tuple[int, list[str]]:
    """Trả về (số lượng tin đăng trên trang, danh sách URL trang chi tiết)."""
    soup = BeautifulSoup(html, "lxml")
    items = soup.select("article.property-item")
    detail_urls = []
    for item in items:
        a_tag = item.select_one("a[itemprop='url']")
        if a_tag and a_tag.has_attr("href"):
            href = a_tag["href"]
            if href.startswith("http"):
                detail_urls.append(href)
            else:
                detail_urls.append(base_url.rstrip("/") + "/" + href.lstrip("/"))
    return len(items), detail_urls


def process_one(row: dict) -> None:
    url = row["url"]
    try:
        result = fetcher.fetch(url)
    except fetcher.BlockedError as exc:
        queue_manager.mark_blocked(row["id"], str(exc))
        logger.warning("BLOCKED: %s", exc)
        return
    except Exception as exc:  # lỗi mạng sau khi đã retry nội bộ trong fetcher.fetch
        queue_manager.mark_failed(row["id"], None, str(exc), permanent=False)
        logger.warning("Fetch lỗi %s: %s", url, exc)
        return

    if result.status_code == 404:
        queue_manager.mark_failed(row["id"], 404, "Tin đã bị gỡ / không tồn tại", permanent=True)
        return

    if result.status_code != 200 or not result.html:
        queue_manager.mark_failed(row["id"], result.status_code, "HTTP không phải 200", permanent=False)
        return

    s3_key = storage_s3.save_raw_html(url, result.html, row["category"])
    queue_manager.mark_done(row["id"], result.status_code, s3_key)

    if row["url_type"] == "list":
        item_count, detail_urls = _extract_list_page_info(result.html, config.BASE_URL)

        if item_count > 0:
            # Còn tin → đưa trang danh sách kế tiếp vào hàng đợi (giai đoạn 1)
            pagination.enqueue_next_page(row["category"], row["page_number"])

            # Đưa các trang chi tiết vừa tìm thấy vào hàng đợi (giai đoạn 2)
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


def run_batch(batch_size: int = config.CRAWL_BATCH_SIZE, target_total: int = 15000) -> dict:
    """Gọi bởi Airflow PythonOperator/TaskFlow — 1 lần gọi = 1 batch có giới hạn."""
    requeued = queue_manager.requeue_stale()
    if requeued:
        logger.info("Requeue %s URL bị treo từ lần chạy trước (nghi crash giữa chừng)", requeued)

    done_so_far = queue_manager.count_done()
    if done_so_far >= target_total:
        logger.info("Đã đạt mục tiêu %s record, không crawl thêm", target_total)
        return {"processed": 0, "done_total": done_so_far}

    batch = queue_manager.claim_batch(batch_size)
    if not batch:
        logger.info("Hàng đợi rỗng — không còn URL pending (có thể cần enqueue_category_seeds() lại)")
        return {"processed": 0, "done_total": done_so_far}

    for i, row in enumerate(batch):
        process_one(row)
        if i < len(batch) - 1:
            fetcher.polite_sleep()

    return {"processed": len(batch), "done_total": queue_manager.count_done()}
