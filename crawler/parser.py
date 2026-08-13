"""
Logic chính cho DAG "Parse" (DAG 2 trong Note.md).

Đọc HTML [URL-DS] đã lưu ở Bronze (S3), extract từng article.property-item
theo đúng bảng field đã xác nhận qua thực nghiệm trong
alonhadat_data_source_analysis.md (mục 5.1) — CHỈ lấy field từ trang danh
sách, không truy cập trang chi tiết.

URL normalization: dùng urllib.parse.urljoin(page_url, href) — GIỐNG hệt
cách Scrapy tự làm với response.urljoin(), để tránh lặp lại bug trước đây
(duplicate PK trong staging.listings_raw do 1 nơi lưu href tương đối, 1
nơi build URL tuyệt đối bằng string concat từ root domain thay vì từ
đúng page_url).
"""
import logging
import re
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from crawler import config, db, storage

logger = logging.getLogger(__name__)


def _text_or_none(el) -> Optional[str]:
    return el.get_text(strip=True) if el else None


def _parse_int(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    match = re.search(r"\d+", text)
    return int(match.group()) if match else None


def _parse_float(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    normalized = text.replace(",", ".")
    match = re.search(r"[\d.]+", normalized)
    return float(match.group()) if match else None


def _extract_listing(article, source_page_url: str, category: str) -> Optional[dict]:
    link_el = article.select_one("a[itemprop='url']")
    if not link_el or not link_el.get("href"):
        return None  # article dị dạng, bỏ qua thay vì crash cả batch

    # Chuẩn hoá URL-CT tuyệt đối TỪ ĐÚNG page_url (không phải root domain)
    url_ct = urljoin(source_page_url, link_el["href"])

    date_posted = None
    time_el = article.select_one("time[itemprop='datePosted']")
    if time_el and time_el.get("datetime"):
        try:
            date_posted = datetime.strptime(time_el["datetime"], "%Y-%m-%d").date()
        except ValueError:
            date_posted = None

    price_el = article.select_one("span.price [itemprop='price']")
    price_vnd = _parse_int(price_el.get("content")) if price_el else None

    area_el = article.select_one("span.area [itemprop='value']")
    area_m2 = _parse_float(_text_or_none(area_el))

    size_text = _text_or_none(article.select_one("span.size span"))
    try:
        size = size_text.split('x')
        width = _parse_float(size[0])
        length = _parse_float(size[1])
    except AttributeError:
        width = None
        length = None

    street_width_el = article.select_one("span.street-width")
    street_width = _parse_float(_text_or_none(street_width_el))

    bedrooms_el = article.select_one("span.bedroom [itemprop='value']")
    bedrooms = _parse_int(_text_or_none(bedrooms_el))

    floors_el = article.select_one("span.floors")
    floors = _parse_int(_text_or_none(floors_el))

    return {
        "url": url_ct,
        "source_page_url": source_page_url,
        "property_type": config.PROPERTY_TYPE_LABELS.get(category, category),
        "date_posted": date_posted,
        "title": _text_or_none(article.select_one("h3.property-title")),
        "price_vnd": price_vnd,
        "area_m2": area_m2,
        "size_text": size_text,
        "width_m": width,
        "length_m": length, 
        "street_width": street_width,
        "bedrooms": bedrooms,
        "floors": floors,
        "has_parking": article.select_one("span.parking") is not None,
        "street": _text_or_none(article.select_one("[itemprop='streetAddress']")),
        "ward_new": _text_or_none(article.select_one("[itemprop='addressLocality']")),
        "province": _text_or_none(article.select_one("[itemprop='addressRegion']")),
        "old_address_full": _text_or_none(article.select_one("p.old-address span")),
        "has_warning": article.select_one("div.warning") is not None,
    }


_UPSERT_SQL = """
INSERT INTO staging.listings_raw (
    url, source_page_url, property_type, date_posted, title, price_vnd,
    area_m2, size_text, width_m, length_m, street_width, bedrooms, floors, 
    has_parking, street, ward_new, province, old_address_full,
    has_warning, first_seen_at, last_seen_at, seen_count
) VALUES (
    %(url)s, %(source_page_url)s, %(property_type)s, %(date_posted)s, %(title)s,
    %(price_vnd)s, %(area_m2)s, %(size_text)s,  %(width_m)s,  %(length_m)s, %(street_width)s, 
    %(bedrooms)s, %(floors)s, %(has_parking)s, %(street)s, %(ward_new)s, %(province)s,
    %(old_address_full)s, %(has_warning)s, now(), now(), 1
)
ON CONFLICT (url) DO UPDATE SET
    -- refresh các trường có thể đổi qua thời gian (giá, mô tả có thể được sửa)
    price_vnd = EXCLUDED.price_vnd,
    area_m2 = EXCLUDED.area_m2,
    title = EXCLUDED.title,
    has_warning = EXCLUDED.has_warning,
    last_seen_at = now(),
    seen_count = staging.listings_raw.seen_count + 1
"""


def parse_one_page(conn, s3_key: str, source_page_url: str, category: str) -> int:
    """Parse 1 file .html.gz, upsert toàn bộ listing tìm thấy. Trả về số listing xử lý được."""
    html_bytes = storage.load_gz(s3_key)
    soup = BeautifulSoup(html_bytes, "lxml")
    articles = soup.select("article.property-item")

    count = 0
    with conn.cursor() as cur:
        for article in articles:
            record = _extract_listing(article, source_page_url, category)
            if record is None:
                continue
            cur.execute(_UPSERT_SQL, record)
            count += 1
    conn.commit()
    return count


def run_parse_batch(limit: int = 100) -> dict:
    """
    Entry point gọi từ Airflow PythonOperator (DAG Parse).
    Lấy các [URL-DS] đã crawl thành công (status='success') mà chưa có
    trong crawl.parse_progress, parse từng file, ghi nhận kết quả.
    """
    conn = db.get_conn()
    stats = {"processed": 0, "success": 0, "failed": 0, "total_listings": 0}
    try:
        with db_dict_cursor(conn) as cur:
            cur.execute(
                """
                SELECT q.id AS queue_id, q.url AS source_page_url, q.category, q.s3_key
                FROM crawl.crawl_queue q
                LEFT JOIN crawl.parse_progress p ON p.s3_key = q.s3_key
                WHERE q.status = 'success'
                  AND q.s3_key IS NOT NULL
                  AND p.s3_key IS NULL
                ORDER BY q.crawl_time ASC
                LIMIT %s
                """,
                (limit,),
            )
            pending_rows = cur.fetchall()

        logger.info("DAG Parse: %d file Bronze cần xử lý", len(pending_rows))

        for row in pending_rows:
            stats["processed"] += 1
            try:
                listing_count = parse_one_page(
                    conn, row["s3_key"], row["source_page_url"], row["category"]
                )
                _record_parse_progress(conn, row["s3_key"], row["queue_id"], "success", listing_count, None)
                stats["success"] += 1
                stats["total_listings"] += listing_count
            except Exception as e:  # noqa: BLE001 - ghi log lỗi, không để 1 file lỗi làm hỏng cả batch
                logger.exception("Lỗi parse s3_key=%s", row["s3_key"])
                _record_parse_progress(conn, row["s3_key"], row["queue_id"], "failed", None, str(e))
                stats["failed"] += 1

        logger.info("DAG Parse HOÀN TẤT: %s", stats)
        return stats
    finally:
        conn.close()


def _record_parse_progress(conn, s3_key, queue_id, status, listing_count, error_message):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO crawl.parse_progress
                (s3_key, queue_id, status, listing_count, error_message, parsed_at)
            VALUES (%s, %s, %s, %s, %s, now())
            ON CONFLICT (s3_key) DO UPDATE SET
                status = EXCLUDED.status,
                listing_count = EXCLUDED.listing_count,
                error_message = EXCLUDED.error_message,
                parsed_at = now()
            """,
            (s3_key, queue_id, status, listing_count, error_message),
        )
    conn.commit()


def db_dict_cursor(conn):
    import psycopg2.extras

    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_parse_batch()
