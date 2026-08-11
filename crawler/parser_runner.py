"""
Entry point cho Airflow task "parse_batch" - đọc HTML thô đã lưu trên S3
(status='done', parsed=FALSE), trích xuất dữ liệu, ghi vào bảng staging
Silver (staging.listings_raw).

Logic selector giữ NGUYÊN tư duy đã xác nhận trong alonhadat_spider_v2.py,
chỉ đổi từ Scrapy Selector sang BeautifulSoup vì không còn dùng Scrapy.

Lưu ý phạm vi: ở đây CHỈ parse thô từng trang (list hoặc detail) thành
JSON, chưa merge list-record + detail-record theo url thành 1 dòng hoàn
chỉnh, và chưa chuẩn hóa địa chỉ/giá/m2. Việc đó thuộc GD4 (PySpark,
theo ke_hoach_do_an_6_tuan.md) - tách riêng để giữ module này đơn giản,
dễ test, và dễ sửa lại nếu selector thay đổi (chỉ cần chạy lại DAG parse,
KHÔNG cần crawl lại - đây là lợi ích chính của việc tách fetch/parse).
"""
import json
import logging
from datetime import datetime, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from . import storage_s3
from .db import get_conn, get_dict_cursor

logger = logging.getLogger(__name__)

FIELD_MAP = {
    "Mã tin": "listing_id",
    "Loại tin": "listing_type",
    "Loại BDS": "property_type",
    "Chiều ngang": "width_m",
    "Chiều dài": "length_m",
    "Hướng": "orientation",
    "Đường trước nhà": "street_width_detail",
    "Pháp lý": "legal_status",
    "Số lầu": "floors_detail",
    "Số phòng ngủ": "bedrooms",
    "Phòng ăn": "has_dining_room",
    "Nhà bếp": "has_kitchen",
    "Sân thượng": "has_rooftop",
    "Chổ để xe hơi": "has_car_parking",
    "Chính chủ": "owner_direct",
}
BOOLEAN_FIELDS = {"has_dining_room", "has_kitchen", "has_rooftop", "has_car_parking", "owner_direct"}
EMPTY_VALUES = {"", "-", "--", "---", "_", "N/A"}


def parse_list_page(html: str, page_url: str) -> list[dict]:
    """page_url: URL của CHÍNH trang danh sách đang được parse (lấy từ
    crawl.crawl_queue.url) - dùng làm base cho urljoin() để chuẩn hóa
    href thành URL TUYỆT ĐỐI, GIỐNG HỆT cách crawl_runner._extract_list_page_info
    làm khi enqueue URL chi tiết. Trước đây hàm này lấy thẳng href thô
    (có thể là đường dẫn tương đối) - khác với URL tuyệt đối crawl_runner
    đã enqueue vào crawl_queue, khiến 1 tin đang có 2 giá trị url khác
    nhau trong staging.listings_raw (1 từ list-page, 1 từ detail-page),
    ON CONFLICT (url) không nhận ra là cùng 1 dòng nên tạo dư dữ liệu."""
    soup = BeautifulSoup(html, "lxml")
    records = []
    for item in soup.select("article.property-item"):
        price_tag = item.select_one("span.price span[itemprop='price']")
        area_tag = item.select_one("span.area span[itemprop='value']")
        url_tag = item.select_one("a[itemprop='url']")
        title_tag = item.select_one("h3.property-title")
        date_tag = item.select_one("time[itemprop='datePosted']")

        area_m2 = None
        if area_tag and area_tag.text:
            try:
                area_m2 = float(area_tag.text.strip().replace(",", "."))
            except ValueError:
                pass

        raw_href = url_tag["href"] if url_tag and url_tag.has_attr("href") else None
        absolute_url = urljoin(page_url, raw_href) if raw_href else None

        records.append({
            "url": absolute_url,
            "source": "alonhadat.com.vn",
            "date_posted": date_tag["datetime"] if date_tag and date_tag.has_attr("datetime") else None,
            "title": title_tag.get_text(strip=True) if title_tag else None,
            "price_vnd": int(price_tag["content"]) if price_tag and price_tag.has_attr("content") else None,
            "area_m2": area_m2,
        })
    return records


def parse_detail_page(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    main = soup.select_one("article.property")  # ít hơn - khác article.property-item (sidebar)
    if not main:
        return {}

    table = main.select_one("section.moreinfor1 table")
    if not table:
        return {}

    cells = table.select("td")
    result = {}
    i = 0
    while i < len(cells) - 1:
        label = cells[i].get_text(strip=True)
        value_cell = cells[i + 1]
        field_name = FIELD_MAP.get(label)
        if field_name:
            if field_name in BOOLEAN_FIELDS:
                has_check_icon = value_cell.select_one("img") is not None
                value_text = value_cell.get_text(strip=True)
                if has_check_icon:
                    result[field_name] = True
                elif value_text in EMPTY_VALUES or not value_text:
                    result[field_name] = None
                else:
                    result[field_name] = False
            else:
                value_text = value_cell.get_text(strip=True)
                result[field_name] = None if value_text in EMPTY_VALUES else value_text
        i += 2
    return result


def _fetch_unparsed_batch(limit: int) -> list[dict]:
    with get_conn() as conn:
        cur = get_dict_cursor(conn)
        cur.execute(
            """
            SELECT id, url, url_type, raw_html_s3_key
            FROM crawl.crawl_queue
            WHERE status = 'done' AND parsed = FALSE AND raw_html_s3_key IS NOT NULL
            ORDER BY fetched_at ASC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


def _mark_parsed(row_id: int) -> None:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE crawl.crawl_queue SET parsed = TRUE WHERE id = %s", (row_id,))


def _save_silver_records(records: list[dict]) -> None:
    if not records:
        return
    with get_conn() as conn:
        cur = conn.cursor()
        for r in records:
            if not r.get("url"):
                continue
            cur.execute(
                """
                INSERT INTO staging.listings_raw (url, payload, ingested_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (url) DO UPDATE
                    SET payload = staging.listings_raw.payload || EXCLUDED.payload,
                        ingested_at = EXCLUDED.ingested_at
                """,
                (r["url"], json.dumps(r, ensure_ascii=False), datetime.now(timezone.utc)),
            )


def run(batch_limit: int = 200) -> dict:
    rows = _fetch_unparsed_batch(batch_limit)
    if not rows:
        logger.info("Không có HTML nào đang chờ parse")
        return {"parsed": 0}

    parsed_count = 0
    for row in rows:
        html = storage_s3.load_raw_html(row["raw_html_s3_key"])
        if row["url_type"] == "list":
            records = parse_list_page(html, row["url"])
            _save_silver_records(records)
        else:
            detail_fields = parse_detail_page(html)
            _save_silver_records([{**detail_fields, "url": row["url"]}])
        _mark_parsed(row["id"])
        parsed_count += 1

    return {"parsed": parsed_count}
