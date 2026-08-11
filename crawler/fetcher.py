"""
Fetcher HTTP thuần bằng requests - KHÔNG cần Selenium/Playwright cho
alonhadat.com.vn (đã xác nhận SSR trong alonhadat_data_source_analysis.md
mục 2). Nếu sau này cần crawl thủ công batdongsan.com.vn (nguồn tham
khảo, quy mô nhỏ), dùng script Playwright riêng ngoài pipeline tự động
này - không trong crawler.

Gồm 3 phần:
    1. Rate limiting - delay ngẫu nhiên MIN..MAX giây giữa 2 request
    2. Retry + exponential backoff cho lỗi tạm thời (5xx, mạng)
    3. Phát hiện chặn bot dựa trên DẤU HIỆU DOM THẬT, không dùng text
       placeholder chung chung - đây chính là lỗi false-positive đã gặp
       ở batch crawler thế hệ trước (dùng title placeholder thay vì
       marker DOM thực sự). Danh sách marker lấy từ batdongsan_data_source_analysis.md.
"""
import random
import time
from typing import Optional

import requests

from . import config

BLOCKING_MARKERS = [
    "#challenge-form",
    "cf-turnstile",
    "challenges.cloudflare.com",
]


class BlockedError(Exception):
    """Nghi bị chặn bot - cần người kiểm tra thủ công, không tự retry."""


class FetchResult:
    def __init__(self, status_code: int, html: Optional[str], elapsed: float,
                 retry_after_seconds: Optional[int] = None):
        self.status_code = status_code
        self.html = html
        self.elapsed = elapsed
        self.retry_after_seconds = retry_after_seconds


def _parse_retry_after(resp) -> Optional[int]:
    """Đọc header Retry-After nếu site có trả về (giá trị chính xác nhất,
    ưu tiên hơn backoff tự do của mình). Chỉ xử lý dạng số giây đơn
    giản, bỏ qua dạng HTTP-date để giữ code đơn giản."""
    value = resp.headers.get("Retry-After")
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _looks_blocked(html: str) -> bool:
    lowered = html.lower()
    return any(marker.lower() in lowered for marker in BLOCKING_MARKERS)


def polite_sleep() -> None:
    """Gọi giữa 2 lần fetch trong cùng 1 batch - tương đương DOWNLOAD_DELAY
    + RANDOMIZE_DOWNLOAD_DELAY của Scrapy, chỉnh theo khuyến nghị đã đo
    thực nghiệm (5-8s, thay vì 3s ban đầu đã gây 429)."""
    time.sleep(random.uniform(config.MIN_DELAY_SECONDS, config.MAX_DELAY_SECONDS))


def fetch(url: str, attempt: int = 1) -> FetchResult:
    """Fetch 1 URL. Retry NỘI BỘ (trong cùng 1 request) CHỈ dành cho lỗi
    mạng/5xx thoáng qua - KHÔNG còn áp dụng cho 429.

    Lý do tách riêng 429: nếu site đang giới hạn rate, thử lại sau vài
    chục giây TRONG CÙNG 1 LẦN GỌI gần như chắc chắn vẫn bị 429 tiếp -
    vừa tốn thêm request (làm tình hình rate-limit tệ hơn), vừa khiến 1
    batch nhiều URL có thể mất hàng chục phút nếu nhiều URL dính 429. 429
    được trả về NGAY LẬP TỨC cho caller, để queue_manager.mark_failed()
    xử lý backoff ở tầng hàng đợi (theo đơn vị PHÚT, hợp lý hơn nhiều so
    với thử lại theo đơn vị GIÂY)."""
    start = time.monotonic()
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": config.USER_AGENT},
            timeout=config.REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException:
        if attempt < 3:
            backoff = min(config.BASE_BACKOFF_SECONDS * (2 ** attempt), config.MAX_BACKOFF_SECONDS)
            time.sleep(backoff)
            return fetch(url, attempt=attempt + 1)
        raise

    elapsed = time.monotonic() - start

    if resp.status_code == 429:
        return FetchResult(
            status_code=429, html=None, elapsed=elapsed,
            retry_after_seconds=_parse_retry_after(resp),
        )

    if resp.status_code in config.RETRY_STATUS_CODES and attempt < 3:
        backoff = min(config.BASE_BACKOFF_SECONDS * (2 ** attempt), config.MAX_BACKOFF_SECONDS)
        time.sleep(backoff)
        return fetch(url, attempt=attempt + 1)

    html = resp.text if resp.status_code == 200 else None

    if html and _looks_blocked(html):
        raise BlockedError(f"Phát hiện dấu hiệu chặn bot tại {url}")

    return FetchResult(status_code=resp.status_code, html=html, elapsed=elapsed)
