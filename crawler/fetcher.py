"""
Fetcher HTTP thuần bằng requests — KHÔNG cần Selenium/Playwright cho
alonhadat.com.vn (đã xác nhận SSR trong alonhadat_data_source_analysis.md
mục 2).

Gồm 3 phần:
    1. Rate limiting — delay ngẫu nhiên MIN..MAX giây giữa 2 request
    2. Retry + exponential backoff cho lỗi tạm thời (429, 5xx, mạng)
    3. Phát hiện chặn bot dựa trên DẤU HIỆU DOM THẬT, không dùng text
       placeholder chung chung — đây chính là lỗi false-positive đã gặp
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
    """Nghi bị chặn bot — cần người kiểm tra thủ công, không tự retry."""


class FetchResult:
    def __init__(self, status_code: int, html: Optional[str], elapsed: float):
        self.status_code = status_code
        self.html = html
        self.elapsed = elapsed


def _looks_blocked(html: str) -> bool:
    lowered = html.lower()
    return any(marker.lower() in lowered for marker in BLOCKING_MARKERS)


def polite_sleep() -> None:
    """Gọi giữa 2 lần fetch trong cùng 1 batch — tương đương DOWNLOAD_DELAY
    + RANDOMIZE_DOWNLOAD_DELAY của Scrapy, chỉnh theo khuyến nghị đã đo
    được thực nghiệm (5-8s, thay vì 3s ban đầu đã gây 429)."""
    time.sleep(random.uniform(config.MIN_DELAY_SECONDS, config.MAX_DELAY_SECONDS))


def fetch(url: str, attempt: int = 1) -> FetchResult:
    """Fetch 1 URL. Đây là retry NỘI BỘ (trong cùng 1 request, cho lỗi
    mạng/429/5xx thoáng qua) — khác với retry ở tầng hàng đợi (mark_failed
    với permanent=False, sẽ cho lần chạy batch KẾ TIẾP xử lý lại)."""
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

    if resp.status_code in config.RETRY_STATUS_CODES and attempt < 3:
        backoff = min(config.BASE_BACKOFF_SECONDS * (2 ** attempt), config.MAX_BACKOFF_SECONDS)
        time.sleep(backoff)
        return fetch(url, attempt=attempt + 1)

    html = resp.text if resp.status_code == 200 else None

    if html and _looks_blocked(html):
        raise BlockedError(f"Phát hiện dấu hiệu chặn bot tại {url}")

    return FetchResult(status_code=resp.status_code, html=html, elapsed=elapsed)
