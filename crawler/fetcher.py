"""
Tầng fetch HTTP thuần tuý (requests) — CHỦ ĐỘNG KHÔNG tự retry khi gặp
429 ở đây.

Bài học đã ghi trong error_log.md: trước đây 429 nằm trong danh sách mã
lỗi được retry ngay bên trong 1 lần gọi fetch(), khiến mỗi request 429
kéo theo vài lần thử lại nội bộ (tốn thời gian) TRƯỚC KHI báo lỗi ra
ngoài cho queue_manager xử lý backoff — làm cả batch chạy rất chậm và
"che mất" tín hiệu rate-limit khỏi tầng queue. Quyết định: fetcher chỉ
retry cho lỗi mạng tạm thời (timeout/connection error), KHÔNG retry 429
— trả 429 thẳng ra ngoài để queue_manager.mark_failed() xử lý backoff
đúng tầng của nó.
"""
import time
from dataclasses import dataclass
from typing import Optional

import requests

from crawler import config

# Chỉ retry nội bộ cho lỗi tạm thời KHÔNG liên quan rate-limit.
# 429 CỐ Ý không có trong danh sách này.
_TRANSIENT_RETRY_STATUS_CODES = {500, 502, 503, 504}
_TRANSIENT_MAX_RETRIES = 2
_TRANSIENT_RETRY_DELAY_SECONDS = 3


@dataclass
class FetchResult:
    status_code: Optional[int]
    content: Optional[bytes]
    error: Optional[str]

    @property
    def ok(self) -> bool:
        return self.status_code == 200 and self.content is not None


_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(
            {
                "User-Agent": config.USER_AGENT,
                "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        )
    return _session


def fetch(url: str) -> FetchResult:
    """
    Fetch 1 [URL-DS]. Trả về FetchResult; KHÔNG raise exception ra ngoài
    (mọi lỗi được bọc lại thành FetchResult.error để caller xử lý đồng nhất).
    """
    session = _get_session()

    for attempt in range(_TRANSIENT_MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=config.REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as e:
            if attempt < _TRANSIENT_MAX_RETRIES:
                time.sleep(_TRANSIENT_RETRY_DELAY_SECONDS)
                continue
            return FetchResult(status_code=None, content=None, error=str(e))

        if resp.status_code in _TRANSIENT_RETRY_STATUS_CODES and attempt < _TRANSIENT_MAX_RETRIES:
            time.sleep(_TRANSIENT_RETRY_DELAY_SECONDS)
            continue

        # 429 (và mọi status khác) trả thẳng ra ngoài, không retry ở đây.
        return FetchResult(status_code=resp.status_code, content=resp.content, error=None)

    return FetchResult(status_code=None, content=None, error="unreachable")
