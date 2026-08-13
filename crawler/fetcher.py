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

CẬP NHẬT 2026/08/13 (sau khi phát hiện CAPTCHA lần đầu, xem error_log.md):
- Thêm detect_captcha(): alonhadat trả CAPTCHA kèm HTTP 200 (không phải
  429), nên KHÔNG thể chỉ dựa vào status_code để biết request có "thành
  công" hay không — phải kiểm tra cả nội dung HTML.
- fetch() nhận thêm tham số `proxy` (tuỳ chọn) để hỗ trợ proxy rotation
  (crawler/proxy_manager.py). fetcher.py CHỦ ĐỘNG KHÔNG tự quyết định
  đổi proxy khi thất bại — đó là quyết định của crawl_runner.py (tầng
  điều phối), fetcher chỉ đơn thuần dùng đúng proxy được truyền vào cho
  1 lần gọi.
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

# Chuỗi đặc trưng để nhận diện trang CAPTCHA của alonhadat.com.vn (xem
# ảnh chụp màn hình đính kèm error_log.md 2026/08/13). Chỉ cần khớp 1
# trong các chuỗi này là đủ — không cần khớp toàn bộ câu.
_CAPTCHA_MARKERS = (
    "Tôi không phải người máy",
    "bị kẻ xấu dùng phần mềm để phá hoại",
    "Viết liền, không dấu",  # label ô nhập captcha, ít khả năng trùng ngẫu nhiên
)


def detect_captcha(html_bytes: Optional[bytes]) -> bool:
    """True nếu nội dung trang là trang thử thách CAPTCHA thay vì trang
    danh sách thật. Decode bằng errors='ignore' vì chỉ cần match chuỗi
    thô, không cần parse HTML chuẩn ở bước này."""
    if not html_bytes:
        return False
    text = html_bytes.decode("utf-8", errors="ignore")
    return any(marker in text for marker in _CAPTCHA_MARKERS)


@dataclass
class FetchResult:
    status_code: Optional[int]
    content: Optional[bytes]
    error: Optional[str]
    captcha_detected: bool = False

    @property
    def ok(self) -> bool:
        """CHỈ True khi status 200, có content, VÀ không phải trang CAPTCHA.
        Trang CAPTCHA thường trả status 200 nên không thể chỉ dựa status_code."""
        return self.status_code == 200 and self.content is not None and not self.captcha_detected


# _session KHÔNG lưu proxy cố định — proxy được truyền theo từng lần gọi
# fetch() (qua tham số `proxies` của requests.Session.get), để có thể đổi
# proxy giữa các request mà không cần tạo lại session/mất connection pooling
# cho các request không dùng proxy.
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


def fetch(url: str, proxy: Optional[str] = None) -> FetchResult:
    """
    Fetch 1 [URL-DS]. Trả về FetchResult; KHÔNG raise exception ra ngoài
    (mọi lỗi được bọc lại thành FetchResult.error để caller xử lý đồng nhất).

    Tham số:
        proxy: URL proxy dạng "http://ip:port" (lấy từ
               proxy_manager.ProxyEntry.url), hoặc None để fetch trực
               tiếp không qua proxy.
    """
    session = _get_session()
    proxies = {"http": proxy, "https": proxy} if proxy else None

    for attempt in range(_TRANSIENT_MAX_RETRIES + 1):
        try:
            resp = session.get(
                url,
                timeout=config.REQUEST_TIMEOUT_SECONDS,
                proxies=proxies,
            )
        except requests.RequestException as e:
            if attempt < _TRANSIENT_MAX_RETRIES:
                time.sleep(_TRANSIENT_RETRY_DELAY_SECONDS)
                continue
            return FetchResult(status_code=None, content=None, error=str(e))

        if resp.status_code in _TRANSIENT_RETRY_STATUS_CODES and attempt < _TRANSIENT_MAX_RETRIES:
            time.sleep(_TRANSIENT_RETRY_DELAY_SECONDS)
            continue

        # 429 (và mọi status khác) trả thẳng ra ngoài, không retry ở đây.
        captcha = detect_captcha(resp.content)
        return FetchResult(
            status_code=resp.status_code,
            content=resp.content,
            error=None,
            captcha_detected=captcha,
        )

    return FetchResult(status_code=None, content=None, error="unreachable")
