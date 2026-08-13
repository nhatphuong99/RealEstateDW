# import random
# import requests

# # Link API lấy thẳng list proxy HTTP của Việt Nam (không cần login)
# # PROXY_API_URL = "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=ipport&format=text&protocol=http&country=vn"
# PROXY_API_URL = "https://fineproxy.org/vi/wp-json/fineproxy/v1/free-proxies/vn/txt"

# def fetch_vietnam_proxies():
#     """Tự động tải danh sách proxy từ API miễn phí."""
#     try:
#         response = requests.get(PROXY_API_URL, timeout=10)
#         if response.status_code == 200:
#             # Tách dòng để lấy danh sách IP:PORT
#             proxies = response.text.strip().split("\n")
#             # Loại bỏ các dòng trống hoặc bị lỗi khoảng trắng
#             return [p.strip() for p in proxies if p.strip()]
#     except Exception as e:
#         print(f"Lỗi khi tải danh sách proxy: {e}")
#     return []


# def crawl_with_rotated_proxy(target_url, proxy_list):
#     """Thực hiện crawl dữ liệu bằng cách xoay vòng proxy ngẫu nhiên."""
#     if not proxy_list:
#         print("Không có proxy nào khả dụng!")
#         return None

#     # Tạo bản sao để không làm ảnh hưởng list gốc khi xóa proxy chết
#     available_proxies = proxy_list.copy()

#     while available_proxies:
#         # Chọn ngẫu nhiên 1 proxy từ danh sách để tránh trùng lặp liên tục
#         chosen_proxy = random.choice(available_proxies)
#         proxy_config = {
#             "http": f"http://{chosen_proxy}",
#             "https": f"http://{chosen_proxy}",
#         }

#         try:
#             print(f"Đang thử crawl qua Proxy: {chosen_proxy}...")
#             # Kiểm tra và tải trang mục tiêu (Đặt timeout thấp để bỏ qua proxy chậm)
#             response = requests.get(target_url, proxies=proxy_config, timeout=5)

#             if response.status_code == 200:
#                 print(f"🎉 Crawl thành công trang bằng proxy: {chosen_proxy}")
#                 return response.text
#         except requests.RequestException:
#             # Proxy lỗi hoặc bị nghẽn mạch -> xóa khỏi danh sách hiện tại và thử IP khác
#             print(f"❌ Proxy chết hoặc quá chậm: {chosen_proxy}. Đang đổi...")
#             available_proxies.remove(chosen_proxy)

#     print("Tất cả proxy trong danh sách đều không kết nối được!")
#     return None


# # === CHẠY THỬ NGHIỆM ===
# if __name__ == "__main__":
#     # 1. Tự động lấy danh sách IP Việt Nam mới nhất từ API
#     vn_proxies = fetch_vietnam_proxies()
#     print(f"Tìm thấy {len(vn_proxies)} proxy Việt Nam từ API.")
#     print(vn_proxies)

#     # 2. Tiến hành crawl thử một trang web bất kỳ
#     TARGET = "https://crawler-test.com/titles/title_with_whitespace"
#     html_content = crawl_with_rotated_proxy(TARGET, vn_proxies)

#     if html_content:
#         print("Dữ liệu cào về thành công!")


"""
crawler/proxy_manager.py

Quản lý pool proxy free (fetch ứng viên từ nguồn công khai, health-check,
rotate, tự loại proxy chết) — dùng cho đồ án DEP305x sau khi phát hiện
alonhadat.com.vn tăng cường phòng thủ (429 dai dẳng + CAPTCHA lần đầu,
xem error_log.md ngày 2026/08/13).

QUYẾT ĐỊNH THIẾT KẾ QUAN TRỌNG:
- KHÔNG dùng list proxy tĩnh hard-code. Tỷ lệ proxy free còn sống thực tế
  tại 1 thời điểm chỉ ~5-12% (khảo sát tham khảo 2026) và chết rất nhanh
  -> một list tĩnh gần như vô dụng chỉ sau vài phút. Pool phải ĐỘNG: fetch
  ứng viên -> health-check -> dùng -> tự refresh khi cạn.
- Hàm health-check gọi tới _HEALTH_CHECK_URL (endpoint trung lập), TUYỆT
  ĐỐI KHÔNG gọi trực tiếp alonhadat.com.vn ở bước này — kiểm tra hạ tầng
  proxy không được phép tốn quota rate-limit (~15-20 req/phút) của site
  đích, quota đó chỉ dành cho request crawl dữ liệu thật.
- report_failure() CHỈ được gọi khi proxy lỗi kỹ thuật (timeout/connection
  error). KHÔNG gọi khi gặp 429/CAPTCHA từ alonhadat — đó là tín hiệu site
  đang chặn (nên rotate sang proxy khác), không phải bằng chứng proxy hỏng.

CẢNH BÁO BẢO MẬT (đọc trước khi dùng):
- Free proxy là 1 relay do người lạ vận hành, có thể log/chèn nội dung
  vào traffic HTTP không mã hoá. TUYỆT ĐỐI không gửi credentials/cookie
  đăng nhập/token qua free proxy.
- alonhadat.com.vn dùng HTTPS nên nội dung response được mã hoá end-to-end
  (proxy không đọc được nội dung trang), nhưng proxy vẫn biết domain bạn
  truy cập (qua SNI) và có thể chủ động làm chậm/drop request tuỳ ý —
  đây là lý do luôn cần pool nhiều proxy dự phòng, không phụ thuộc 1 cái.
"""
from __future__ import annotations

import concurrent.futures
import logging
import random
import time
from dataclasses import dataclass, field
from typing import List, Optional

import requests

logger = logging.getLogger(__name__)

# Endpoint trung lập để test proxy còn sống hay không.
_HEALTH_CHECK_URL = "https://httpbin.org/ip"
_HEALTH_CHECK_TIMEOUT = 6  # giây — free proxy thường chậm, không cần chờ lâu hơn

# Nguồn free proxy public phổ biến, không cần đăng ký (tham khảo 08/2026).
# Tỷ lệ sống thực tế biến động liên tục, KHÔNG lưu cứng kết quả — luôn
# fetch lại + health-check trước mỗi lần dùng.
PROXYSCRAPE_API = (
    "https://api.proxyscrape.com/v4/free-proxy-list/get"
    "?request=display_proxies&proxy_format=protocolipport"
    "&format=text&protocol=http"
)
GEONODE_API = (
    "https://proxylist.geonode.com/api/proxy-list"
    "?limit=100&page=1&sort_by=lastChecked&sort_type=desc&protocols=http"
)


@dataclass
class ProxyEntry:
    url: str  # dạng "http://ip:port"
    source: str
    consecutive_failures: int = 0
    last_checked: float = field(default_factory=time.time)
    alive: bool = True


def fetch_candidate_proxies() -> List[ProxyEntry]:
    """
    Lấy danh sách proxy ỨNG VIÊN (chưa kiểm chứng) từ các nguồn public.
    Mỗi nguồn lỗi độc lập (try/except riêng) để 1 nguồn sập không chặn
    toàn bộ hàm — càng nhiều nguồn, pool sau health-check càng lớn.
    """
    candidates: List[ProxyEntry] = []

    # Nguồn 1: ProxyScrape v4 — trả về text, mỗi dòng "ip:port"
    try:
        resp = requests.get(PROXYSCRAPE_API, timeout=10)
        resp.raise_for_status()
        for line in resp.text.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            candidates.append(ProxyEntry(url=f"http://{line}", source="proxyscrape"))
    except requests.RequestException as e:
        logger.warning("Không lấy được proxy từ ProxyScrape: %s", e)

    # Nguồn 2: GeoNode — trả JSON
    try:
        resp = requests.get(GEONODE_API, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("data", []):
            ip = item.get("ip")
            port = item.get("port")
            if ip and port:
                candidates.append(ProxyEntry(url=f"http://{ip}:{port}", source="geonode"))
    except (requests.RequestException, ValueError) as e:
        logger.warning("Không lấy được proxy từ GeoNode: %s", e)

    # Khử trùng theo url, giữ nguyên thứ tự
    seen = set()
    unique: List[ProxyEntry] = []
    for c in candidates:
        if c.url not in seen:
            seen.add(c.url)
            unique.append(c)

    logger.info(
        "Tổng %d proxy ứng viên (đã khử trùng) từ %d nguồn",
        len(unique), len({c.source for c in unique}),
    )
    return unique


def _check_one(proxy: ProxyEntry) -> ProxyEntry:
    """Health-check 1 proxy qua _HEALTH_CHECK_URL — KHÔNG phải alonhadat.com.vn."""
    try:
        resp = requests.get(
            _HEALTH_CHECK_URL,
            proxies={"http": proxy.url, "https": proxy.url},
            timeout=_HEALTH_CHECK_TIMEOUT,
        )
        proxy.alive = resp.status_code == 200
    except requests.RequestException:
        proxy.alive = False
    proxy.last_checked = time.time()
    return proxy


def build_working_pool(candidates: List[ProxyEntry], max_workers: int = 30) -> List[ProxyEntry]:
    """
    Health-check song song toàn bộ candidates (I/O-bound -> dùng thread
    pool), trả về CHỈ CÁC PROXY CÒN SỐNG.
    Vì tỷ lệ sống thực tế free proxy chỉ ~5-12%, nên gọi
    fetch_candidate_proxies() lấy càng nhiều ứng viên càng tốt trước khi lọc.
    """
    working: List[ProxyEntry] = []
    if not candidates:
        return working

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(_check_one, candidates):
            if result.alive:
                working.append(result)

    logger.info(
        "Health-check xong: %d/%d proxy còn sống (%.1f%%)",
        len(working), len(candidates), 100 * len(working) / len(candidates),
    )
    return working


class ProxyPool:
    """
    Pool proxy dùng trong lúc crawl thật — round-robin, tự loại proxy
    chết sau N lần LỖI KỸ THUẬT liên tiếp (không tính 429/CAPTCHA từ
    alonhadat — xử lý riêng ở tầng crawl_runner/fetcher).
    """

    def __init__(self, proxies: List[ProxyEntry], max_consecutive_failures: int = 2):
        self._proxies = list(proxies)
        random.shuffle(self._proxies)  # tránh luôn ưu tiên đúng 1 proxy đầu danh sách
        self._idx = 0
        self._max_consecutive_failures = max_consecutive_failures

    def __len__(self) -> int:
        return len(self._proxies)

    def get_next(self) -> Optional[ProxyEntry]:
        """Lấy proxy kế tiếp theo round-robin. None nếu pool rỗng."""
        if not self._proxies:
            return None
        proxy = self._proxies[self._idx % len(self._proxies)]
        self._idx += 1
        return proxy

    def report_failure(self, proxy: ProxyEntry) -> None:
        """Gọi khi proxy lỗi KỸ THUẬT (timeout/connection error).
        KHÔNG gọi khi gặp 429/CAPTCHA — đó là site chặn, không phải proxy hỏng."""
        proxy.consecutive_failures += 1
        if proxy.consecutive_failures >= self._max_consecutive_failures:
            self._proxies = [p for p in self._proxies if p.url != proxy.url]
            logger.warning(
                "Loại proxy %s khỏi pool sau %d lần lỗi liên tiếp. Còn lại %d proxy.",
                proxy.url, proxy.consecutive_failures, len(self._proxies),
            )

    def report_success(self, proxy: ProxyEntry) -> None:
        proxy.consecutive_failures = 0

    def is_low(self, threshold: int = 5) -> bool:
        """True nếu pool còn quá ít proxy sống, cần refresh bằng
        fetch_candidate_proxies() + build_working_pool() lại."""
        return len(self._proxies) < threshold

    def refill(self, new_proxies: List[ProxyEntry]) -> None:
        """
        Bổ sung thêm proxy mới (đã health-check) vào pool HIỆN CÓ, khử
        trùng theo url. Dùng mutate-in-place (không tạo object mới) để
        caller giữ nguyên tham chiếu ProxyPool đang dùng — quan trọng vì
        Python không cho reassign biến của caller từ trong 1 hàm khác.
        """
        existing_urls = {p.url for p in self._proxies}
        added = [p for p in new_proxies if p.url not in existing_urls]
        self._proxies.extend(added)
        logger.info(
            "Đã bổ sung %d proxy mới vào pool (tổng hiện có: %d)",
            len(added), len(self._proxies),
        )


if __name__ == "__main__":
    # Chạy thử độc lập: python -m crawler.proxy_manager
    logging.basicConfig(level=logging.INFO)
    candidates = fetch_candidate_proxies()
    pool_entries = build_working_pool(candidates)
    pool = ProxyPool(pool_entries)
    print(f"\nPool sẵn sàng: {len(pool)} proxy sống / {len(candidates)} ứng viên")
    for p in pool_entries[:10]:
        print(" -", p.url, "(nguồn:", p.source + ")")
