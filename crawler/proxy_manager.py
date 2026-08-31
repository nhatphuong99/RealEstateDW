"""
crawler/proxy_manager.py

Quản lý proxy động cho crawler:
- Nguồn: ProxyScrape v4; GeoNode.
- Health-check song song qua httpbin.org/ip.
- ProxyPool: round-robin + refill; implement Protocol ProxyPool (current/rotate/mark_failed)
  — cắm thẳng vào WebCrawlerCore.
- Không persist proxy xuống DB (proxy free chết nhanh).
- Ghi chú API: ProxyScrape trả text — phải lọc tiền tố "http://";
  GeoNode yêu cầu User-Agent giả trình duyệt (thiếu → 403).
"""


from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests

from crawler import config

logger = logging.getLogger("proxy_manager")

PROXYSCRAPE_URL = "https://api.proxyscrape.com/v4/free-proxy-list/get"
GEONODE_URL = "https://proxylist.geonode.com/api/proxy-list"
HEALTH_CHECK_URL = "https://httpbin.org/ip"

# GeoNode chặn User-Agent mặc định của requests/curl (403) — đã xác nhận
# thực tế, BẮT BUỘC set header giả trình duyệt.
GEONODE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


# ============================================================
# 1. Fetch proxy thô từ 2 nguồn
# ============================================================

def fetch_from_proxyscrape(timeout: float = 15.0, limit: int = 500) -> list[str]:
    """Lấy proxy từ ProxyScrape v4 (không cần API key).
    Trả về dạng "http://ip:port" — chỉ giữ proxy http://, loại bỏ socks4/socks5
    (vì API filter protocol=http không chuẩn tuyệt đối)."""
    try:
        response = requests.get(
            PROXYSCRAPE_URL,
            params={
                "request": "getproxies",
                "protocol": "http",
                "proxy_format": "protocolipport",
                "format": "text",
                "timeout": 10000,
                "limit": limit,
            },
            timeout=timeout,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        logger.warning("Không lấy được proxy từ ProxyScrape: %s", exc)
        return []

    lines = response.text.strip().splitlines()
    return [line.strip() for line in lines if line.strip().lower().startswith("http://")]


def fetch_from_geonode(timeout: float = 15.0, limit: int = 100) -> list[str]:
    """Lấy danh sách proxy HTTP từ GeoNode. BẮT BUỘC có header User-Agent
    thật (xem GEONODE_HEADERS) — thiếu sẽ bị 403."""
    try:
        response = requests.get(
            GEONODE_URL,
            params={
                "limit": limit,
                "page": 1,
                "sort_by": "lastChecked",
                "sort_type": "desc",
                "protocols": "http",
            },
            headers=GEONODE_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        logger.warning("Không lấy được proxy từ GeoNode: %s", exc)
        return []

    try:
        rows = response.json().get("data", [])
    except ValueError:
        logger.warning("GeoNode trả về dữ liệu không phải JSON hợp lệ")
        return []

    proxies: list[str] = []
    for row in rows:
        ip = row.get("ip")
        port = row.get("port")
        protocols = row.get("protocols") or []
        if ip and port and "http" in protocols:
            proxies.append(f"http://{ip}:{port}")
    return proxies


# ============================================================
# 2. Health-check song song
# ============================================================

def health_check_one(proxy_url: str, timeout: float = 6.0) -> bool:
    """Kiểm tra proxy còn sống và đủ nhanh.
    Timeout ngắn (từ ProxyConfig) tự lọc chất lượng:
    proxy quá tải/băng thông kém sẽ không trả lời kịp dù vẫn "sống"."""
    start = time.monotonic()
    try:
        response = requests.get(
            HEALTH_CHECK_URL,
            proxies={"http": proxy_url, "https": proxy_url},
            timeout=timeout,
        )
        elapsed = time.monotonic() - start
        alive = response.status_code == 200
        if alive:
            logger.debug("Proxy %s sống, phản hồi sau %.2fs", proxy_url, elapsed)
        return alive
    except requests.exceptions.RequestException:
        return False


def health_check_parallel(
    candidates: list[str], max_workers: int = 20, timeout: float = 6.0
) -> list[str]:
    """Health-check song song nhiều proxy.
    Proxy free chết nhanh nên phải check ngay trước khi dùng,
    không tin danh sách gốc từ ProxyScrape/GeoNode.
    Trả về danh sách proxy còn sống."""

    if not candidates:
        return []

    alive: list[str] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_proxy = {
            executor.submit(health_check_one, proxy, timeout): proxy for proxy in candidates
        }
        for future in as_completed(future_to_proxy):
            proxy = future_to_proxy[future]
            try:
                if future.result():
                    alive.append(proxy)
            except Exception:  # noqa: BLE001 - health-check không được phép làm crash refill()
                logger.debug("Health-check lỗi bất thường với proxy %s", proxy)

    logger.info("Health-check: %d/%d proxy còn sống", len(alive), len(candidates))
    return alive


def fetch_fresh_proxies(
    max_candidates: int = config.PROXY_MAX_CANDIDATES,
    health_check_workers: int = config.PROXY_HEALTH_CHECK_WORKERS,
    health_check_timeout: float = config.PROXY_HEALTH_CHECK_TIMEOUT_SECONDS,
) -> list[str]:
    """Lấy proxy mới từ cả 2 nguồn, gộp + dedup (giữ thứ tự),
    health-check song song, trả về danh sách proxy còn sống và đủ nhanh
    (timeout ngắn tự loại proxy quá tải/dùng băng thông kém)."""
    proxyscrape_proxies = fetch_from_proxyscrape()
    geonode_proxies = fetch_from_geonode()

    combined = list(dict.fromkeys(proxyscrape_proxies + geonode_proxies))
    candidates = combined[:max_candidates]

    logger.info(
        "Thu được %d proxy thô (ProxyScrape=%d, GeoNode=%d) -> health-check %d proxy (timeout=%.1fs)",
        len(combined), len(proxyscrape_proxies), len(geonode_proxies), len(candidates),
        health_check_timeout,
    )
    return health_check_parallel(candidates, max_workers=health_check_workers, timeout=health_check_timeout)


# ============================================================
# 3. ProxyPool — round-robin + failure tracking + refill()
# ============================================================

class ProxyPool:
    """Quản lý pool proxy trong bộ nhớ (không lưu DB — proxy free chết nhanh).
    Implement Protocol ProxyPool (current/rotate/mark_failed), cắm thẳng vào WebCrawlerCore.

    `refill()` là method riêng, orchestrator (DAG) tự gọi khi cần bổ sung proxy
    (đầu run hoặc khi cạn) — network call tốn thời gian, không gọi trong vòng lặp fetch chính."""


    def __init__(self, proxies: Optional[list[str]] = None) -> None:
        self._lock = threading.Lock()
        self._proxies: list[str] = list(proxies) if proxies else []
        self._failed: set[str] = set()
        self._index: int = 0 if self._proxies else -1

    def __len__(self) -> int:
        return len(self._proxies)

    # -------- Protocol ProxyPool (dùng bởi WebCrawlerCore) --------

    def current(self) -> Optional[str]:
        with self._lock:
            if self._index < 0 or self._index >= len(self._proxies):
                return None
            return self._proxies[self._index]

    def rotate(self) -> Optional[str]:
        with self._lock:
            current = (
                self._proxies[self._index]
                if 0 <= self._index < len(self._proxies)
                else None
            )
            candidates = [
                p for p in self._proxies if p not in self._failed and p != current
            ]
            if not candidates:
                self._index = -1
                return None
            next_proxy = candidates[0]
            self._index = self._proxies.index(next_proxy)
            return next_proxy

    def mark_failed(self, proxy_url: str) -> None:
        with self._lock:
            self._failed.add(proxy_url)

    # -------- Mutation riêng, orchestrator (DAG) tự gọi --------

    def refill(
        self,
        max_candidates: int = config.PROXY_MAX_CANDIDATES,
        health_check_workers: int = config.PROXY_HEALTH_CHECK_WORKERS,
        health_check_timeout: float = config.PROXY_HEALTH_CHECK_TIMEOUT_SECONDS,
    ) -> int:
        """Fetch + health-check proxy mới, thay toàn bộ danh sách hiện tại
        (bỏ danh sách failed cũ). Trả về số proxy sống và đủ nhanh."""
        fresh = fetch_fresh_proxies(max_candidates, health_check_workers, health_check_timeout)
        with self._lock:
            self._proxies = fresh
            self._failed = set()
            self._index = 0 if fresh else -1
        logger.info("refill() hoàn tất: pool hiện có %d proxy sống", len(fresh))
        return len(fresh)

    def healthy_count(self) -> int:
        """Số proxy hiện còn hợp lệ (chưa bị mark_failed) trong pool."""
        with self._lock:
            return len([p for p in self._proxies if p not in self._failed])


if __name__ == "__main__":
    # Smoke test thủ công — CHỈ chạy trên máy có mạng thật, không chạy
    # trong CI/sandbox (2 API không nằm trong domain allowlist của agent).
    logging.basicConfig(level=logging.INFO)
    pool = ProxyPool()
    n = pool.refill()
    print(f"Đã nạp {n} proxy sống. current() = {pool.current()}")
