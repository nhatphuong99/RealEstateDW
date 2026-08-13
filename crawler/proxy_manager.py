"""
crawler/proxy_manager.py

Quản lý pool proxy free cho crawler DEP305x — cache 2 tầng + chính sách
HYBRID về vòng đời proxy trong cache (xem error_log.md 2026/08/13, 2 lần
thảo luận):

  TẦNG 1 — Cache (nhanh, DB-backed, bảng crawl.proxy_cache):
      Chỉ giữ các proxy đã CHẠY THẬT thành công (fetch alonhadat.com.vn
      thành công, không phải chỉ health-check httpbin). Mỗi lần chạy,
      thử lại (health-check nhanh) đúng những proxy này trước.

  TẦNG 2 — Quét lại toàn bộ nguồn free (chậm, chỉ chạy khi TẦNG 1 không
      đủ PROXY_POOL_MIN_SIZE proxy sống).

  VÒNG ĐỜI 1 proxy trong cache — HYBRID (không phải "dùng mãi mãi" và
  cũng KHÔNG phải "loại ngay sau lần dùng đầu"):
    - Chết (không thành công quá PROXY_CACHE_MAX_STALE_RUNS lần chạy
      liên tiếp) -> xoá.
    - Dùng thành công đủ PROXY_MAX_REUSE_COUNT lần (dù vẫn còn sống)
      -> CHỦ ĐỘNG xoá, ép pool luân chuyển sang proxy khác, tránh 1 IP
      tích luỹ quá nhiều dấu vết truy cập alonhadat theo thời gian.

Health-check TẦNG 2 LUÔN gọi tới _HEALTH_CHECK_URL (endpoint trung lập),
TUYỆT ĐỐI KHÔNG gọi trực tiếp alonhadat.com.vn — không tốn quota
rate-limit của site đích cho việc kiểm tra hạ tầng proxy.

CẢNH BÁO BẢO MẬT: free proxy là relay do người lạ vận hành, có thể
log/chèn traffic HTTP không mã hoá. TUYỆT ĐỐI không gửi credentials/cookie
đăng nhập qua free proxy.
"""
from __future__ import annotations

import concurrent.futures
import logging
import random
import time
from dataclasses import dataclass, field
from typing import List, Optional, Set

import psycopg2.extras
import requests

from crawler import config

logger = logging.getLogger(__name__)

_HEALTH_CHECK_URL = "https://httpbin.org/ip"

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


# =======================================================================
# TẦNG 2 — Fetch & health-check candidate từ nguồn public
# =======================================================================

def fetch_candidate_proxies() -> List[ProxyEntry]:
    candidates: List[ProxyEntry] = []

    try:
        resp = requests.get(PROXYSCRAPE_API, timeout=10)
        resp.raise_for_status()
        for line in resp.text.strip().splitlines():
            line = line.strip()
            if line:
                candidates.append(ProxyEntry(url=f"http://{line}", source="proxyscrape"))
    except requests.RequestException as e:
        logger.warning("Không lấy được proxy từ ProxyScrape: %s", e)

    try:
        resp = requests.get(GEONODE_API, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("data", []):
            ip, port = item.get("ip"), item.get("port")
            if ip and port:
                candidates.append(ProxyEntry(url=f"http://{ip}:{port}", source="geonode"))
    except (requests.RequestException, ValueError) as e:
        logger.warning("Không lấy được proxy từ GeoNode: %s", e)

    seen: Set[str] = set()
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
    try:
        resp = requests.get(
            _HEALTH_CHECK_URL,
            proxies={"http": proxy.url, "https": proxy.url},
            timeout=config.PROXY_HEALTH_CHECK_TIMEOUT_SECONDS,
        )
        proxy.alive = resp.status_code == 200
    except requests.RequestException:
        proxy.alive = False
    proxy.last_checked = time.time()
    return proxy


def build_working_pool(candidates: List[ProxyEntry], max_workers: int = None) -> List[ProxyEntry]:
    working: List[ProxyEntry] = []
    if not candidates:
        return working

    if max_workers is None:
        max_workers = config.PROXY_CANDIDATE_SCAN_WORKERS
    workers = max(1, min(max_workers, len(candidates)))

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        for result in executor.map(_check_one, candidates):
            if result.alive:
                working.append(result)

    logger.info(
        "Health-check xong: %d/%d proxy còn sống (%.1f%%)",
        len(working), len(candidates), 100 * len(working) / len(candidates),
    )
    return working


# =======================================================================
# TẦNG 1 — Cache DB-backed (bảng crawl.proxy_cache, sql/migration_002)
# =======================================================================

def load_cache(conn) -> List[ProxyEntry]:
    """Lấy các proxy đang có trong crawl.proxy_cache (đã loại sẵn proxy
    chết/hết trần tái sử dụng ở record_proxy_outcome() của lần chạy
    trước, nên không cần filter thêm ở đây)."""
    entries: List[ProxyEntry] = []
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT proxy_url, source FROM crawl.proxy_cache")
        for row in cur.fetchall():
            entries.append(ProxyEntry(url=row["proxy_url"], source=row["source"] or "cache"))
    return entries


def get_or_refresh_pool(conn, min_size: int = None) -> "ProxyPool":
    """Thử cache trước (nhanh), CHỈ quét lại toàn bộ nguồn public (chậm)
    khi cache không đủ min_size proxy sống. Trả về ProxyPool RỖNG (không
    raise lỗi) nếu không tìm được proxy nào — crawl_runner tự fallback
    về fetch trực tiếp không qua proxy."""
    if min_size is None:
        min_size = config.PROXY_POOL_MIN_SIZE

    cached = load_cache(conn)
    alive_from_cache = build_working_pool(cached, max_workers=max(len(cached), 1)) if cached else []

    if len(alive_from_cache) >= min_size:
        logger.info(
            "Dùng %d proxy từ cache (đủ min_size=%d) — bỏ qua quét toàn bộ nguồn free.",
            len(alive_from_cache), min_size,
        )
        return ProxyPool(alive_from_cache)

    logger.info(
        "Cache chỉ còn %d/%d proxy sống (< min_size=%d) -> quét lại toàn bộ nguồn public...",
        len(alive_from_cache), len(cached), min_size,
    )
    candidates = fetch_candidate_proxies()
    fresh_alive = build_working_pool(candidates)

    combined = {p.url: p for p in (alive_from_cache + fresh_alive)}
    if not combined:
        logger.warning(
            "KHÔNG có proxy nào sống (cache lẫn quét mới) — batch này sẽ fetch "
            "TRỰC TIẾP không qua proxy."
        )

    # get_or_refresh_pool(conn, min_size)
    return ProxyPool(list(combined.values()))


def record_proxy_outcome(
    conn,
    successful_urls: Set[str],
    max_stale_runs: int = None,
    max_reuse_count: int = None,
) -> None:
    """
    Gọi 1 LẦN DUY NHẤT ở CUỐI mỗi run_batch() THẬT.

    Chính sách HYBRID áp dụng ở đây:
      1. Proxy trong `successful_urls` -> upsert last_success_at=now(),
         runs_since_last_success=0, times_used += 1.
      2. Proxy còn lại trong cache (không dùng thành công lần này) ->
         +1 runs_since_last_success.
      3. Xoá khỏi cache proxy CHẾT (runs_since_last_success > max_stale_runs).
      4. Xoá khỏi cache proxy đã dùng ĐỦ max_reuse_count lần (dù vẫn còn
         sống) -> ép luân chuyển sang proxy khác ở lần chạy sau.
    """
    if max_stale_runs is None:
        max_stale_runs = config.PROXY_CACHE_MAX_STALE_RUNS
    if max_reuse_count is None:
        max_reuse_count = config.PROXY_MAX_REUSE_COUNT

    with conn.cursor() as cur:
        for url in successful_urls:
            cur.execute(
                """
                INSERT INTO crawl.proxy_cache
                    (proxy_url, source, last_success_at, runs_since_last_success, times_used)
                VALUES (%s, %s, now(), 0, 1)
                ON CONFLICT (proxy_url) DO UPDATE
                    SET last_success_at = now(),
                        runs_since_last_success = 0,
                        times_used = crawl.proxy_cache.times_used + 1
                """,
                (url, "confirmed_alonhadat"),
            )

        exclude = tuple(successful_urls) if successful_urls else ("",)
        cur.execute(
            """
            UPDATE crawl.proxy_cache
            SET runs_since_last_success = runs_since_last_success + 1
            WHERE proxy_url NOT IN %s
            """,
            (exclude,),
        )

        cur.execute(
            "DELETE FROM crawl.proxy_cache WHERE runs_since_last_success > %s",
            (max_stale_runs,),
        )

        cur.execute(
            "DELETE FROM crawl.proxy_cache WHERE times_used >= %s",
            (max_reuse_count,),
        )
    conn.commit()
    logger.info(
        "record_proxy_outcome: %d proxy xác nhận sống lần này (fetch alonhadat thật thành công); "
        "đã dọn proxy chết (>%d lần thất bại liên tiếp) và proxy hết trần tái dùng (>=%d lần).",
        len(successful_urls), max_stale_runs, max_reuse_count,
    )


class ProxyPool:
    """Pool proxy dùng trong lúc crawl thật — round-robin, tự loại proxy
    chết sau N lần LỖI KỸ THUẬT liên tiếp trong CÙNG 1 lần chạy (khác với
    vòng đời dài hạn trong cache, xử lý ở record_proxy_outcome)."""

    def __init__(self, proxies: List[ProxyEntry], max_consecutive_failures: int = 2):
        self._proxies = list(proxies)
        random.shuffle(self._proxies)
        self._idx = 0
        self._max_consecutive_failures = max_consecutive_failures

    def __len__(self) -> int:
        return len(self._proxies)

    def get_next(self) -> Optional[ProxyEntry]:
        if not self._proxies:
            return None
        proxy = self._proxies[self._idx % len(self._proxies)]
        self._idx += 1
        return proxy

    def report_failure(self, proxy: ProxyEntry) -> None:
        proxy.consecutive_failures += 1
        if proxy.consecutive_failures >= self._max_consecutive_failures:
            self._proxies = [p for p in self._proxies if p.url != proxy.url]
            logger.warning(
                "Loại proxy %s khỏi pool (lần chạy này) sau %d lần lỗi liên tiếp. Còn lại %d proxy.",
                proxy.url, proxy.consecutive_failures, len(self._proxies),
            )

    def report_success(self, proxy: ProxyEntry) -> None:
        proxy.consecutive_failures = 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    candidates = fetch_candidate_proxies()
    pool_entries = build_working_pool(candidates)
    pool = ProxyPool(pool_entries)
    print(f"\nPool sẵn sàng: {len(pool)} proxy sống / {len(candidates)} ứng viên")
    for p in pool_entries[:10]:
        print(" -", p.url, "(nguồn:", p.source + ")")
