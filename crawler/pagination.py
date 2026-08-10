"""
Sinh URL trang danh sách bằng số học (/trang-N) (không dùng link ">>" trên
trang vì từng quan sát thấy nó nhảy cóc 1 -> 9 -> 13). Điểm khác với
Scrapy: ở đây chỉ TẠO BẢN GHI trong hàng đợi SQL, việc quyết định "còn
trang hay hết trang" nằm ở crawl_runner.py (dựa vào số lượng item đọc
được từ HTML sau khi fetch).
"""
from . import config
from .queue_manager import enqueue_urls


def category_start_url(category: str) -> str:
    return f"{config.BASE_URL}{config.CATEGORIES[category]}"


def build_page_url(category: str, page_number: int) -> str:
    base = f"{config.BASE_URL}{config.CATEGORIES[category]}"
    if page_number <= 1:
        return base
    return f"{base}/trang-{page_number}"


def enqueue_category_seeds() -> int:
    """Gọi 1 lần lúc setup (hoặc mỗi khi hàng đợi rỗng hoàn toàn) — đưa
    trang 1 của mỗi category vào hàng đợi."""
    rows = [
        {
            "url": category_start_url(cat),
            "url_type": "list",
            "category": cat,
            "page_number": 1,
        }
        for cat in config.CATEGORIES
    ]
    return enqueue_urls(rows)


def enqueue_next_page(category: str, current_page: int) -> int:
    """Đưa trang kế tiếp của category vào hàng đợi."""
    next_page = current_page + 1
    return enqueue_urls([{
        "url": build_page_url(category, next_page),
        "url_type": "list",
        "category": category,
        "page_number": next_page,
    }])
