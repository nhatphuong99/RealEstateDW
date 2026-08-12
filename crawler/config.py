"""
Cấu hình dùng chung cho toàn bộ crawler.
Đọc biến môi trường từ .env (dùng python-dotenv) để không hard-code
credentials, đúng nguyên tắc bảo mật đã đặt ra từ GĐ2 của đồ án.
"""
import os
import random
from pathlib import Path

from dotenv import load_dotenv

# Tự tìm .env ở gốc project (2 cấp trên file này: crawler/ -> project root)
_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH)


# ---------------------------------------------------------------------
# Database (Postgres DW) — 2 DSN riêng biệt:
#   - POSTGRES_DW_DSN        : dùng khi code chạy BÊN TRONG container
#                               Airflow (resolve qua tên service Docker)
#   - POSTGRES_DW_DSN_LOCAL  : dùng khi chạy trực tiếp trên máy host
#                               (VD: script test/debug ngoài Docker)
# db.py sẽ tự chọn biến phù hợp dựa trên RUNNING_IN_CONTAINER.
# ---------------------------------------------------------------------
POSTGRES_DW_DSN = os.getenv("POSTGRES_DW_DSN")
POSTGRES_DW_DSN_LOCAL = os.getenv("POSTGRES_DW_DSN_LOCAL")
# Trong image Airflow chính thức, biến này không tồn tại trên host, chỉ có
# trong container -> dùng để phân biệt môi trường đang chạy.
RUNNING_IN_CONTAINER = os.getenv("AIRFLOW_HOME") is not None or os.getenv(
    "RUNNING_IN_CONTAINER", ""
).lower() in ("1", "true")


# ---------------------------------------------------------------------
# AWS S3 (Bronze layer)
# ---------------------------------------------------------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BRONZE_BUCKET = os.getenv("S3_BRONZE_BUCKET")


# ---------------------------------------------------------------------
# Danh mục (category) crawl — CHỈ lấy các category "lá", KHÔNG lấy
# category cha "can-ban-nha" (Mua bán nhà ở HCM nói chung) vì nó CHỒNG
# LẤN với "nha_mat_tien" + "nha_trong_hem" -> gây đọc lặp giữa các
# category (đã ghi nhận là nguyên nhân khuếch đại đọc lặp, mục 8.2
# tong_hop_boi_canh_crawler_alonhadat.md). Property_type suy ra trực
# tiếp từ category, không suy luận từ tiêu đề.
# ---------------------------------------------------------------------
CATEGORIES = {
    "can_ho_chung_cu": "https://alonhadat.com.vn/can-ban-can-ho-chung-cu/ho-chi-minh",
    "phong_tro_nha_tro": "https://alonhadat.com.vn/can-ban-phong-tro-nha-tro/ho-chi-minh",
    "biet_thu_nha_lien_ke": "https://alonhadat.com.vn/can-ban-biet-thu-nha-lien-ke/ho-chi-minh",
    "nha_mat_tien": "https://alonhadat.com.vn/can-ban-nha-mat-tien/ho-chi-minh",
    "nha_trong_hem": "https://alonhadat.com.vn/can-ban-nha-trong-hem/ho-chi-minh",
}

PROPERTY_TYPE_LABELS = {
    "can_ho_chung_cu": "Căn hộ chung cư",
    "phong_tro_nha_tro": "Phòng trọ/Nhà trọ",
    "biet_thu_nha_lien_ke": "Biệt thự/Nhà liền kề",
    "nha_mat_tien": "Nhà mặt tiền",
    "nha_trong_hem": "Nhà trong hẻm",
}


# ---------------------------------------------------------------------
# Proxy rotation (bổ sung sau khi phát hiện 429 dai dẳng + CAPTCHA lần
# đầu 2026/08/13 — xem error_log.md). Escalation: direct -> proxy
# rotation -> Playwright (dự phòng, chưa code).
# ---------------------------------------------------------------------
USE_PROXY_ROTATION = True
PROXY_POOL_MIN_SIZE = 5           # dưới ngưỡng này -> refresh pool
PROXY_MAX_CONSECUTIVE_FAILURES = 2



# ---------------------------------------------------------------------
# Rate limiting & vận hành — dựa trên ngưỡng thực nghiệm ~15-20
# request/phút gây 429 (alonhadat_data_source_analysis.md, mục 3) và
# các quyết định cuối cùng ở tong_hop_boi_canh_crawler_alonhadat.md, mục 5.
# ---------------------------------------------------------------------
MIN_DELAY_SECONDS = 15
MAX_DELAY_SECONDS = 25
# Giảm batch, tăng tần suất trigger DAG bù lại (dự kiến sửa cron trong
# dag_crawl_alonhadat.py từ "0 1,5,9,13,17,21 * * *" -> dày hơn, VD mỗi 90 phút)
MAX_PAGES_PER_RUN = 15            # giảm từ 40 -> 15 — CẦN ĐO LẠI, đây là con số ước lượng ban đầu
REQUEST_TIMEOUT_SECONDS = 15

MAX_ATTEMPTS = 5
BACKOFF_BASE_MINUTES = 2        # backoff tăng dần: 2, 4, 8, 16, 32... phút
BACKOFF_MAX_MINUTES = 60

CIRCUIT_BREAKER_THRESHOLD = 3   # 3 lần 429 liên tiếp trong 1 run -> dừng run
CIRCUIT_BREAKER_COOLDOWN_MINUTES = 30

# Row bị kẹt ở status='in_progress' quá lâu (task/worker crash giữa chừng)
# sẽ được requeue_stale() đưa lại về 'pending'.
STALE_IN_PROGRESS_MINUTES = 30

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def random_delay_seconds() -> float:
    """Delay ngẫu nhiên giữa 2 lần fetch, theo đúng MIN/MAX_DELAY_SECONDS."""
    return random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)


def backoff_minutes(attempt_count: int) -> int:
    """Exponential backoff: 2, 4, 8, 16, 32... phút, chặn trên BACKOFF_MAX_MINUTES."""
    minutes = BACKOFF_BASE_MINUTES * (2 ** max(attempt_count - 1, 0))
    return min(minutes, BACKOFF_MAX_MINUTES)
