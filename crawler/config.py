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
# category cha "can-ban-nha" (chồng lấn nha_mat_tien + nha_trong_hem).
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
# Rate limiting & vận hành — dựa trên ngưỡng thực nghiệm ~15-20
# request/phút gây 429. CẬP NHẬT 2026/08/13: sau khi phát hiện 429 dai
# dẳng + CAPTCHA lần đầu, giảm MAX_PAGES_PER_RUN. Con số 15 là điểm khởi
# đầu để đo lại thực nghiệm, CHƯA có căn cứ chắc chắn.
# ---------------------------------------------------------------------
MIN_DELAY_SECONDS = 15
MAX_DELAY_SECONDS = 25
MAX_PAGES_PER_RUN = 15          # giảm từ 40 -> 15, CẦN ĐO LẠI thực nghiệm
REQUEST_TIMEOUT_SECONDS = 15

MAX_ATTEMPTS = 5
BACKOFF_BASE_MINUTES = 2
BACKOFF_MAX_MINUTES = 60

CIRCUIT_BREAKER_THRESHOLD = 3
CIRCUIT_BREAKER_COOLDOWN_MINUTES = 30

STALE_IN_PROGRESS_MINUTES = 30

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def random_delay_seconds() -> float:
    return random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)


def backoff_minutes(attempt_count: int) -> int:
    minutes = BACKOFF_BASE_MINUTES * (2 ** max(attempt_count - 1, 0))
    return min(minutes, BACKOFF_MAX_MINUTES)


# ---------------------------------------------------------------------
# Proxy rotation — bổ sung 2026/08/13, thiết kế lại theo mô hình HYBRID
# sau khi làm rõ ý định gốc (2026/08/13, lần 2):
#
#   - KHÔNG loại bỏ hoàn toàn proxy đã dùng thành công (sẽ khiến pool
#     gần như luôn rỗng, vì tỷ lệ sống <1% đã đo được — loại bỏ đúng
#     những proxy hiếm hoi đã chứng minh vượt được rate-limit là lãng
#     phí thông tin quý giá nhất).
#   - CŨNG KHÔNG tái dùng vô hạn 1 proxy (rủi ro 1 IP tích luỹ quá nhiều
#     dấu vết truy cập alonhadat theo thời gian).
#   - => HYBRID: cho tái dùng nhưng có TRẦN số lần (PROXY_MAX_REUSE_COUNT),
#     hết trần thì CHỦ ĐỘNG loại dù proxy vẫn còn sống, ép pool luân
#     chuyển sang proxy khác.
# ---------------------------------------------------------------------
USE_PROXY_ROTATION = True

# Số proxy sống tối thiểu trong cache để KHÔNG cần quét lại toàn bộ
# nguồn free (quét toàn bộ tốn thời gian đáng kể).
PROXY_POOL_MIN_SIZE = 3

# Timeout cho health-check 1 proxy (giây).
PROXY_HEALTH_CHECK_TIMEOUT_SECONDS = 5

# Số thread song song khi health-check TOÀN BỘ candidate pool (chỉ chạy
# khi cache cạn).
PROXY_CANDIDATE_SCAN_WORKERS = 50

# Loại proxy CHẾT khỏi cache: nếu 1 proxy trong cache KHÔNG được xác
# nhận thành công (fetch thật vào alonhadat.com.vn) trong quá
# PROXY_CACHE_MAX_STALE_RUNS lần run_batch() liên tiếp -> coi là đã chết,
# xoá khỏi cache (đếm theo SỐ LẦN CHẠY, không phải theo thời gian thực).
PROXY_CACHE_MAX_STALE_RUNS = 5

# Trần tái sử dụng cho proxy CÒN SỐNG: dù 1 proxy vẫn đang chạy tốt, chỉ
# cho phép dùng THÀNH CÔNG tối đa PROXY_MAX_REUSE_COUNT lần rồi CHỦ ĐỘNG
# loại khỏi cache (không đợi nó chết) — mục đích: không để 1 IP tích luỹ
# quá nhiều dấu vết truy cập alonhadat theo thời gian, ép pool phải luân
# chuyển sang proxy khác dù proxy cũ vẫn "còn dùng được". Đặt = 1 nếu
# muốn loại trừ NGAY từ lần dùng đầu tiên (đúng ý định gốc thuần túy,
# không tái dùng).
PROXY_MAX_REUSE_COUNT = 5

# Khi 1 URL gặp 429/CAPTCHA, số lần được phép đổi proxy để thử lại CÙNG
# URL đó trước khi coi là thất bại thật sự.
PROXY_RETRY_PER_URL = 2
