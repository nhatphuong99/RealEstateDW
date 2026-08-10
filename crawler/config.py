"""
Cấu hình dùng chung cho crawler_v3.
Đọc từ biến môi trường (Airflow Connections / .env), KHÔNG hard-code
credentials — dùng nguyên tắc bảo mật đã thống nhất trong 01_phan_tich_yeu_cau_so_bo.md.
"""
import os

# --- Database (postgres-dw, service đã có sẵn trong docker-compose.yaml) ---
DB_DSN = os.environ["POSTGRES_DW_DSN"]  # ví dụ: postgresql://user:pass@postgres-dw:5432/dbname

# --- S3 (Bronze layer) ---
S3_BUCKET = os.environ["S3_BUCKET_NAME"]
S3_PREFIX = "bronze/alonhadat"

# --- HTTP ---
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT_SECONDS = 15

# --- Rate limiting ---
# Theo khuyến nghị đã xác nhận thực nghiệm trong alonhadat_data_source_analysis.md
# mục 3 (ngưỡng 429 rơi vào khoảng ~15-20 request/phút cho cùng 1 domain).
MIN_DELAY_SECONDS = 5.0
MAX_DELAY_SECONDS = 8.0

# --- Retry / backoff cho lỗi tạm thời (429, 5xx) ---
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
BASE_BACKOFF_SECONDS = 10.0
MAX_BACKOFF_SECONDS = 300.0

# --- Batch size cho 1 LẦN CHẠY Airflow task ---
# Không chạy vòng lặp sleep vô hạn trong 1 task (anti-pattern Airflow —
# giữ worker slot quá lâu). Thay vào đó: mỗi lần Airflow trigger DAG chỉ
# xử lý 1 batch giới hạn rồi task tự kết thúc; DAG được lịch chạy nhiều
# lần/ngày (xem dags/dag_crawl_alonhadat.py) — đúng tinh thần "chia batch
# ngắn" đã rút ra từ phát hiện rate-limit.
CRAWL_BATCH_SIZE = 150
STALE_IN_PROGRESS_MINUTES = 25  # quá thời gian này mà còn 'in_progress' → coi là crash, requeue

# --- Nguồn crawl ---
# alonhadat.com.vn
BASE_URL = "https://alonhadat.com.vn"
CATEGORIES = {
    "can-ban-nha-tp-hcm": "/can-ban-nha/ho-chi-minh",
    "can-ban-can-ho-chung-cu-tp-hcm": "/can-ban-can-ho-chung-cu/ho-chi-minh",
    "can-ban-biet-thu-nha-lien-ke-tp-hcm": "/can-ban-biet-thu-nha-lien-ke/ho-chi-minh",
    "can-ban-phong-tro-nha-tro-tp-hcm": "/can-ban-phong-tro-nha-tro/ho-chi-minh",
}
