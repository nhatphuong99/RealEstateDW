"""
Cấu hình dùng chung cho crawler.
Đọc từ biến môi trường (Airflow Connections / .env), KHÔNG hard-code
credentials - dùng nguyên tắc bảo mật đã thống nhất trong 01_phan_tich_yeu_cau_so_bo.md.
"""
import os

# --- Database (postgres-dw, service đã có sẵn trong docker-compose.yaml) ---
DB_DSN = os.environ["POSTGRES_DW_DSN"]  # vd: postgresql://user:pass@postgres-dw:5432/dbname

# --- S3 (Bronze layer) ---
S3_BUCKET = os.environ["S3_BRONZE_BUCKET"]
S3_PREFIX = "bronze/alonhadat"

# --- HTTP ---
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT_SECONDS = 15

# --- Rate limiting ---
# Đã tăng so với khuyến nghị ban đầu (5-8s) sau khi quan sát 429 vẫn
# xuất hiện dù đã giãn cách 5-10 phút giữa các lần chạy - cho thấy
# ngưỡng chặn có thể tích lũy theo phiên/giờ, không chỉ theo phút như
# giả định ban đầu. Tăng biên độ để giảm tần suất trung bình trong lúc
# còn đang dò lại hành vi thật của site.
MIN_DELAY_SECONDS = 15.0
MAX_DELAY_SECONDS = 30.0

# --- Retry / backoff cho lỗi tạm thời ---
# QUAN TRỌNG: 429 KHÔNG nằm trong danh sách này - xem lý do trong
# fetcher.py (retry nội bộ cho 429 gần như vô ích và làm batch chậm bất
# thường). 429 được xử lý riêng, backoff hoàn toàn ở tầng hàng đợi
# (queue_manager.mark_failed), theo đơn vị phút chứ không phải giây.
RETRY_STATUS_CODES = {500, 502, 503, 504}
BASE_BACKOFF_SECONDS = 30.0
MAX_BACKOFF_SECONDS = 300.0

# --- Circuit breaker cho 429 liên tiếp trong 1 batch ---
# Nếu gặp N lần 429 LIÊN TIẾP trong 1 batch, dừng xử lý các URL còn lại
# của batch ngay (thay vì cố "càn" hết toàn bộ batch, vừa tốn thời gian
# vừa tiếp tục gửi request vào 1 site đang rõ ràng giới hạn).
CIRCUIT_BREAKER_CONSECUTIVE_429 = 3
BATCH_COOLDOWN_MINUTES = 30  # backoff áp dụng cho các URL bị đẩy lại do circuit breaker

# --- Batch size cho 1 LẦN CHẠY Airflow task ---
# Không chạy vòng lặp sleep vô hạn trong 1 task (anti-pattern Airflow -
# giữ worker slot quá lâu). Thay vào đó: mỗi lần Airflow trigger DAG chỉ
# xử lý 1 batch giới hạn rồi task tự kết thúc; DAG được lịch chạy nhiều
# lần/ngày (xem dags/dag_crawl_alonhadat.py) - đúng tinh thần "chia batch
# ngắn" đã rút ra từ phát hiện rate-limit.
CRAWL_BATCH_SIZE = 120
STALE_IN_PROGRESS_MINUTES = 60  # quá thời gian này mà còn 'in_progress' -> coi là crash, requeue

# --- Nguồn crawl ---
# Chỉ còn alonhadat.com.vn là nguồn chính - xem quyết định trong
# batdongsan_data_source_analysis.md mục 6 (batdongsan bị loại do
# Cloudflare Turnstile leo thang khi phát hiện pattern crawl tự động).
BASE_URL = "https://alonhadat.com.vn"
CATEGORIES = {
    "can-ban-nha-tp-hcm": "/can-ban-nha/ho-chi-minh",
    "can-ban-can-ho-chung-cu-tp-hcm": "/can-ban-can-ho-chung-cu/ho-chi-minh",
    "can-ban-biet-thu-nha-lien-ke-tp-hcm": "/can-ban-biet-thu-nha-lien-ke/ho-chi-minh",
    "can-ban-phong-tro-nha-tro-tp-hcm": "/can-ban-phong-tro-nha-tro/ho-chi-minh",
}
