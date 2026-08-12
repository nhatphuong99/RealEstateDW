"""
crawler
=======
Package tự viết để crawl alonhadat.com.vn, KHÔNG dùng Scrapy.

Lý do không dùng Scrapy (đã chốt trong tong_hop_boi_canh_crawler_alonhadat.md,
mục 4): Scrapy quản lý queue trong RAM của 1 lần chạy (JOBDIR chỉ đảm bảo
resume khi "dừng sạch", không đảm bảo khi crash cứng) — không đáp ứng được
yêu cầu tự quản lý queue để chống crawl lặp một cách chắc chắn khi chạy
định kỳ qua Airflow trong nhiều ngày.

Các module chính:
    config.py        - toàn bộ cấu hình dùng chung (categories, rate-limit,...)
    db.py             - kết nối Postgres (tách DSN cho container vs. host)
    queue_manager.py  - CRUD/claim/backoff cho crawl.crawl_queue
    fetcher.py        - HTTP fetch thuần (requests), không tự retry 429
    storage.py        - upload/download HTML .gz lên/từ S3 (Bronze)
    crawl_runner.py   - logic DAG "Fetch" (run_batch)
    parser.py         - logic DAG "Parse" (run_parse_batch)
"""
