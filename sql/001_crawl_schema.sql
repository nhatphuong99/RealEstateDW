-- =============================================================================
-- sql/001_crawl_schema.sql
-- Control-plane cho DAG 2 (web_crawler) — crawl trực tiếp
-- alonhadat.com.vn theo cơ chế queue (listing_progress + detail_queue).
--
-- Idempotent: dùng IF NOT EXISTS, có thể chạy lại nhiều lần an toàn.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS crawl;

-- 1. Con trỏ crawl trang danh sách, theo từng tổ hợp loại-tin x loại-BĐS
CREATE TABLE IF NOT EXISTS crawl.listing_progress (
    id              SERIAL PRIMARY KEY,
    listing_type    TEXT NOT NULL,          -- can-ban / cho-thue
    property_type   TEXT NOT NULL,          -- 5 loại BĐS
    current_page    INT NOT NULL DEFAULT 1, -- trang tiếp theo cần crawl
    status          TEXT NOT NULL DEFAULT 'active', -- active / exhausted
    crawl_date      DATE NOT NULL,          -- ngày logic, reset mỗi ngày mới (giữ lại lịch sử ngày cũ)
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (listing_type, property_type, crawl_date)
);

-- 2. Hàng đợi URL trang chi tiết — discovered_at (FIFO) tách biệt claimed_at
CREATE TABLE IF NOT EXISTS crawl.detail_queue (
    id                  SERIAL PRIMARY KEY,
    url                 TEXT UNIQUE NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending', -- pending/in_progress/done/failed
    discovered_at       TIMESTAMPTZ NOT NULL DEFAULT now(), -- lúc phát hiện từ listing page (cố định)
    claimed_at          TIMESTAMPTZ,                        -- lúc worker lấy ra crawl (reset khi reclaim)
    discovered_page_id  INT REFERENCES crawl.listing_progress(id),
    crawl_date          DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_detail_queue_pending_fifo
    ON crawl.detail_queue (discovered_at)
    WHERE status = 'pending';

-- 3. Trạng thái mỗi lần chạy DAG 2 — stopped_reason chỉ còn 5 giá trị
--    (max_pages/time_box/no_more_data = bình thường; fetch_error/
--    proxy_exhausted = bất thường, xem is_success() trong web_crawler_io.py)
CREATE TABLE IF NOT EXISTS crawl.run_state (
    id                SERIAL PRIMARY KEY,
    run_id            TEXT UNIQUE NOT NULL,   -- Airflow run_id
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at          TIMESTAMPTZ,
    stopped_reason    TEXT,
    detail_pages_done INT NOT NULL DEFAULT 0,
    output_s3_key     TEXT
);

-- 4. Timezone mặc định của database. Áp dụng cho toàn bộ session
--    kết nối tới postgres-dw, đảm bảo crawl_date/now() tính theo giờ HCM.
ALTER DATABASE real_estate_dw SET timezone TO 'Asia/Ho_Chi_Minh';
