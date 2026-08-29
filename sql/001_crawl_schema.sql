-- =============================================================================
-- sql/001_crawl_schema.sql
-- Control-plane cho DAG 2 (web_crawler) — crawl alonhadat.com.vn theo queue.
-- Idempotent: dùng IF NOT EXISTS, chạy lại nhiều lần an toàn.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS crawl;

-- 1. Con trỏ crawl trang danh sách (listing_progress)
CREATE TABLE IF NOT EXISTS crawl.listing_progress (
    id              SERIAL PRIMARY KEY,
    listing_type    TEXT NOT NULL,          -- can-ban / cho-thue
    property_type   TEXT NOT NULL,          -- 5 loại BĐS
    current_page    INT NOT NULL DEFAULT 1, -- trang tiếp theo
    status          TEXT NOT NULL DEFAULT 'active', -- active / exhausted
    crawl_date      DATE NOT NULL,          -- reset mỗi ngày
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (listing_type, property_type, crawl_date)
);

-- 2. Hàng đợi URL chi tiết (detail_queue)
CREATE TABLE IF NOT EXISTS crawl.detail_queue (
    id                  SERIAL PRIMARY KEY,
    url                 TEXT UNIQUE NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending', -- pending/processing/done/failed
    discovered_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at          TIMESTAMPTZ,
    discovered_page_id  INT REFERENCES crawl.listing_progress(id),
    crawl_date          DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_detail_queue_pending_fifo
    ON crawl.detail_queue (discovered_at)
    WHERE status = 'pending';

-- 3. Trạng thái mỗi lần chạy DAG 2 (run_state)
CREATE TABLE IF NOT EXISTS crawl.run_state (
    id                SERIAL PRIMARY KEY,
    run_id            TEXT UNIQUE NOT NULL,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at          TIMESTAMPTZ,
    stopped_reason    TEXT,
    detail_pages_done INT NOT NULL DEFAULT 0,
    output_s3_key     TEXT
);

-- 4. Timezone mặc định (Asia/Ho_Chi_Minh)
ALTER DATABASE real_estate_dw SET timezone TO 'Asia/Ho_Chi_Minh';
