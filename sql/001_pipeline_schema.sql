-- =============================================================================
-- sql/001_pipeline_schema.sql
-- Control-plane cho DAG 2 (web_crawler) — crawl alonhadat.com.vn theo queue.
-- Idempotent: dùng IF NOT EXISTS, chạy lại nhiều lần an toàn.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS pipeline;

-- 1. Con trỏ crawl trang danh sách (listing_progress)
CREATE TABLE IF NOT EXISTS pipeline.listing_progress (
    id              SERIAL PRIMARY KEY,
    province_old    TEXT NOT NULL,          -- ho-chi-minh / ba-ria-vung-tau / binh-duong
    listing_type    TEXT NOT NULL,          -- can-ban / cho-thue
    property_type   TEXT NOT NULL,          -- 5 loại BĐS
    current_page    INT NOT NULL DEFAULT 1, -- trang tiếp theo
    status          TEXT NOT NULL DEFAULT 'active', -- active / exhausted
    crawl_date      DATE NOT NULL,          -- reset mỗi ngày
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (province_old, listing_type, property_type, crawl_date)
);

-- 2. Hàng đợi URL chi tiết (detail_queue)
CREATE TABLE IF NOT EXISTS pipeline.detail_queue (
    id                  SERIAL PRIMARY KEY,
    url                 TEXT UNIQUE NOT NULL,
    -- pending -> processing -> fetched -> flushed -> done (thành công)
    --                   |            |         |
    --                   +------------+---------+--> failed (hết retry/proxy)
    -- fetched = đã fetch HTML xong, đang chờ tới lượt flush lên S3 (RAM only)
    -- flushed = đã ghi an toàn lên S3 (.inprogress), chưa chắc có bản final
    -- done    = đã nằm trong file .parquet final -> Silver đọc được
    status              TEXT NOT NULL DEFAULT 'pending',
    discovered_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at          TIMESTAMPTZ,
    discovered_page_id  INT REFERENCES pipeline.listing_progress(id),
    crawl_date          DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_detail_queue_pending_fifo
    ON pipeline.detail_queue (discovered_at)
    WHERE status = 'pending';

-- 3. Trạng thái mỗi lần chạy DAG 2 (run_state)
CREATE TABLE IF NOT EXISTS pipeline.run_state (
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
