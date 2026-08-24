-- =============================================================================
-- sql/002_dataset_part_state.sql
-- Control-plane cho DAG 1 (bronze_load_dataset) — tải 77 part cố định từ CDN lên S3.
-- Số lượng part cố định (1–77) -> không cần logic dò part mới, chỉ seed 1 lần.
-- Idempotent: IF NOT EXISTS + ON CONFLICT DO NOTHING.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS crawl;

-- Trạng thái tải từng part (dùng cho A1–A4)
CREATE TABLE IF NOT EXISTS crawl.dataset_part_state (
    part_number   INT PRIMARY KEY,               -- 1..77
    status        TEXT NOT NULL DEFAULT 'pending', -- pending/done/failed
    s3_key        TEXT,                          -- set khi status='done'
    probed_at     TIMESTAMPTZ,                   -- lần cuối probe
    downloaded_at TIMESTAMPTZ,                   -- lần cuối upload S3
    last_error    TEXT,                          -- lỗi gần nhất nếu failed
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed 77 dòng (1..77), idempotent
INSERT INTO crawl.dataset_part_state (part_number)
SELECT generate_series(1, 77)
ON CONFLICT (part_number) DO NOTHING;

-- Index cho part chưa xong (pending/failed)
CREATE INDEX IF NOT EXISTS idx_dataset_part_state_pending
    ON crawl.dataset_part_state (part_number)
    WHERE status IN ('pending', 'failed');
