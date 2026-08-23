-- =============================================================================
-- sql/002_dataset_part_state.sql
-- Control-plane cho DAG 1 (bronze_load_dataset) — tải 77 part cố định
-- (part1..part77.parquet) từ CDN lên s3://bucket/bronze/dataset/.
--
-- Số lượng part CỐ ĐỊNH (đã xác nhận: part 1–76 = 10.000 dòng/file,
-- part 77 = 4.212 dòng, KHÔNG có part 78+) -> KHÔNG cần logic dò part mới
-- (discover_new_parts) như bản nháp cũ, chỉ seed đúng 1 lần 77 dòng.
--
-- Idempotent: dùng IF NOT EXISTS + ON CONFLICT DO NOTHING, có thể chạy lại
-- nhiều lần an toàn.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS crawl;

-- Trạng thái tải từng part — dùng chung cho cả 4 chức năng A1-A4:
--   A1 probe_part_exists            -> cập nhật probed_at
--   A2 download_and_upload_part     -> cập nhật status='done', s3_key, downloaded_at
--   A3 reconcile_missing_storage_objects -> so status='done' với S3 thật, phát hiện lệch
--   A4 scan_and_fill_gaps           -> tìm status IN ('pending','failed') để tải lại
CREATE TABLE IF NOT EXISTS crawl.dataset_part_state (
    part_number   INT PRIMARY KEY,               -- 1..77, cố định
    status        TEXT NOT NULL DEFAULT 'pending', -- pending/done/failed
    s3_key        TEXT,                          -- bronze/dataset/part=N.parquet, set khi status='done'
    probed_at     TIMESTAMPTZ,                   -- lần cuối probe_part_exists (GET + Range: bytes=0-0)
    downloaded_at TIMESTAMPTZ,                   -- lần cuối upload S3 thành công
    last_error    TEXT,                          -- lỗi gần nhất, nếu status='failed'
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed đúng 1 lần 77 dòng (1..77) — idempotent nhờ ON CONFLICT DO NOTHING,
-- chạy lại script này không tạo trùng hay reset tiến độ đã có.
INSERT INTO crawl.dataset_part_state (part_number)
SELECT generate_series(1, 77)
ON CONFLICT (part_number) DO NOTHING;

-- Truy vấn nhanh các part chưa xong (dùng bởi A4 scan_and_fill_gaps)
CREATE INDEX IF NOT EXISTS idx_dataset_part_state_pending
    ON crawl.dataset_part_state (part_number)
    WHERE status IN ('pending', 'failed');
