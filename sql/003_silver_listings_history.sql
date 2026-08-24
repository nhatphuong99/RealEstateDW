-- ============================================================================
-- 003_silver_listing_history.sql (REVISED v2)
-- Bảng lịch sử tin đăng (Silver layer), SCD Type 2 trên 5 trường biến động.
-- Idempotent.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS silver;

-- Hàm tính row_hash duy nhất cho Silver layer (IMMUTABLE, dùng trong GENERATED).
CREATE OR REPLACE FUNCTION silver.compute_row_hash(
    p_price_vnd NUMERIC,
    p_price_is_negotiable BOOLEAN,
    p_is_expired BOOLEAN,
    p_has_warning BOOLEAN,
    p_area_m2 NUMERIC
) RETURNS CHAR(32)
LANGUAGE sql IMMUTABLE AS $$
    SELECT MD5(
        COALESCE(p_price_vnd::TEXT, 'NULL') || '|' ||
        p_price_is_negotiable::TEXT || '|' ||
        p_is_expired::TEXT || '|' ||
        p_has_warning::TEXT || '|' ||
        COALESCE(p_area_m2::TEXT, 'NULL')
    )
$$;

COMMENT ON FUNCTION silver.compute_row_hash IS
    'Công thức row_hash duy nhất cho Silver layer, dùng chung cho listing_history và staging_batch.';

CREATE TABLE silver.listing_history (
    -- Khóa & SCD2
    listing_key BIGSERIAL PRIMARY KEY,
    listing_id BIGINT NOT NULL,
    listing_url TEXT NOT NULL,
    source_part VARCHAR(50) NOT NULL,
    source_bronze_key TEXT NOT NULL,
    crawl_date TIMESTAMPTZ NOT NULL,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    last_seen_at TIMESTAMPTZ NOT NULL,

    -- Nội dung tĩnh
    title TEXT NOT NULL,
    listing_type VARCHAR(10) NOT NULL,
    property_type VARCHAR(50) NOT NULL,
    posted_date DATE NOT NULL,

    -- Giá (SCD2)
    price_vnd NUMERIC(16,0),
    price_raw TEXT,
    price_is_negotiable BOOLEAN NOT NULL DEFAULT FALSE,

    -- Diện tích (SCD2)
    area_m2 NUMERIC(10,2),
    area_raw TEXT,
    area_is_undetermined BOOLEAN NOT NULL DEFAULT FALSE,

    -- Giá/m2 (GENERATED)
    price_per_m2_vnd NUMERIC(16,2)
        GENERATED ALWAYS AS (
            CASE WHEN area_is_undetermined OR area_m2 IS NULL OR area_m2=0 OR price_vnd IS NULL
                 THEN NULL ELSE ROUND(price_vnd/area_m2,2) END
        ) STORED,

    -- Kích thước
    length_m NUMERIC(6,2),
    width_m NUMERIC(6,2),
    street_width_m NUMERIC(6,2),
    floors SMALLINT,
    bedrooms SMALLINT,

    -- Đặc điểm
    orientation VARCHAR(20),
    legal_status VARCHAR(50),

    -- Tiện ích
    has_dining_room BOOLEAN,
    has_kitchen BOOLEAN,
    has_rooftop BOOLEAN,
    has_car_parking BOOLEAN,
    owner_direct BOOLEAN,

    -- Trạng thái (SCD2)
    is_expired BOOLEAN NOT NULL DEFAULT FALSE,
    has_warning BOOLEAN NOT NULL DEFAULT FALSE,

    -- Địa chỉ mới
    address_street_new VARCHAR(200),
    address_ward_new VARCHAR(100),
    address_province_new VARCHAR(100),

    -- Địa chỉ cũ
    address_old_raw TEXT,
    address_ward_old VARCHAR(100),
    address_district_old VARCHAR(100),

    -- row_hash (GENERATED)
    row_hash CHAR(32)
        GENERATED ALWAYS AS (
            silver.compute_row_hash(price_vnd, price_is_negotiable, is_expired, has_warning, area_m2)
        ) STORED,

    -- Ràng buộc giá
    CONSTRAINT chk_price_negotiable_null CHECK (
        (price_is_negotiable=TRUE AND price_vnd IS NULL)
        OR (price_is_negotiable=FALSE AND price_vnd IS NOT NULL)
    ),

    -- Audit
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE silver.listing_history IS
    'Lịch sử tin đăng BĐS (Silver layer). Grain: 1 dòng = 1 phiên bản giá của 1 tin đăng. '
    'SCD Type 2 áp dụng trên 5 trường: price_vnd, price_is_negotiable, is_expired, has_warning, area_m2.';

COMMENT ON COLUMN silver.listing_history.price_vnd IS
    'NULL khi price_is_negotiable=TRUE (KHÔNG dùng 0 — xem CHECK constraint chk_price_negotiable_null). '
    'NULL giúp AVG()/SUM() ở Gold tự loại trừ tin thỏa thuận mà không cần filter thủ công.';
COMMENT ON COLUMN silver.listing_history.row_hash IS
    'GENERATED STORED, gọi silver.compute_row_hash() — 1 nguồn sự thật duy nhất, dùng chung với '
    'silver.listing_staging_batch. Parser Spark/Python KHÔNG tự tính MD5.';
COMMENT ON COLUMN silver.listing_history.source_bronze_key IS
    'S3 key chính xác của file Bronze sinh ra version này — dùng để debug/truy vết khi SCD2 merge sai.';
COMMENT ON COLUMN silver.listing_history.crawl_date IS
    'Mốc nguồn (Bronze): thời điểm HTML thực tế được crawl về. Khác với valid_from (mốc nghiệp vụ SCD2).';
COMMENT ON COLUMN silver.listing_history.valid_from IS
    'Mốc SCD2: = crawl_date của lần crawl đầu tiên phát hiện ra tổ hợp giá trị (row_hash) này.';
COMMENT ON COLUMN silver.listing_history.last_seen_at IS
    'Cập nhật mỗi lần crawl lại mà row_hash không đổi - KHÔNG sinh phiên bản mới, chỉ update tại chỗ trên bản hiện tại.';

-- Đảm bảo mỗi listing_id chỉ có đúng 1 phiên bản is_current = true.
CREATE UNIQUE INDEX ux_listing_history_current
    ON silver.listing_history (listing_id)
    WHERE is_current;

-- Hỗ trợ bước MERGE: tra cứu nhanh bản ghi hiện tại của 1 listing_id để so sánh row_hash.
CREATE INDEX idx_listing_history_id_current
    ON silver.listing_history (listing_id)
    WHERE is_current;

-- Hỗ trợ truy vấn toàn bộ lịch sử của 1 listing_id (sắp xếp theo thời gian) —
-- cũng là index chính dùng bởi bước MERGE SCD2 (window function LAG theo crawl_date).
CREATE INDEX idx_listing_history_id_valid_from
    ON silver.listing_history (listing_id, valid_from);
