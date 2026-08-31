-- ============================================================================
-- sql/003_silver_listings_history.sql
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
    'Công thức row_hash duy nhất, dùng chung cho listing_history và staging_batch.';

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
    area_is_outlier BOOLEAN NOT NULL DEFAULT FALSE,   -- area_m2 gốc > 10.000m2 hoặc < 3m2, đã null hóa

    -- Giá/m2 (GENERATED)
    price_per_m2_vnd NUMERIC(15,2)
        GENERATED ALWAYS AS (
            CASE WHEN area_is_undetermined OR area_m2 IS NULL OR area_m2=0 OR price_vnd IS NULL
                 THEN NULL ELSE ROUND(price_vnd/area_m2,2) END
        ) STORED,

    -- TRUE khi price_vnd/area_m2 > 5 tỷ/m2. price_vnd GIỮ NGUYÊN (không null hóa)
    -- — không làm GENERATED vì phụ thuộc area_m2 đã sanitize, không tự suy lại được.
    price_is_outlier BOOLEAN NOT NULL DEFAULT FALSE,

    -- Kích thước (đã sanitize ở tầng parser: null hóa nếu ngoài ngưỡng hợp lý vật lý)
    length_m NUMERIC(6,2),
    width_m NUMERIC(6,2),
    street_width_m NUMERIC(6,2),
    floors SMALLINT,
    bedrooms SMALLINT,

    -- Đặc điểm (chuỗi tự do, không enum cứng — "" khi thiếu, xem quy ước bên dưới)
    orientation VARCHAR(20) NOT NULL DEFAULT '',
    legal_status VARCHAR(50) NOT NULL DEFAULT '',

    -- Tiện ích (tri-state: TRUE = có icon check | NULL = không xác định, site không có ký hiệu phủ định)
    has_dining_room BOOLEAN,
    has_kitchen BOOLEAN,
    has_rooftop BOOLEAN,
    has_car_parking BOOLEAN,
    owner_direct BOOLEAN,

    -- Trạng thái (SCD2)
    is_expired BOOLEAN NOT NULL DEFAULT FALSE,
    has_warning BOOLEAN NOT NULL DEFAULT FALSE,

    -- Địa chỉ mới
    address_street_new VARCHAR(200) NOT NULL DEFAULT '',
    address_ward_new VARCHAR(100) NOT NULL DEFAULT '',
    address_province_new VARCHAR(100) NOT NULL DEFAULT '',

    -- Địa chỉ cũ (address_old_raw chỉ audit ở Silver, không đưa xuống Gold)
    address_old_raw TEXT NOT NULL DEFAULT '',
    address_ward_old VARCHAR(100) NOT NULL DEFAULT '',
    address_district_old VARCHAR(100) NOT NULL DEFAULT '',
    address_province_old VARCHAR(100) NOT NULL DEFAULT '',   -- có thể khác address_province_new, xem comment cột

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
    'Lịch sử tin đăng BĐS. Grain: 1 dòng = 1 phiên bản giá của 1 tin đăng. '
    'SCD Type 2 trên 5 trường: price_vnd, price_is_negotiable, is_expired, has_warning, area_m2.';

COMMENT ON COLUMN silver.listing_history.price_vnd IS
    'NULL khi price_is_negotiable=TRUE (không dùng 0) — giúp AVG()/SUM() tự loại tin thỏa thuận.';
COMMENT ON COLUMN silver.listing_history.price_is_outlier IS
    'TRUE khi giá/m2 (đã sanitize) > 5 tỷ/m2 (benchmark: căn hộ cao cấp ~55-85tr/m2, đất trung tâm Q1 ~1-2 tỷ/m2). '
    'Không null hóa price_vnd — lọc bằng cờ này ở Gold/dashboard.';
COMMENT ON COLUMN silver.listing_history.area_is_outlier IS
    'TRUE khi area_m2 gốc > 10.000m2 hoặc < 3m2 (đã null hóa ở parser _sanitize_area).';
COMMENT ON COLUMN silver.listing_history.row_hash IS
    'GENERATED STORED qua silver.compute_row_hash() — nguồn sự thật duy nhất, Python/Spark không tự tính MD5.';
COMMENT ON COLUMN silver.listing_history.source_bronze_key IS
    'S3 key của file Bronze sinh ra version này — dùng debug/truy vết khi SCD2 merge sai.';
COMMENT ON COLUMN silver.listing_history.address_province_old IS
    'Tỉnh/thành theo địa giới CŨ, có thể khác address_province_new khi khu vực bị sáp nhập. '
    'KHÔNG dùng cột này để lọc scope — is_in_scope() chỉ dùng address_province_new.';
COMMENT ON COLUMN silver.listing_history.crawl_date IS
    'Mốc nguồn (Bronze): thời điểm HTML được crawl. Khác valid_from (mốc nghiệp vụ SCD2).';
COMMENT ON COLUMN silver.listing_history.valid_from IS
    'Mốc SCD2 = crawl_date lần đầu phát hiện tổ hợp row_hash này. Dùng làm observed_date_key ở Gold.';
COMMENT ON COLUMN silver.listing_history.valid_to IS
    'NULL ở đa số dòng — nghĩa là "chưa từng crawl lại lần 2 để xác nhận thay đổi" '
    '(pipeline.detail_queue chỉ enqueue mỗi URL 1 lần), KHÔNG có nghĩa "còn hiệu lực tới hiện tại".';
COMMENT ON COLUMN silver.listing_history.last_seen_at IS
    'Cập nhật mỗi lần crawl lại mà row_hash không đổi — không sinh version mới.';

-- Quy ước cột CHUỖI: NOT NULL DEFAULT '' khi thiếu (áp dụng orientation/legal_status/address_*).
-- Ràng buộc khai báo thẳng ở DDL để lỗi lộ ngay tại Bronze->Silver thay vì trôi xuống Gold
-- rồi làm rớt dòng khỏi JOIN ('' = NULL luôn UNKNOWN trong SQL). Cột SỐ/BOOLEAN nullable
-- không theo quy ước này — giữ NULL đúng nghĩa "thiếu"/"không xác định".

-- Đảm bảo mỗi listing_id chỉ có đúng 1 phiên bản is_current = true.
CREATE UNIQUE INDEX ux_listing_history_current
    ON silver.listing_history (listing_id)
    WHERE is_current;

-- Hỗ trợ bước MERGE: tra cứu nhanh bản ghi hiện tại của 1 listing_id.
CREATE INDEX idx_listing_history_id_current
    ON silver.listing_history (listing_id)
    WHERE is_current;

-- Hỗ trợ truy vấn lịch sử + là index chính cho MERGE SCD2 (window LAG theo crawl_date).
CREATE INDEX idx_listing_history_id_valid_from
    ON silver.listing_history (listing_id, valid_from);
