-- ============================================================================
-- 003_silver_listing_history.sql  (REVISED — v2, sau buổi thiết kế ETL Bronze→Silver)
-- Mục đích: Bảng lịch sử tin đăng (Silver layer), áp dụng SCD Type 2 trên
--           5 trường biến động (giá, diện_tích, trạng_thái) qua row_hash.
--           Đặt tên theo sqlstyle.guide/vn (không phải bảng dim/fact).
--
-- THAY ĐỔI so với v1:
--   1. price_vnd: bỏ NOT NULL, dùng NULL khi price_is_negotiable=TRUE thay vì 0
--      (0 làm nhiễu AVG/thống kê ở Gold; NULL để Postgres tự loại trừ khi AVG()).
--      Thêm CHECK ràng buộc tính nhất quán giữa 2 trường này.
--   2. row_hash: đổi từ giá trị ứng dụng tự tính sang GENERATED ALWAYS STORED
--      (giống price_per_m2_vnd) — 1 nguồn sự thật duy nhất cho logic hash,
--      Python parser không cần tự tính lại nữa.
--   3. Thêm source_bronze_key: lưu đúng S3 key nguồn (thay vì chỉ nhãn chung
--      chung ở source_part) để truy vết 1 version Silver đến từ file Bronze nào.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS silver;

-- ----------------------------------------------------------------------------
-- Hàm dùng CHUNG cho công thức row_hash — 1 NGUỒN SỰ THẬT DUY NHẤT.
-- Cả silver.listing_history VÀ silver.listing_staging_batch đều gọi hàm này
-- trong cột GENERATED của mình. Spark KHÔNG cần tự tính MD5 nữa — chỉ ghi
-- các trường thô (price_vnd, area_m2...), Postgres tự tính hash khi INSERT.
-- IMMUTABLE bắt buộc phải có để dùng được trong biểu thức GENERATED.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION silver.compute_row_hash(
    p_price_vnd            NUMERIC,
    p_price_is_negotiable   BOOLEAN,
    p_is_expired            BOOLEAN,
    p_has_warning           BOOLEAN,
    p_area_m2               NUMERIC
) RETURNS CHAR(32)
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT MD5(
        COALESCE(p_price_vnd::TEXT, 'NULL') || '|' ||
        p_price_is_negotiable::TEXT || '|' ||
        p_is_expired::TEXT || '|' ||
        p_has_warning::TEXT || '|' ||
        COALESCE(p_area_m2::TEXT, 'NULL')
    )
$$;

COMMENT ON FUNCTION silver.compute_row_hash IS
    'Công thức row_hash DUY NHẤT cho toàn bộ Silver layer. listing_history và '
    'listing_staging_batch đều gọi hàm này trong cột GENERATED — sửa logic hash '
    'chỉ cần sửa 1 chỗ. Spark parser KHÔNG tự tính MD5.';

CREATE TABLE silver.listing_history (
    -- --- Khóa và quản lý SCD Type 2 ---
    listing_key       BIGSERIAL PRIMARY KEY,
    listing_id        BIGINT       NOT NULL,          -- Natural key, trích từ URL (vd: -12345678.html).
    listing_url       TEXT         NOT NULL,
    source_part       VARCHAR(50)  NOT NULL,          -- 'dataset:part_N' hoặc 'web' — nhãn nguồn, giữ cho tương thích ngược.
    source_bronze_key TEXT         NOT NULL,          -- S3 key CHÍNH XÁC của file Bronze sinh ra version này (lineage).
    crawl_date        TIMESTAMPTZ  NOT NULL,          -- Mốc nguồn gốc: lần crawl thực tế sinh ra bản ghi này.
    valid_from        TIMESTAMPTZ  NOT NULL,          -- Mốc SCD2: phiên bản này bắt đầu hiệu lực.
    valid_to          TIMESTAMPTZ,                    -- NULL nếu là phiên bản hiện tại.
    is_current        BOOLEAN      NOT NULL DEFAULT TRUE,
    last_seen_at      TIMESTAMPTZ  NOT NULL,          -- Lần crawl gần nhất xác nhận phiên bản này vẫn đúng (hash không đổi).

    -- --- Nội dung tĩnh (Type 1, không tham gia SCD2) ---
    title             TEXT         NOT NULL,
    listing_type      VARCHAR(10)  NOT NULL,          -- 'sale' | 'rent'.
    property_type     VARCHAR(50)  NOT NULL,
    posted_date       DATE         NOT NULL,          -- Ngày đăng tin gốc, lấy từ attr datetime (KHÔNG lấy text hiển thị).

    -- --- Giá (theo dõi SCD2) ---
    -- price_vnd = NULL khi price_is_negotiable = TRUE (KHÔNG dùng 0 — 0 làm nhiễu
    -- AVG/SUM ở Gold; để NULL thì AVG() tự loại trừ, không cần filter thủ công).
    price_vnd             NUMERIC(16, 0),
    price_raw             TEXT,                        -- Giữ nguyên text gốc để audit ("2.5 tỷ", "1,9 tỷ /tháng", "Thỏa thuận").
    price_is_negotiable    BOOLEAN NOT NULL DEFAULT FALSE,

    -- --- Diện tích (theo dõi SCD2) ---
    area_m2                NUMERIC(10, 2),             -- Qua parse_vn_number(), xử lý case "." là hàng nghìn khi >1000.
    area_raw                TEXT,                        -- Giữ nguyên kể cả text "KXD".
    area_is_undetermined     BOOLEAN NOT NULL DEFAULT FALSE,

    -- --- Giá/m2: tính sẵn một lần duy nhất, Gold sẽ copy sang không tính lại ---
    price_per_m2_vnd    NUMERIC(16, 2)
        GENERATED ALWAYS AS (
            CASE
                WHEN area_is_undetermined OR area_m2 IS NULL OR area_m2 = 0
                     OR price_vnd IS NULL THEN NULL
                ELSE ROUND(price_vnd / area_m2, 2)
            END
        ) STORED,

    -- --- Kích thước vật lý ---
    length_m           NUMERIC(6, 2),
    width_m             NUMERIC(6, 2),
    street_width_m       NUMERIC(6, 2),
    floors              SMALLINT,
    bedrooms            SMALLINT,

    -- --- Đặc điểm ---
    orientation         VARCHAR(20),                  -- Ký hiệu thiếu "_" -> NULL.
    legal_status         VARCHAR(50),

    -- --- Tiện ích: true = có icon check; NULL = ký hiệu thiếu (_/-/--/---); false thật chưa từng quan sát ---
    has_dining_room       BOOLEAN,
    has_kitchen           BOOLEAN,
    has_rooftop           BOOLEAN,
    has_car_parking        BOOLEAN,
    owner_direct          BOOLEAN,

    -- --- Trạng thái tin (theo dõi SCD2) ---
    is_expired           BOOLEAN NOT NULL DEFAULT FALSE,
    has_warning           BOOLEAN NOT NULL DEFAULT FALSE,

    -- --- Địa chỉ mới (sau sáp nhập) ---
    address_street_new      VARCHAR(200),
    address_ward_new        VARCHAR(100),
    address_province_new     VARCHAR(100),

    -- --- Địa chỉ cũ (trước sáp nhập) - parse từ address_old_raw ---
    address_old_raw           TEXT,
    address_ward_old          VARCHAR(100),              -- Phường/Xã cũ, cùng cấp với address_ward_new.
    address_district_old       VARCHAR(100),              -- Quận/Huyện, chỉ có ở địa chỉ cũ.

    -- --- SCD2 row_hash: GENERATED, gọi hàm dùng chung silver.compute_row_hash() ---
    row_hash          CHAR(32)
        GENERATED ALWAYS AS (
            silver.compute_row_hash(price_vnd, price_is_negotiable, is_expired, has_warning, area_m2)
        ) STORED,

    -- --- Ràng buộc toàn vẹn: price_vnd và price_is_negotiable phải nhất quán ---
    CONSTRAINT chk_price_negotiable_null CHECK (
        (price_is_negotiable = TRUE  AND price_vnd IS NULL)
        OR
        (price_is_negotiable = FALSE AND price_vnd IS NOT NULL)
    ),

    -- --- Audit ---
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now()
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
