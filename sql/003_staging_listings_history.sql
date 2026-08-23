-- ============================================================================
-- 003_staging_listing_history.sql
-- Mục đích: Bảng lịch sử tin đăng (Silver layer), áp dụng SCD Type 2 trên
--           5 trường biến động (giá, diện_tích, trạng_thái) qua row_hash.
--           Đặt tên theo sqlstyle.guide/vn (không phải bảng dim/fact).
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE staging.listing_history (
    -- --- Khóa và quản lý SCD Type 2 ---
    listing_key       BIGSERIAL PRIMARY KEY,
    listing_id        BIGINT       NOT NULL,          -- Natural key, trích từ URL (vd: -12345678.html).
    listing_url       TEXT         NOT NULL,
    source_part       VARCHAR(50)  NOT NULL,          -- 'dataset:part_N' hoặc 'web'.
    crawl_date        TIMESTAMPTZ  NOT NULL,          -- Mốc nguồn gốc: lần crawl thực tế sinh ra bản ghi này.
    valid_from        TIMESTAMPTZ  NOT NULL,          -- Mốc SCD2: phiên bản này bắt đầu hiệu lực.
    valid_to          TIMESTAMPTZ,                    -- NULL nếu là phiên bản hiện tại.
    is_current        BOOLEAN      NOT NULL DEFAULT TRUE,
    row_hash          CHAR(32)     NOT NULL,          -- MD5 trên 5 trường theo dõi (xem chú thích cuối file).
    last_seen_at      TIMESTAMPTZ  NOT NULL,          -- Lần crawl gần nhất xác nhận phiên bản này vẫn đúng (hash không đổi).

    -- --- Nội dung tĩnh (Type 1, không tham gia SCD2) ---
    title             TEXT         NOT NULL,
    listing_type      VARCHAR(10)  NOT NULL,          -- 'sale' | 'rent'.
    property_type     VARCHAR(50)  NOT NULL,
    posted_date       DATE         NOT NULL,          -- Ngày đăng tin gốc, lấy từ attr datetime (KHÔNG lấy text hiển thị).

    -- --- Giá (theo dõi SCD2) ---
    price_vnd            NUMERIC(16, 0) NOT NULL,     -- Lấy thẳng từ attr value, đã là số VND sạch.
    price_raw             TEXT,                        -- Giữ nguyên text gốc để audit ("2.5 tỷ", "1,9 tỷ /tháng").
    price_is_negotiable    BOOLEAN NOT NULL DEFAULT FALSE, -- value="0" + text "Thỏa thuận".

    -- --- Diện tích (theo dõi SCD2) ---
    area_m2                NUMERIC(10, 2),             -- Qua parse_vn_number(), xử lý case "." là hàng nghìn khi >1000.
    area_raw                TEXT,                        -- Giữ nguyên kể cả text "KXD".
    area_is_undetermined     BOOLEAN NOT NULL DEFAULT FALSE,

    -- --- Giá/m2: tính sẵn một lần duy nhất, Gold sẽ copy sang không tính lại ---
    price_per_m2_vnd    NUMERIC(16, 2)
        GENERATED ALWAYS AS (
            CASE
                WHEN area_is_undetermined OR area_m2 IS NULL OR area_m2 = 0 THEN NULL
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

    -- --- Audit ---
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE staging.listing_history IS
    'Lịch sử tin đăng BĐS (Silver layer). Grain: 1 dòng = 1 phiên bản giá của 1 tin đăng. '
    'SCD Type 2 áp dụng trên 5 trường: price_vnd, price_is_negotiable, is_expired, has_warning, area_m2.';

COMMENT ON COLUMN staging.listing_history.crawl_date IS
    'Mốc nguồn (Bronze): thời điểm HTML thực tế được crawl về. Khác với valid_from (mốc nghiệp vụ SCD2).';
COMMENT ON COLUMN staging.listing_history.valid_from IS
    'Mốc SCD2: = crawl_date của lần crawl đầu tiên phát hiện ra tổ hợp giá trị (row_hash) này.';
COMMENT ON COLUMN staging.listing_history.last_seen_at IS
    'Cập nhật mỗi lần crawl lại mà row_hash không đổi - KHÔNG sinh phiên bản mới, chỉ update tại chỗ trên bản hiện tại.';

-- Đảm bảo mỗi listing_id chỉ có đúng 1 phiên bản is_current = true.
CREATE UNIQUE INDEX ux_listing_history_current
    ON staging.listing_history (listing_id)
    WHERE is_current;

-- Hỗ trợ bước MERGE: tra cứu nhanh bản ghi hiện tại của 1 listing_id để so sánh row_hash.
CREATE INDEX idx_listing_history_id_current
    ON staging.listing_history (listing_id)
    WHERE is_current;

-- Hỗ trợ truy vấn toàn bộ lịch sử của 1 listing_id (sắp xếp theo thời gian).
CREATE INDEX idx_listing_history_id_valid_from
    ON staging.listing_history (listing_id, valid_from);
