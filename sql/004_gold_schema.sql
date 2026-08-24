-- ============================================================================
-- 004_gold_schema.sql
-- Mục đích: Star Schema phục vụ phân tích giá/m2 theo Phường-Xã/Loại hình
--           BĐS/Thời gian. Đặt tên theo Kimball naming convention.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- ----------------------------------------------------------------------------
-- DIM_DATE - Role-playing dimension: dùng chung cho cả posted_date lẫn
-- valid_from của Fact (2 FK riêng trỏ vào cùng 1 bảng này, không tạo view).
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_date (
    date_key      INTEGER      PRIMARY KEY,   -- Format YYYYMMDD.
    full_date     DATE         NOT NULL UNIQUE,
    day           SMALLINT     NOT NULL,
    month         SMALLINT     NOT NULL,
    quarter       SMALLINT     NOT NULL,
    year          SMALLINT     NOT NULL
);

COMMENT ON TABLE gold.dim_date IS
    'Bảng ngày chuẩn Kimball. Được Fact tham chiếu 2 lần (role-playing): '
    'posted_date_key (ngày đăng tin) và price_valid_from_date_key (ngày giá bắt đầu hiệu lực). '
    'Không tạo view riêng - tầng BI tự alias khi JOIN 2 lần vào bảng này.';

-- ----------------------------------------------------------------------------
-- DIM_LOCATION - Grain: 1 tổ hợp địa chỉ duy nhất (KHÔNG phải 1 phường mới),
-- vì 1 phường mới có thể gom từ nhiều phường cũ (sáp nhập 2025).
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_location (
    location_key    BIGSERIAL     PRIMARY KEY,
    province         VARCHAR(100)  NOT NULL,   -- Dùng chung cả địa chỉ cũ/mới, hiện tại luôn 'Ho Chi Minh'.
    ward_new         VARCHAR(100),              -- Phường/Xã theo địa giới MỚI - khóa nhóm chính cho phân tích chuẩn.
    ward_old         VARCHAR(100),              -- Phường/Xã theo địa giới CŨ - cùng cấp với ward_new.
    district_old      VARCHAR(100),              -- Quận/Huyện - chỉ có ở địa chỉ cũ.
    street            VARCHAR(200),              -- Mô tả thêm, KHÔNG dùng để GROUP BY.

    CONSTRAINT uq_dim_location UNIQUE (province, ward_new, ward_old, district_old, street)
);

COMMENT ON TABLE gold.dim_location IS
    'Địa điểm. GROUP BY ward_new cho phân tích chuẩn theo địa giới mới; '
    'filter theo ward_old/district_old cho người dùng quen địa chỉ cũ. '
    'Lưu ý: UNIQUE constraint coi các giá trị NULL là khác nhau (chuẩn Postgres) - '
    'ETL cần chuẩn hóa NULL trước khi upsert để tránh sinh trùng lặp không mong muốn.';

-- ----------------------------------------------------------------------------
-- DIM_PROPERTY_TYPE
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_property_type (
    property_type_key    BIGSERIAL     PRIMARY KEY,
    property_type_name    VARCHAR(50)   NOT NULL,
    listing_type           VARCHAR(10)   NOT NULL,   -- 'sale' | 'rent'.

    CONSTRAINT uq_dim_property_type UNIQUE (property_type_name, listing_type)
);

-- ----------------------------------------------------------------------------
-- DIM_SOURCE
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_source (
    source_key    BIGSERIAL     PRIMARY KEY,
    source_name    VARCHAR(20)   NOT NULL,   -- 'dataset_cdn' | 'web_crawl'.
    source_part     VARCHAR(50)   NOT NULL,

    CONSTRAINT uq_dim_source UNIQUE (source_name, source_part)
);

-- ----------------------------------------------------------------------------
-- DIM_PROPERTY_FEATURES - Junk Dimension: chỉ gồm trường categorical/flag,
-- KHÔNG gồm bedrooms/floors (numeric, có thể range-filter trực tiếp trên Fact).
-- Dùng MD5 hash làm key, nhất quán cách làm SCD2 ở Silver.
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_property_features (
    feature_key       CHAR(32)      PRIMARY KEY,   -- MD5 trên 7 trường bên dưới.
    orientation        VARCHAR(20),
    legal_status        VARCHAR(50),
    has_dining_room     BOOLEAN,
    has_kitchen         BOOLEAN,
    has_rooftop         BOOLEAN,
    has_car_parking      BOOLEAN,
    owner_direct        BOOLEAN
);

COMMENT ON TABLE gold.dim_property_features IS
    'Junk dimension cho 7 trường categorical/flag tỷ lệ null cao hoặc giá trị lệch. '
    'Không gồm bedrooms/floors vì chúng cần lọc theo khoảng giá trị, phù hợp để thẳng trên Fact hơn.';

-- ----------------------------------------------------------------------------
-- FACT_LISTING_PRICE
-- Grain: 1 dòng = 1 phiên bản giá của 1 tin đăng (kế thừa SCD2 từ Silver).
-- ----------------------------------------------------------------------------
CREATE TABLE gold.fact_listing_price (
    listing_key       BIGINT        PRIMARY KEY,   -- Map thẳng từ staging.listing_history.listing_key.
    listing_id         BIGINT        NOT NULL,       -- Degenerate dimension, trace ngược về Silver/Bronze.
    listing_url         TEXT          NOT NULL,

    -- Khóa ngoại tới các dimension
    location_key         BIGINT   NOT NULL REFERENCES gold.dim_location (location_key),
    property_type_key     BIGINT   NOT NULL REFERENCES gold.dim_property_type (property_type_key),
    feature_key           CHAR(32) NOT NULL REFERENCES gold.dim_property_features (feature_key),
    source_key             BIGINT   NOT NULL REFERENCES gold.dim_source (source_key),

    -- Role-playing date: 2 FK cùng trỏ vào gold.dim_date
    posted_date_key             INTEGER NOT NULL REFERENCES gold.dim_date (date_key), -- Ngày đăng tin.
    price_valid_from_date_key    INTEGER NOT NULL REFERENCES gold.dim_date (date_key), -- Ngày giá bắt đầu hiệu lực.

    -- Chi tiết SCD2 mức độ timestamp (chính xác hơn date_key, phục vụ point-in-time).
    valid_from        TIMESTAMPTZ  NOT NULL,
    valid_to          TIMESTAMPTZ,
    is_current        BOOLEAN      NOT NULL,

    -- Measure chính
    price_vnd            NUMERIC(16, 0) NOT NULL,
    price_per_m2_vnd       NUMERIC(16, 2),           -- Copy từ Silver, KHÔNG tính lại.
    area_m2                NUMERIC(10, 2),

    -- Degenerate measure/attribute
    bedrooms              SMALLINT,
    floors                SMALLINT,
    length_m              NUMERIC(6, 2),
    width_m                NUMERIC(6, 2),
    street_width_m          NUMERIC(6, 2),

    -- Có lọc
    price_is_negotiable      BOOLEAN NOT NULL,
    area_is_undetermined      BOOLEAN NOT NULL,
    has_warning              BOOLEAN NOT NULL,
    is_expired               BOOLEAN NOT NULL
);

COMMENT ON TABLE gold.fact_listing_price IS
    'Fact chính. Grain: 1 dòng = 1 phiên bản giá (SCD2). '
    'Khi tính AVG(price_per_m2_vnd) phải lọc price_is_negotiable=false '
    'để tránh dữ liệu thỏa thuận (price_vnd=0) làm lệch kết quả.';
COMMENT ON COLUMN gold.fact_listing_price.posted_date_key IS
    'Ngày đăng tin — dùng cho xu hướng theo thời điểm rao tin.';
COMMENT ON COLUMN gold.fact_listing_price.price_valid_from_date_key IS
    'Ngày phiên bản giá bắt đầu hiệu lực — dùng cho giá thị trường tại từng thời điểm.';

-- Index phục vụ truy vấn chính: giá theo Phường-Xã/Loại hình/Thời gian
CREATE INDEX idx_fact_location            ON gold.fact_listing_price (location_key);
CREATE INDEX idx_fact_property_type       ON gold.fact_listing_price (property_type_key);
CREATE INDEX idx_fact_posted_date         ON gold.fact_listing_price (posted_date_key);
CREATE INDEX idx_fact_price_valid_date    ON gold.fact_listing_price (price_valid_from_date_key);
CREATE INDEX idx_fact_listing_id          ON gold.fact_listing_price (listing_id);
CREATE INDEX idx_fact_current             ON gold.fact_listing_price (is_current) WHERE is_current;
