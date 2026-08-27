-- ============================================================================
-- 005_gold_schema.sql
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
-- DIM_LOCATION - Grain: 1 tổ hợp địa chỉ duy nhất (KHÔNG phải 1 phường mới).
--
-- QUY ƯỚC: các cột chuỗi dưới đây NOT NULL DEFAULT '' — Silver layer đảm bảo 
-- KHÔNG BAO GIỜ trả NULL cho trường kiểu chuỗi -> UPSERT (INSERT ... ON CONFLICT) 
-- hoạt động idempotent đúng nghĩa — không cần bước chuẩn hóa NULL riêng ở tầng ETL Gold.
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_location (
    location_key    BIGSERIAL     PRIMARY KEY,
    province         VARCHAR(100)  NOT NULL DEFAULT '',   -- Hiện tại luôn 'Hồ Chí Minh'.
    ward_new         VARCHAR(100)  NOT NULL DEFAULT '',   -- Phường/Xã theo địa giới MỚI - khóa nhóm chính.
    ward_old         VARCHAR(100)  NOT NULL DEFAULT '',   -- Phường/Xã theo địa giới CŨ.
    district_old      VARCHAR(100)  NOT NULL DEFAULT '',   -- Quận/Huyện - chỉ có ở địa chỉ cũ.
    street            VARCHAR(200)  NOT NULL DEFAULT '',   -- Mô tả thêm, KHÔNG dùng để GROUP BY.

    CONSTRAINT uq_dim_location UNIQUE (province, ward_new, ward_old, district_old, street)
);

COMMENT ON TABLE gold.dim_location IS
    'Địa điểm. GROUP BY ward_new cho phân tích chuẩn theo địa giới mới; '
    'filter theo ward_old/district_old cho người dùng quen địa chỉ cũ. '
    'Tất cả cột chuỗi NOT NULL DEFAULT '''' (kế thừa quy ước từ Silver) — '
    'UNIQUE constraint hoạt động idempotent đúng nghĩa, không cần chuẩn hóa NULL ở ETL.';

-- ----------------------------------------------------------------------------
-- DIM_PROPERTY_TYPE
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_property_type (
    property_type_key    BIGSERIAL     PRIMARY KEY,
    property_type_name    VARCHAR(50)   NOT NULL,
    listing_type           VARCHAR(10)   NOT NULL,   -- 'Cần bán' | 'Cho thuê' — giữ nguyên tiếng Việt như Silver.

    CONSTRAINT uq_dim_property_type UNIQUE (property_type_name, listing_type)
);

-- ----------------------------------------------------------------------------
-- DIM_SOURCE
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_source (
    source_key    BIGSERIAL     PRIMARY KEY,
    source_name    VARCHAR(20)   NOT NULL,   -- 'dataset' | 'web' — giữ nguyên như crawl.bronze_file_state.source.
    source_part     VARCHAR(50)   NOT NULL,

    CONSTRAINT uq_dim_source UNIQUE (source_name, source_part)
);

COMMENT ON COLUMN gold.dim_source.source_name IS
    'Giữ nguyên giá trị ngắn gọn như crawl.bronze_file_state.source '
    '(quyết định: không dịch sang dataset_cdn/web_crawl).';

-- ----------------------------------------------------------------------------
-- DIM_PROPERTY_FEATURES - Junk Dimension: chỉ gồm trường categorical/flag,
-- KHÔNG gồm bedrooms/floors (numeric, có thể range-filter trực tiếp trên Fact).
--
-- feature_key GENERATED STORED qua gold.compute_feature_key() — nhất quán
-- với pattern silver.compute_row_hash() (1 nguồn sự thật duy nhất cho hash,
-- ETL Python KHÔNG tự tính MD5 tay, tránh lệch thuật toán giữa Python/SQL).
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION gold.compute_feature_key(
    p_orientation VARCHAR,
    p_legal_status VARCHAR,
    p_has_dining_room BOOLEAN,
    p_has_kitchen BOOLEAN,
    p_has_rooftop BOOLEAN,
    p_has_car_parking BOOLEAN,
    p_owner_direct BOOLEAN
) RETURNS CHAR(32)
LANGUAGE sql IMMUTABLE AS $$
    -- orientation/legal_status là CHUỖI, đã NOT NULL DEFAULT '' (không cần COALESCE).
    -- has_*/owner_direct là BOOLEAN nullable (tri-state unknown) -> vẫn cần COALESCE.
    SELECT MD5(
        p_orientation || '|' ||
        p_legal_status || '|' ||
        COALESCE(p_has_dining_room::TEXT, 'NULL') || '|' ||
        COALESCE(p_has_kitchen::TEXT, 'NULL') || '|' ||
        COALESCE(p_has_rooftop::TEXT, 'NULL') || '|' ||
        COALESCE(p_has_car_parking::TEXT, 'NULL') || '|' ||
        COALESCE(p_owner_direct::TEXT, 'NULL')
    )
$$;

COMMENT ON FUNCTION gold.compute_feature_key IS
    'Công thức feature_key duy nhất cho gold.dim_property_features. '
    'Cùng pattern với silver.compute_row_hash() — ETL Python KHÔNG tự tính MD5.';

CREATE TABLE gold.dim_property_features (
    orientation        VARCHAR(20)   NOT NULL DEFAULT '',
    legal_status        VARCHAR(50)   NOT NULL DEFAULT '',
    has_dining_room     BOOLEAN,
    has_kitchen         BOOLEAN,
    has_rooftop         BOOLEAN,
    has_car_parking      BOOLEAN,
    owner_direct        BOOLEAN,

    -- feature_key phải khai báo SAU các cột nó tham chiếu (yêu cầu cú pháp
    -- GENERATED của Postgres: chỉ tham chiếu cột đã định nghĩa trước đó).
    feature_key       CHAR(32)      GENERATED ALWAYS AS (
        gold.compute_feature_key(
            orientation, legal_status, has_dining_room,
            has_kitchen, has_rooftop, has_car_parking, owner_direct
        )
    ) STORED PRIMARY KEY
);

COMMENT ON TABLE gold.dim_property_features IS
    'Junk dimension cho 7 trường categorical/flag tỷ lệ null cao hoặc giá trị lệch. '
    'Không gồm bedrooms/floors vì chúng cần lọc theo khoảng giá trị, phù hợp để thẳng trên Fact hơn. '
    'feature_key GENERATED — xem gold.compute_feature_key().';

-- ----------------------------------------------------------------------------
-- FACT_LISTING_PRICE
-- Grain: 1 dòng = 1 phiên bản giá của 1 tin đăng (kế thừa SCD2 từ Silver).
-- ----------------------------------------------------------------------------
CREATE TABLE gold.fact_listing_price (
    listing_key       BIGINT        PRIMARY KEY,   -- Map thẳng từ silver.listing_history.listing_key.
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
    price_vnd            NUMERIC(16, 0),              -- Nullable: NULL khi price_is_negotiable=TRUE (kế thừa Silver, Option A).
    price_per_m2_vnd       NUMERIC(15, 2),            -- Copy từ Silver, KHÔNG tính lại.
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
    area_is_outlier            BOOLEAN NOT NULL,     -- MỚI: mirror từ silver.listing_history.area_is_outlier
    has_warning              BOOLEAN NOT NULL,
    is_expired               BOOLEAN NOT NULL
);

COMMENT ON TABLE gold.fact_listing_price IS
    'Fact chính. Grain: 1 dòng = 1 phiên bản giá (SCD2). '
    'Khi tính AVG(price_per_m2_vnd) phải lọc price_is_negotiable=false '
    'để tránh NULL (tin thỏa thuận) làm lệch kết quả — AVG()/SUM() Postgres tự bỏ NULL nên '
    'thực ra không bắt buộc filter thủ công, nhưng filter tường minh vẫn nên làm cho rõ ý query.';
COMMENT ON COLUMN gold.fact_listing_price.price_vnd IS
    'NULL khi price_is_negotiable=TRUE — nhất quán với silver.listing_history, '
    'KHÔNG map sang 0, tránh rủi ro quên filter price_is_negotiable khi tính AVG/SUM.';
COMMENT ON COLUMN gold.fact_listing_price.area_is_outlier IS
    'Mirror từ silver.listing_history.area_is_outlier — TRUE khi area_m2 gốc >10.000m2 đã bị null hóa ở Silver.';
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
