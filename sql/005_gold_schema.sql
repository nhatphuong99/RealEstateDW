-- ============================================================================
-- sql/005_gold_schema.sql
-- Star Schema Kimball — Fact loại Transaction/Observation-grain.
--
-- Grain fact_listing_price: 1 dòng = 1 version giá đã quan sát được, khớp
-- 1:1 với silver.listing_history (KHÔNG fan-out theo ngày kiểu Periodic
-- Snapshot). Lý do: crawler chỉ enqueue mỗi URL 1 lần (UNIQUE(url)), không
-- có đủ bằng chứng để suy diễn "còn active vào ngày X" cho mọi ngày giữa
-- 2 lần crawl — Periodic Snapshot sẽ buộc phải nội suy sai thực tế.
-- Chi tiết: xem kien_truc_tong_hop_he_thong.md mục "Vì sao Transaction/
-- Observation-grain".
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- ----------------------------------------------------------------------------
-- DIM_DATE — Type 0. Role-playing: observed_date_key (thuộc grain Fact —
-- ngày quan sát) và posted_date_key (thông tin phụ) cùng trỏ bảng này.
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
    'Bảng ngày chuẩn Kimball, Type 0. Role-playing: observed_date_key (thuộc grain Fact, '
    '= valid_from::date) và posted_date_key (thông tin phụ). Tầng BI tự alias khi JOIN 2 lần.';

-- ----------------------------------------------------------------------------
-- DIM_LOCATION — Type 1 (không SCD2, vì đổi địa chỉ giữa các version Silver
-- rất hiếm và Fact tự resolve location_key riêng mỗi ETL run).
--
-- province_new/province_old tách riêng vì có tin lệch tỉnh cũ/mới do sáp
-- nhập địa giới. address_old_raw chỉ audit ở Silver, không xuống Gold.
-- Cột chuỗi NOT NULL DEFAULT '' khớp quy ước Silver để UPSERT idempotent.
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_location (
    location_key    BIGSERIAL     PRIMARY KEY,
    province_new     VARCHAR(100)  NOT NULL DEFAULT '',   -- Hiện tại luôn 'Hồ Chí Minh'.
    ward_new         VARCHAR(100)  NOT NULL DEFAULT '',   -- Phường/Xã địa giới MỚI - khóa nhóm chính.
    province_old      VARCHAR(100)  NOT NULL DEFAULT '',   -- Có thể KHÁC province_new.
    ward_old         VARCHAR(100)  NOT NULL DEFAULT '',   -- Phường/Xã địa giới CŨ.
    district_old      VARCHAR(100)  NOT NULL DEFAULT '',   -- Quận/Huyện - chỉ có ở địa chỉ cũ.
    street            VARCHAR(200)  NOT NULL DEFAULT '',   -- Mô tả thêm, KHÔNG dùng để GROUP BY.

    CONSTRAINT uq_dim_location UNIQUE (province_new, ward_new, province_old, ward_old, district_old, street)
);

COMMENT ON TABLE gold.dim_location IS
    'Địa điểm, Type 1. GROUP BY ward_new cho phân tích theo địa giới mới; '
    'filter ward_old/district_old/province_old cho người dùng quen địa chỉ cũ.';
COMMENT ON COLUMN gold.dim_location.province_old IS
    'Tỉnh/thành trước sáp nhập — KHÔNG dùng để lọc scope (is_in_scope() chỉ dùng province_new).';

-- ----------------------------------------------------------------------------
-- DIM_PROPERTY_TYPE — Type 0, đúng 10 tổ hợp cố định (5 property_type x 2 listing_type).
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_property_type (
    property_type_key    BIGSERIAL     PRIMARY KEY,
    property_type_name    VARCHAR(50)   NOT NULL,
    listing_type           VARCHAR(10)   NOT NULL,   -- 'Cần bán' | 'Cho thuê'.

    CONSTRAINT uq_dim_property_type UNIQUE (property_type_name, listing_type)
);

-- ----------------------------------------------------------------------------
-- DIM_SOURCE — Type 0.
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_source (
    source_key    BIGSERIAL     PRIMARY KEY,
    source_name    VARCHAR(20)   NOT NULL,   -- 'dataset' | 'web', giữ nguyên như pipeline.bronze_file_state.source.
    source_part     VARCHAR(50)   NOT NULL,

    CONSTRAINT uq_dim_source UNIQUE (source_name, source_part)
);

COMMENT ON COLUMN gold.dim_source.source_name IS
    'Suy từ prefix source_bronze_key qua CASE/LIKE ở etl_silver_to_gold.sql — duplicate có chủ đích '
    'của hàm Python infer_source_from_bronze_key() (SQL không gọi được). Đổi convention S3 key '
    'phải sửa đồng bộ cả 2 nơi.';

-- ----------------------------------------------------------------------------
-- DIM_PROPERTY_FEATURES — Junk Dimension, Type 1. 7 trường categorical/flag,
-- không gồm bedrooms/floors (numeric, filter trực tiếp trên Fact). Không
-- SCD2 vì các field này không nằm trong 5 trường trigger SCD2 của Silver.
-- feature_key GENERATED STORED, cùng pattern silver.compute_row_hash().
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
    -- orientation/legal_status NOT NULL DEFAULT '' (không cần COALESCE); has_*/owner_direct nullable -> cần COALESCE.
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
    'Công thức feature_key duy nhất cho dim_property_features — cùng pattern silver.compute_row_hash().';

CREATE TABLE gold.dim_property_features (
    orientation        VARCHAR(20)   NOT NULL DEFAULT '',
    legal_status        VARCHAR(50)   NOT NULL DEFAULT '',
    has_dining_room     BOOLEAN,
    has_kitchen         BOOLEAN,
    has_rooftop         BOOLEAN,
    has_car_parking      BOOLEAN,
    owner_direct        BOOLEAN,

    -- feature_key phải khai báo SAU các cột nó tham chiếu (yêu cầu cú pháp GENERATED của Postgres).
    feature_key       CHAR(32)      GENERATED ALWAYS AS (
        gold.compute_feature_key(
            orientation, legal_status, has_dining_room,
            has_kitchen, has_rooftop, has_car_parking, owner_direct
        )
    ) STORED PRIMARY KEY
);

COMMENT ON TABLE gold.dim_property_features IS
    'Junk dimension cho 7 trường categorical/flag tỷ lệ null cao. Không gồm bedrooms/floors '
    '(cần lọc theo khoảng, để thẳng trên Fact). feature_key GENERATED, xem compute_feature_key().';

-- ============================================================================
-- FACT_LISTING_PRICE — Transaction/Observation-grain.
-- Grain: 1 dòng = 1 version giá đã quan sát được, khớp 1:1 silver.listing_history.
-- ============================================================================
CREATE TABLE gold.fact_listing_price (
    listing_key       BIGINT        PRIMARY KEY,   -- Map thẳng từ silver.listing_history.listing_key.
    listing_id         BIGINT        NOT NULL,       -- Degenerate dimension, trace về Silver/Bronze.
    listing_url         TEXT          NOT NULL,

    -- Khóa ngoại tới các dimension
    location_key         BIGINT   NOT NULL REFERENCES gold.dim_location (location_key),
    property_type_key     BIGINT   NOT NULL REFERENCES gold.dim_property_type (property_type_key),
    feature_key           CHAR(32) NOT NULL REFERENCES gold.dim_property_features (feature_key),
    source_key             BIGINT   NOT NULL REFERENCES gold.dim_source (source_key),

    -- Role-playing date: observed_date_key dùng cho mọi phân tích xu hướng
    -- (nên bucket theo tuần/tháng — mật độ quan sát thưa/không đều).
    observed_date_key           INTEGER NOT NULL REFERENCES gold.dim_date (date_key),
    posted_date_key               INTEGER NOT NULL REFERENCES gold.dim_date (date_key),

    -- Temporal metadata
    valid_from        TIMESTAMPTZ  NOT NULL,   -- = thời điểm quan sát đầu tiên (crawl_date).
    valid_to          TIMESTAMPTZ,             -- NULL ở đa số dòng — xem COMMENT cột.
    last_seen_at      TIMESTAMPTZ  NOT NULL,   -- Lần xác nhận gần nhất (có thể = valid_from).
    is_current        BOOLEAN      NOT NULL,

    -- Cờ chất lượng quan sát
    is_reconfirmed    BOOLEAN GENERATED ALWAYS AS (last_seen_at > valid_from) STORED,

    -- Measure chính
    price_vnd            NUMERIC(16, 0),              -- Nullable: NULL khi price_is_negotiable=TRUE.
    price_per_m2_vnd       NUMERIC(15, 2),            -- Copy từ Silver, KHÔNG tính lại.
    area_m2                NUMERIC(10, 2),

    -- Degenerate measure/attribute
    bedrooms              SMALLINT,
    floors                SMALLINT,
    length_m              NUMERIC(6, 2),
    width_m                NUMERIC(6, 2),
    street_width_m          NUMERIC(6, 2),

    -- Cờ lọc
    price_is_negotiable      BOOLEAN NOT NULL,
    price_is_outlier            BOOLEAN NOT NULL,     -- Mirror silver.listing_history.price_is_outlier.
    area_is_undetermined      BOOLEAN NOT NULL,
    area_is_outlier            BOOLEAN NOT NULL,     -- Mirror silver.listing_history.area_is_outlier.
    has_warning              BOOLEAN NOT NULL,
    is_expired               BOOLEAN NOT NULL
);

COMMENT ON TABLE gold.fact_listing_price IS
    'Transaction/Observation-grain Fact. Grain: 1 dòng = 1 version giá đã quan sát, khớp 1:1 '
    'silver.listing_history. Khi tính AVG(price_per_m2_vnd) nên lọc price_is_negotiable=false '
    'và price_is_outlier=false tường minh (dễ đọc query, dù AVG tự bỏ NULL).';
COMMENT ON COLUMN gold.fact_listing_price.price_vnd IS
    'NULL khi price_is_negotiable=TRUE, nhất quán với Silver — KHÔNG map sang 0.';
COMMENT ON COLUMN gold.fact_listing_price.price_is_outlier IS
    'Mirror silver.listing_history.price_is_outlier (TRUE khi giá/m2 > 5 tỷ/m2). '
    'price_vnd không bị null hóa — lọc bằng cờ này khi tính AVG/dashboard.';
COMMENT ON COLUMN gold.fact_listing_price.area_is_outlier IS
    'Mirror silver.listing_history.area_is_outlier (area_m2 gốc >10.000m2 hoặc <3m2, đã null hóa ở Silver).';
COMMENT ON COLUMN gold.fact_listing_price.valid_to IS
    'NULL ở đa số dòng — nghĩa "chưa từng crawl lại lần 2 để xác nhận thay đổi", '
    'KHÔNG có nghĩa "tin vẫn đang active". Không suy diễn "còn hiệu lực tới hiện tại" từ NULL này.';
COMMENT ON COLUMN gold.fact_listing_price.is_current IS
    'Nghĩa chính xác: "chưa từng phát hiện version mới hơn thay thế" — KHÔNG đảm bảo tin vẫn '
    'tồn tại trên site thật, vì phần lớn chưa từng được crawl lại để kiểm chứng.';
COMMENT ON COLUMN gold.fact_listing_price.is_reconfirmed IS
    'TRUE khi listing có ≥2 lần quan sát (last_seen_at > valid_from) — có bằng chứng xác nhận '
    'thực tế, không chỉ suy từ 1 lần crawl. Dùng lọc độ tin cậy cao hoặc KPI "% dữ liệu kiểm chứng lại".';
COMMENT ON COLUMN gold.fact_listing_price.observed_date_key IS
    'Ngày quan sát đầu tiên (= valid_from::date). Trục thời gian chính cho phân tích xu hướng — '
    'nên GROUP BY theo tuần/tháng, không theo ngày (mật độ quan sát thưa, không đều).';
COMMENT ON COLUMN gold.fact_listing_price.posted_date_key IS
    'Ngày đăng tin (site tự ghi nhận) — thông tin phụ, không dùng làm trục phân tích chính.';

-- Index phục vụ truy vấn chính: giá theo Phường-Xã/Loại hình/Thời gian
CREATE INDEX idx_fact_location            ON gold.fact_listing_price (location_key);
CREATE INDEX idx_fact_property_type       ON gold.fact_listing_price (property_type_key);
CREATE INDEX idx_fact_observed_date       ON gold.fact_listing_price (observed_date_key);
CREATE INDEX idx_fact_posted_date         ON gold.fact_listing_price (posted_date_key);
CREATE INDEX idx_fact_listing_id          ON gold.fact_listing_price (listing_id);
CREATE INDEX idx_fact_current             ON gold.fact_listing_price (is_current) WHERE is_current;
