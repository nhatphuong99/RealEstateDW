-- ============================================================================
-- sql/schema_full.sql
-- DDL hợp nhất toàn bộ database real_estate_dw (gộp từ 6 file 001-006).
-- Thứ tự: pipeline (control-plane) -> silver (SCD2) -> gold (star schema).
-- Idempotent: CREATE ... IF NOT EXISTS, chạy lại an toàn trên DB rỗng.
--
-- Thay đổi so với bản gốc (đã thống nhất):
--   - Bỏ silver.listing_history.crawl_date (trùng giá trị valid_from mọi dòng)
--   - Bỏ gold.dim_date.day/month/quarter/year (không dùng, dashboard tự DATE_TRUNC)
--   - Bỏ idx_listing_history_id_current (trùng ux_listing_history_current)
-- => Phải chạy kèm bản vá merge_scd2_listing_history.sql + etl_silver_to_gold.sql.
-- ============================================================================

-- ============================================================================
-- 1. SCHEMA pipeline — control-plane cho DAG 1/2/3
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS pipeline;

-- Con trỏ crawl trang danh sách (DAG 2)
CREATE TABLE IF NOT EXISTS pipeline.listing_progress (
    id              SERIAL PRIMARY KEY,
    province_old    TEXT NOT NULL,
    listing_type    TEXT NOT NULL,
    property_type   TEXT NOT NULL,
    current_page    INT NOT NULL DEFAULT 1,
    status          TEXT NOT NULL DEFAULT 'active',   -- active / exhausted
    crawl_date      DATE NOT NULL,                    -- reset mỗi ngày
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (province_old, listing_type, property_type, crawl_date)
);

-- Hàng đợi URL chi tiết (DAG 2)
-- Vòng đời: pending -> processing -> fetched -> flushed -> done (hoặc -> failed)
CREATE TABLE IF NOT EXISTS pipeline.detail_queue (
    id                  SERIAL PRIMARY KEY,
    url                 TEXT UNIQUE NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',
    discovered_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at          TIMESTAMPTZ,                  -- dùng để reset task treo quá lâu
    discovered_page_id  INT REFERENCES pipeline.listing_progress(id),
    crawl_date          DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_detail_queue_pending_fifo
    ON pipeline.detail_queue (discovered_at)
    WHERE status = 'pending';

-- Trạng thái mỗi lần chạy DAG 2
CREATE TABLE IF NOT EXISTS pipeline.run_state (
    id                SERIAL PRIMARY KEY,
    run_id            TEXT UNIQUE NOT NULL,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at          TIMESTAMPTZ,
    stopped_reason    TEXT,
    detail_pages_done INT NOT NULL DEFAULT 0,
    output_s3_key     TEXT                             -- chỉ để audit, không dùng lại trong logic
);

-- Trạng thái tải từng part CDN cố định (DAG 1)
CREATE TABLE IF NOT EXISTS pipeline.dataset_part_state (
    part_number   INT PRIMARY KEY,                     -- 1..77
    status        TEXT NOT NULL DEFAULT 'pending',      -- pending/done/failed
    s3_key        TEXT,
    probed_at     TIMESTAMPTZ,
    downloaded_at TIMESTAMPTZ,
    last_error    TEXT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO pipeline.dataset_part_state (part_number)
SELECT generate_series(1, 77)
ON CONFLICT (part_number) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_dataset_part_state_pending
    ON pipeline.dataset_part_state (part_number)
    WHERE status IN ('pending', 'failed');

-- Trạng thái parse từng file Bronze (DAG 3)
CREATE TABLE IF NOT EXISTS pipeline.bronze_file_state (
    s3_key            TEXT PRIMARY KEY,
    source            TEXT NOT NULL,                   -- 'dataset' | 'web'
    status            TEXT NOT NULL DEFAULT 'pending',  -- pending/processing/done/failed
    rows_parsed       INT,
    rows_quarantined  INT,
    discovered_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at      TIMESTAMPTZ,
    last_error        TEXT
);

CREATE INDEX IF NOT EXISTS idx_bronze_file_state_pending
    ON pipeline.bronze_file_state (discovered_at)
    WHERE status = 'pending';

-- Timezone mặc định toàn DB
ALTER DATABASE real_estate_dw SET timezone TO 'Asia/Ho_Chi_Minh';

-- ============================================================================
-- 2. SCHEMA silver — SCD2 (Clean zone)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS silver;

-- Hàm tính row_hash — nguồn sự thật duy nhất để phát hiện thay đổi (SCD2 trigger).
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

-- Lịch sử tin đăng, 1 dòng = 1 phiên bản giá (SCD2 trên 5 trường: price_vnd,
-- price_is_negotiable, is_expired, has_warning, area_m2).
CREATE TABLE silver.listing_history (
    listing_key BIGSERIAL PRIMARY KEY,
    listing_id BIGINT NOT NULL,
    listing_url TEXT NOT NULL,
    source_part VARCHAR(50) NOT NULL,
    source_bronze_key TEXT NOT NULL,       -- S3 key sinh ra version này (debug/trace)
    valid_from TIMESTAMPTZ NOT NULL,        -- mốc SCD2 = thời điểm crawl phát hiện version này
    valid_to TIMESTAMPTZ,                    -- NULL = chưa từng crawl lại lần 2 để xác nhận
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    last_seen_at TIMESTAMPTZ NOT NULL,        -- cập nhật khi crawl lại mà hash không đổi

    title TEXT NOT NULL,                       -- chỉ audit, không dùng ở Gold
    listing_type VARCHAR(10) NOT NULL,
    property_type VARCHAR(50) NOT NULL,
    posted_date DATE NOT NULL,                  -- trend axis chính dùng ở Gold

    price_vnd NUMERIC(16,0),                     -- NULL khi price_is_negotiable=TRUE
    price_raw TEXT,
    price_is_negotiable BOOLEAN NOT NULL DEFAULT FALSE,

    area_m2 NUMERIC(10,2),
    area_raw TEXT,
    area_is_undetermined BOOLEAN NOT NULL DEFAULT FALSE,
    area_is_outlier BOOLEAN NOT NULL DEFAULT FALSE,   -- area_m2 gốc ngoài [3, 10.000] m2, đã null hóa

    price_per_m2_vnd NUMERIC(15,2)
        GENERATED ALWAYS AS (
            CASE WHEN area_is_undetermined OR area_m2 IS NULL OR area_m2=0 OR price_vnd IS NULL
                 THEN NULL ELSE ROUND(price_vnd/area_m2,2) END
        ) STORED,

    price_is_outlier BOOLEAN NOT NULL DEFAULT FALSE,   -- giá/m2 > 5 tỷ; price_vnd giữ nguyên, không null hóa

    length_m NUMERIC(6,2),
    width_m NUMERIC(6,2),
    street_width_m NUMERIC(6,2),
    floors SMALLINT,
    bedrooms SMALLINT,

    orientation VARCHAR(20) NOT NULL DEFAULT '',
    legal_status VARCHAR(50) NOT NULL DEFAULT '',

    -- Tri-state: TRUE = có icon check | NULL = không xác định (site không có ký hiệu phủ định)
    has_dining_room BOOLEAN,
    has_kitchen BOOLEAN,
    has_rooftop BOOLEAN,
    has_car_parking BOOLEAN,
    owner_direct BOOLEAN,

    is_expired BOOLEAN NOT NULL DEFAULT FALSE,
    has_warning BOOLEAN NOT NULL DEFAULT FALSE,

    address_street_new VARCHAR(200) NOT NULL DEFAULT '',
    address_ward_new VARCHAR(100) NOT NULL DEFAULT '',
    address_province_new VARCHAR(100) NOT NULL DEFAULT '',

    address_old_raw TEXT NOT NULL DEFAULT '',    -- chỉ audit ở Silver, không xuống Gold
    address_ward_old VARCHAR(100) NOT NULL DEFAULT '',
    address_district_old VARCHAR(100) NOT NULL DEFAULT '',
    address_province_old VARCHAR(100) NOT NULL DEFAULT '',   -- KHÔNG dùng để lọc scope

    row_hash CHAR(32)
        GENERATED ALWAYS AS (
            silver.compute_row_hash(price_vnd, price_is_negotiable, is_expired, has_warning, area_m2)
        ) STORED,

    CONSTRAINT chk_price_negotiable_null CHECK (
        (price_is_negotiable=TRUE AND price_vnd IS NULL)
        OR (price_is_negotiable=FALSE AND price_vnd IS NOT NULL)
    ),

    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Quy ước: cột CHUỖI NOT NULL DEFAULT '' khi thiếu ('' luôn UNKNOWN trong SQL,
-- khai báo thẳng ở DDL để lỗi lộ ngay, không trôi xuống Gold). Cột SỐ/BOOLEAN
-- giữ NULL đúng nghĩa "thiếu".

CREATE UNIQUE INDEX ux_listing_history_current       -- đảm bảo mỗi listing_id chỉ 1 bản is_current
    ON silver.listing_history (listing_id)
    WHERE is_current;

CREATE INDEX idx_listing_history_id_valid_from        -- phục vụ LAG() khi merge SCD2
    ON silver.listing_history (listing_id, valid_from);

-- Landing zone tạm 1 batch Spark parse. Cùng cột listing_history, trừ cột chỉ
-- sinh khi vào listing_history (listing_key, valid_from, valid_to, is_current,
-- last_seen_at, ingested_at) — thay vào đó giữ crawl_date làm nguồn cho valid_from.
CREATE UNLOGGED TABLE IF NOT EXISTS silver.listing_staging_batch (
    listing_id           BIGINT       NOT NULL,
    listing_url          TEXT         NOT NULL,
    source_part          VARCHAR(50)  NOT NULL,
    source_bronze_key    TEXT         NOT NULL,
    crawl_date           TIMESTAMPTZ  NOT NULL,   -- sẽ trở thành valid_from khi merge vào listing_history

    title                TEXT         NOT NULL,
    listing_type         VARCHAR(10)  NOT NULL,
    property_type        VARCHAR(50)  NOT NULL,
    posted_date          DATE         NOT NULL,

    price_vnd            NUMERIC(16, 0),
    price_raw            TEXT,
    price_is_negotiable  BOOLEAN      NOT NULL DEFAULT FALSE,
    price_is_outlier     BOOLEAN      NOT NULL DEFAULT FALSE,

    area_m2               NUMERIC(10, 2),
    area_raw               TEXT,
    area_is_undetermined   BOOLEAN      NOT NULL DEFAULT FALSE,
    area_is_outlier        BOOLEAN      NOT NULL DEFAULT FALSE,

    length_m       NUMERIC(6, 2),
    width_m        NUMERIC(6, 2),
    street_width_m NUMERIC(6, 2),
    floors         SMALLINT,
    bedrooms       SMALLINT,

    orientation    VARCHAR(20)  NOT NULL DEFAULT '',
    legal_status   VARCHAR(50)  NOT NULL DEFAULT '',

    has_dining_room  BOOLEAN,
    has_kitchen      BOOLEAN,
    has_rooftop      BOOLEAN,
    has_car_parking  BOOLEAN,
    owner_direct     BOOLEAN,

    is_expired    BOOLEAN NOT NULL DEFAULT FALSE,
    has_warning   BOOLEAN NOT NULL DEFAULT FALSE,

    address_street_new    VARCHAR(200) NOT NULL DEFAULT '',
    address_ward_new       VARCHAR(100) NOT NULL DEFAULT '',
    address_province_new    VARCHAR(100) NOT NULL DEFAULT '',

    address_old_raw           TEXT         NOT NULL DEFAULT '',
    address_ward_old           VARCHAR(100) NOT NULL DEFAULT '',
    address_district_old        VARCHAR(100) NOT NULL DEFAULT '',
    address_province_old         VARCHAR(100) NOT NULL DEFAULT '',

    row_hash CHAR(32)
        GENERATED ALWAYS AS (
            silver.compute_row_hash(price_vnd, price_is_negotiable, is_expired, has_warning, area_m2)
        ) STORED,

    CONSTRAINT chk_staging_price_negotiable_null CHECK (
        (price_is_negotiable = TRUE  AND price_vnd IS NULL)
        OR (price_is_negotiable = FALSE AND price_vnd IS NOT NULL)
    )
);

COMMENT ON TABLE silver.listing_staging_batch IS
    'Landing zone tạm/batch, UNLOGGED, TRUNCATE trước mỗi lần chạy — mất dữ liệu khi crash chấp nhận được vì Bronze immutable, chạy lại ETL là đủ.';

CREATE INDEX IF NOT EXISTS idx_staging_listing_id_crawl_date
    ON silver.listing_staging_batch (listing_id, crawl_date);

-- Bản ghi Bronze parse thất bại (HTML lệch cấu trúc, thiếu field bắt buộc...)
CREATE TABLE IF NOT EXISTS silver.parse_quarantine (
    id                 BIGSERIAL    PRIMARY KEY,
    url                TEXT         NOT NULL,
    crawl_date         TIMESTAMPTZ  NOT NULL,
    source_bronze_key  TEXT         NOT NULL,
    error_reason       TEXT         NOT NULL,
    raw_html           BYTEA,                    -- giữ để debug, khỏi đọc lại S3
    quarantined_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_parse_quarantine_source_key
    ON silver.parse_quarantine (source_bronze_key);

-- ============================================================================
-- 3. SCHEMA gold — Star Schema Kimball (Fact Observation-grain, 1:1 listing_history)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS gold;

-- DIM_DATE — Type 0, chỉ phủ khoảng posted_date.
CREATE TABLE gold.dim_date (
    date_key      INTEGER      PRIMARY KEY,   -- YYYYMMDD
    full_date     DATE         NOT NULL UNIQUE
);

-- DIM_LOCATION — Type 1. street KHÔNG dùng GROUP BY, chỉ tăng độ chi tiết grain.
CREATE TABLE gold.dim_location (
    location_key    BIGSERIAL     PRIMARY KEY,
    province_new     VARCHAR(100)  NOT NULL DEFAULT '',
    ward_new         VARCHAR(100)  NOT NULL DEFAULT '',
    province_old      VARCHAR(100)  NOT NULL DEFAULT '',   -- có thể KHÁC province_new (sáp nhập địa giới)
    ward_old         VARCHAR(100)  NOT NULL DEFAULT '',
    district_old      VARCHAR(100)  NOT NULL DEFAULT '',
    street            VARCHAR(200)  NOT NULL DEFAULT '',

    CONSTRAINT uq_dim_location UNIQUE (province_new, ward_new, province_old, ward_old, district_old, street)
);

-- DIM_PROPERTY_TYPE — Type 0, 10 tổ hợp cố định (5 property_type x 2 listing_type).
CREATE TABLE gold.dim_property_type (
    property_type_key    BIGSERIAL     PRIMARY KEY,
    property_type_name    VARCHAR(50)   NOT NULL,
    listing_type           VARCHAR(10)   NOT NULL,

    CONSTRAINT uq_dim_property_type UNIQUE (property_type_name, listing_type)
);

-- DIM_SOURCE — Type 0, phục vụ lineage (dataset vs web).
CREATE TABLE gold.dim_source (
    source_key    BIGSERIAL     PRIMARY KEY,
    source_name    VARCHAR(20)   NOT NULL,   -- 'dataset' | 'web'
    source_part     VARCHAR(50)   NOT NULL,

    CONSTRAINT uq_dim_source UNIQUE (source_name, source_part)
);

COMMENT ON COLUMN gold.dim_source.source_name IS
    'Suy từ prefix source_bronze_key qua hàm Python infer_source_from_bronze_key() — đổi convention S3 key phải sửa đồng bộ cả 2 nơi.';

-- DIM_PROPERTY_FEATURES — Junk dimension, Type 1. feature_key GENERATED STORED.
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

CREATE TABLE gold.dim_property_features (
    orientation        VARCHAR(20)   NOT NULL DEFAULT '',
    legal_status        VARCHAR(50)   NOT NULL DEFAULT '',
    has_dining_room     BOOLEAN,
    has_kitchen         BOOLEAN,
    has_rooftop         BOOLEAN,
    has_car_parking      BOOLEAN,
    owner_direct        BOOLEAN,

    feature_key       CHAR(32)      GENERATED ALWAYS AS (
        gold.compute_feature_key(
            orientation, legal_status, has_dining_room,
            has_kitchen, has_rooftop, has_car_parking, owner_direct
        )
    ) STORED PRIMARY KEY
);

-- FACT_LISTING_PRICE — 1 dòng = 1 version giá đã quan sát, khớp 1:1 silver.listing_history.
CREATE TABLE gold.fact_listing_price (
    listing_key       BIGINT        PRIMARY KEY,   -- map thẳng silver.listing_history.listing_key
    listing_id         BIGINT        NOT NULL,       -- degenerate dimension, trace về Silver/Bronze

    location_key         BIGINT   NOT NULL REFERENCES gold.dim_location (location_key),
    property_type_key     BIGINT   NOT NULL REFERENCES gold.dim_property_type (property_type_key),
    feature_key           CHAR(32) NOT NULL REFERENCES gold.dim_property_features (feature_key),
    source_key             BIGINT   NOT NULL REFERENCES gold.dim_source (source_key),
    posted_date_key         INTEGER  NOT NULL REFERENCES gold.dim_date (date_key),

    valid_from        TIMESTAMPTZ  NOT NULL,   -- lineage/audit, không phải trend axis
    valid_to          TIMESTAMPTZ,
    is_current        BOOLEAN      NOT NULL,

    price_vnd            NUMERIC(16, 0),          -- NULL khi price_is_negotiable=TRUE
    price_per_m2_vnd       NUMERIC(15, 2),          -- copy từ Silver, không tính lại
    area_m2                NUMERIC(10, 2),

    bedrooms              SMALLINT,
    floors                SMALLINT,
    length_m              NUMERIC(6, 2),
    width_m                NUMERIC(6, 2),
    street_width_m          NUMERIC(6, 2),

    price_is_negotiable      BOOLEAN NOT NULL,
    price_is_outlier            BOOLEAN NOT NULL,   -- mirror Silver, lọc khi AVG/SUM
    area_is_undetermined      BOOLEAN NOT NULL,
    area_is_outlier            BOOLEAN NOT NULL,
    has_warning              BOOLEAN NOT NULL,
    is_expired               BOOLEAN NOT NULL
);

COMMENT ON TABLE gold.fact_listing_price IS
    'Tính AVG(price_per_m2_vnd) nên lọc price_is_negotiable=false và price_is_outlier=false tường minh.';

CREATE INDEX idx_fact_location            ON gold.fact_listing_price (location_key);
CREATE INDEX idx_fact_property_type       ON gold.fact_listing_price (property_type_key);
CREATE INDEX idx_fact_posted_date         ON gold.fact_listing_price (posted_date_key);
CREATE INDEX idx_fact_listing_id          ON gold.fact_listing_price (listing_id);
CREATE INDEX idx_fact_current             ON gold.fact_listing_price (is_current) WHERE is_current;

-- ============================================================================
-- 4. Tầng BI (Metabase) — KHÔNG đụng dữ liệu Silver/Gold gốc
-- ============================================================================

-- Crosswalk Quận/Huyện cũ -> tên khớp GeoJSON (snapshot cũ trước khi lên TP)
CREATE TABLE IF NOT EXISTS gold.map_district_geo_crosswalk (
    district_old         VARCHAR(100)  PRIMARY KEY,
    geojson_ten_day_du     VARCHAR(100)  NOT NULL,
    ghi_chu                TEXT
);

INSERT INTO gold.map_district_geo_crosswalk (district_old, geojson_ten_day_du, ghi_chu) VALUES
    ('Thành phố Bến Cát',  'Thị xã Bến Cát',  'GeoJSON snapshot trước 01/05/2024'),
    ('Thành phố Dĩ An',    'Thị xã Dĩ An',    'GeoJSON snapshot trước 2020'),
    ('Thành phố Thuận An', 'Thị xã Thuận An', 'GeoJSON snapshot trước 2020'),
    ('Thành phố Tân Uyên', 'Thị xã Tân Uyên', 'GeoJSON snapshot trước 2023'),
    ('Thành phố Phú Mỹ',   'Huyện Phú Mỹ',    'GeoJSON snapshot trước khi lên TP')
ON CONFLICT (district_old) DO NOTHING;

-- Crosswalk Phường/Xã mới -> tên khớp GeoJSON (data quality issue nguồn alonhadat)
CREATE TABLE IF NOT EXISTS gold.map_ward_geo_crosswalk (
    ward_new              VARCHAR(100)  PRIMARY KEY,
    geojson_ten_day_du     VARCHAR(100)  NOT NULL,
    ghi_chu                TEXT
);

INSERT INTO gold.map_ward_geo_crosswalk (ward_new, geojson_ten_day_du, ghi_chu) VALUES
    ('Phường Hóc Môn', 'Xã Hóc Môn', 'Tên chính thức theo NQ 1685/NQ-UBTVQH15 (01/07/2025) là Xã Hóc Môn')
ON CONFLICT (ward_new) DO NOTHING;

-- View phẳng — nguồn chính cho mọi Question Metabase. Grain = fact_listing_price.
-- Chuẩn hóa Unicode NFC 2 chiều khi so khớp GeoJSON (tránh lệch NFC/NFD).
CREATE OR REPLACE VIEW gold.vw_fact_report AS
SELECT
    f.listing_key, f.listing_id,
    f.price_vnd, f.price_per_m2_vnd, f.area_m2, f.bedrooms, f.floors,
    f.length_m, f.width_m, f.street_width_m,
    f.price_is_negotiable, f.price_is_outlier, f.area_is_undetermined, f.area_is_outlier,
    f.has_warning, f.is_expired, f.is_current, f.valid_from, f.valid_to,

    dd.full_date  AS posted_date,

    dl.province_new, dl.ward_new, dl.province_old, dl.ward_old, dl.district_old, dl.street,

    -- Map key cho region map Metabase (đã qua crosswalk + NFC nếu cần)
    COALESCE(mw.geojson_ten_day_du, NORMALIZE(dl.ward_new, NFC)) AS ward_new_map_key,
    COALESCE(md.geojson_ten_day_du, NORMALIZE(dl.district_old, NFC)) AS district_old_map_key,

    dpt.property_type_name, dpt.listing_type,
    ds.source_name, ds.source_part,
    dpf.orientation, dpf.legal_status, dpf.has_dining_room, dpf.has_kitchen,
    dpf.has_rooftop, dpf.has_car_parking, dpf.owner_direct

FROM gold.fact_listing_price f
JOIN gold.dim_date dd            ON dd.date_key = f.posted_date_key
JOIN gold.dim_location dl             ON dl.location_key = f.location_key
JOIN gold.dim_property_type dpt       ON dpt.property_type_key = f.property_type_key
JOIN gold.dim_source ds               ON ds.source_key = f.source_key
JOIN gold.dim_property_features dpf   ON dpf.feature_key = f.feature_key
LEFT JOIN gold.map_ward_geo_crosswalk mw
       ON NORMALIZE(mw.ward_new, NFC) = NORMALIZE(dl.ward_new, NFC)
LEFT JOIN gold.map_district_geo_crosswalk md
       ON NORMALIZE(md.district_old, NFC) = NORMALIZE(dl.district_old, NFC);
