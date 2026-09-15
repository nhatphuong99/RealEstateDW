-- ============================================================================
-- sql/schema_full.sql
-- Consolidated DDL for the entire real_estate_dw database 
-- Order: pipeline (control-plane) -> silver (SCD2) -> gold (star schema).
-- Idempotent: every CREATE TABLE/INDEX uses IF NOT EXISTS, every function
-- uses CREATE OR REPLACE — safe to re-run this entire file as-is against
-- an EMPTY database (fresh init) OR an EXISTING one (e.g. to pick up a
-- newly added function/table without recreating what's already there).
-- Re-running does NOT alter columns of tables that already exist — adding
-- a column to an existing table still needs a manual ALTER TABLE.
-- ============================================================================

-- ============================================================================
-- 1. SCHEMA pipeline — control-plane for DAG 1/2/3
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS pipeline;

-- Listing-page crawl cursor (DAG 2)
CREATE TABLE IF NOT EXISTS pipeline.listing_progress (
    id              SERIAL PRIMARY KEY,
    province_old    TEXT NOT NULL,
    listing_type    TEXT NOT NULL,
    property_type   TEXT NOT NULL,
    current_page    INT NOT NULL DEFAULT 1,
    status          TEXT NOT NULL DEFAULT 'active',   -- active / exhausted
    crawl_date      DATE NOT NULL,                    -- reset every day
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (province_old, listing_type, property_type, crawl_date)
);

-- Detail-page URL queue (DAG 2)
-- Lifecycle: pending -> processing -> fetched -> flushed -> done (or -> failed)
CREATE TABLE IF NOT EXISTS pipeline.detail_queue (
    id                  SERIAL PRIMARY KEY,
    url                 TEXT UNIQUE NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',
    discovered_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at          TIMESTAMPTZ,                  -- used to reset tasks stuck too long
    discovered_page_id  INT REFERENCES pipeline.listing_progress(id),
    crawl_date          DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_detail_queue_pending_fifo
    ON pipeline.detail_queue (discovered_at)
    WHERE status = 'pending';

-- State of each DAG 2 run
CREATE TABLE IF NOT EXISTS pipeline.run_state (
    id                SERIAL PRIMARY KEY,
    run_id            TEXT UNIQUE NOT NULL,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at          TIMESTAMPTZ,
    stopped_reason    TEXT,
    detail_pages_done INT NOT NULL DEFAULT 0,
    output_s3_key     TEXT                             -- audit only, not read back by any logic
);

-- Download state of each fixed CDN part (DAG 1)
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

-- Parse state of each Bronze file (DAG 3)
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

-- Default timezone for the whole DB
ALTER DATABASE real_estate_dw SET timezone TO 'Asia/Ho_Chi_Minh';

-- ============================================================================
-- 2. SCHEMA silver — SCD2 (Clean zone)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS silver;

-- Computes row_hash — the single source of truth for change detection (SCD2 trigger).
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

-- Listing history, 1 row = 1 observed price version (SCD2 on 5 fields:
-- price_vnd, price_is_negotiable, is_expired, has_warning, area_m2).
CREATE TABLE IF NOT EXISTS silver.listing_history (
    listing_key BIGSERIAL PRIMARY KEY,
    listing_id BIGINT NOT NULL,
    listing_url TEXT NOT NULL,
    source_part VARCHAR(50) NOT NULL,
    source_bronze_key TEXT NOT NULL,       -- S3 key that produced this version (debug/trace)
    valid_from TIMESTAMPTZ NOT NULL,        -- SCD2 timestamp = when this version was first crawled
    valid_to TIMESTAMPTZ,                    -- NULL = never re-crawled a second time to confirm
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    last_seen_at TIMESTAMPTZ NOT NULL,        -- updated on re-crawl when the hash hasn't changed

    title TEXT NOT NULL,                       -- audit only, not used in Gold
    listing_type VARCHAR(10) NOT NULL,
    property_type VARCHAR(50) NOT NULL,
    posted_date DATE NOT NULL,                  -- main trend axis used in Gold

    price_vnd NUMERIC(16,0),                     -- NULL when price_is_negotiable=TRUE
    price_raw TEXT,
    price_is_negotiable BOOLEAN NOT NULL DEFAULT FALSE,

    area_m2 NUMERIC(10,2),
    area_raw TEXT,
    area_is_undetermined BOOLEAN NOT NULL DEFAULT FALSE,
    area_is_outlier BOOLEAN NOT NULL DEFAULT FALSE,   -- raw area_m2 outside [3, 10,000] m2, nulled out

    price_per_m2_vnd NUMERIC(15,2)
        GENERATED ALWAYS AS (
            CASE WHEN area_is_undetermined OR area_m2 IS NULL OR area_m2=0 OR price_vnd IS NULL
                 THEN NULL ELSE ROUND(price_vnd/area_m2,2) END
        ) STORED,

    price_is_outlier BOOLEAN NOT NULL DEFAULT FALSE,   -- price/m2 > 5B VND; price_vnd kept as-is, not nulled

    length_m NUMERIC(6,2),
    width_m NUMERIC(6,2),
    street_width_m NUMERIC(6,2),
    floors SMALLINT,
    bedrooms SMALLINT,

    orientation VARCHAR(20) NOT NULL DEFAULT '',
    legal_status VARCHAR(50) NOT NULL DEFAULT '',

    -- Tri-state: TRUE = check icon present | NULL = undetermined (site has no negation marker)
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

    address_old_raw TEXT NOT NULL DEFAULT '',    -- Silver audit only, not carried into Gold
    address_ward_old VARCHAR(100) NOT NULL DEFAULT '',
    address_district_old VARCHAR(100) NOT NULL DEFAULT '',
    address_province_old VARCHAR(100) NOT NULL DEFAULT '',   -- NOT used for scope filtering

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

-- Convention: STRING columns are NOT NULL DEFAULT '' when missing (declared
-- directly in the DDL so violations surface immediately instead of drifting
-- into Gold). NUMERIC/BOOLEAN columns keep NULL to mean "missing".

CREATE UNIQUE INDEX IF NOT EXISTS ux_listing_history_current       -- guarantees exactly 1 is_current row per listing_id
    ON silver.listing_history (listing_id)
    WHERE is_current;

CREATE INDEX IF NOT EXISTS idx_listing_history_id_valid_from        -- supports LAG() during the SCD2 merge
    ON silver.listing_history (listing_id, valid_from);

-- Temporary landing zone for one Spark parse batch. Same columns as
-- listing_history, minus the ones only generated when merged into it
-- (listing_key, valid_from, valid_to, is_current, last_seen_at,
-- ingested_at) — crawl_date is kept instead, as the source for valid_from.
CREATE UNLOGGED TABLE IF NOT EXISTS silver.listing_staging_batch (
    listing_id           BIGINT       NOT NULL,
    listing_url          TEXT         NOT NULL,
    source_part          VARCHAR(50)  NOT NULL,
    source_bronze_key    TEXT         NOT NULL,
    crawl_date           TIMESTAMPTZ  NOT NULL,   -- becomes valid_from when merged into listing_history

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
    'Temporary per-batch landing zone, UNLOGGED, TRUNCATEd before every run — losing data on a crash is acceptable since Bronze is immutable and re-running the ETL is enough.';

CREATE INDEX IF NOT EXISTS idx_staging_listing_id_crawl_date
    ON silver.listing_staging_batch (listing_id, crawl_date);

-- Records that failed to parse from Bronze (malformed HTML, missing required field...)
CREATE TABLE IF NOT EXISTS silver.parse_quarantine (
    id                 BIGSERIAL    PRIMARY KEY,
    url                TEXT         NOT NULL,
    crawl_date         TIMESTAMPTZ  NOT NULL,
    source_bronze_key  TEXT         NOT NULL,
    error_reason       TEXT         NOT NULL,
    raw_html           BYTEA,                    -- kept for debugging, avoids re-reading from S3
    quarantined_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_parse_quarantine_source_key
    ON silver.parse_quarantine (source_bronze_key);

-- ============================================================================
-- 3. SCHEMA gold — Kimball Star Schema (Observation-grain Fact, 1:1 with listing_history)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS gold;

-- DIM_DATE — date dimension.
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key      INTEGER      PRIMARY KEY,   -- YYYYMMDD
    full_date     DATE         NOT NULL UNIQUE
);

-- DIM_LOCATION — address dimension.
CREATE TABLE IF NOT EXISTS gold.dim_location (
    location_key    BIGSERIAL     PRIMARY KEY,
    province_new     VARCHAR(100)  NOT NULL DEFAULT '',
    ward_new         VARCHAR(100)  NOT NULL DEFAULT '',
    province_old      VARCHAR(100)  NOT NULL DEFAULT '',   -- may DIFFER from province_new (administrative merger)
    ward_old         VARCHAR(100)  NOT NULL DEFAULT '',
    district_old      VARCHAR(100)  NOT NULL DEFAULT '',
    street            VARCHAR(200)  NOT NULL DEFAULT '',

    CONSTRAINT uq_dim_location UNIQUE (province_new, ward_new, province_old, ward_old, district_old, street)
);

-- DIM_PROPERTY_TYPE — 10 fixed combinations (5 property_type x 2 listing_type).
CREATE TABLE IF NOT EXISTS gold.dim_property_type (
    property_type_key    BIGSERIAL     PRIMARY KEY,
    property_type_name    VARCHAR(50)   NOT NULL,
    listing_type           VARCHAR(10)   NOT NULL,

    CONSTRAINT uq_dim_property_type UNIQUE (property_type_name, listing_type)
);

-- ----------------------------------------------------------------------
-- Infers 'source' ('dataset'|'web') from the source_bronze_key prefix.
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gold.infer_source_from_bronze_key(p_source_bronze_key TEXT)
RETURNS VARCHAR(20)
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN p_source_bronze_key LIKE 'bronze/dataset/%' THEN 'dataset'
        WHEN p_source_bronze_key LIKE 'bronze/web/%' THEN 'web'
        ELSE NULL
    END
$$;

-- DIM_SOURCE — lineage (dataset vs web).
CREATE TABLE IF NOT EXISTS gold.dim_source (
    source_key    BIGSERIAL     PRIMARY KEY,
    source_name    VARCHAR(20)   NOT NULL,   -- 'dataset' | 'web'
    source_part     VARCHAR(50)   NOT NULL,

    CONSTRAINT uq_dim_source UNIQUE (source_name, source_part)
);

COMMENT ON COLUMN gold.dim_source.source_name IS
    'Inferred from the source_bronze_key prefix via gold.infer_source_from_bronze_key() (SQL) / infer_source_from_bronze_key() (Python, parser/bronze_to_silver_core.py) — changing the S3 key convention requires updating BOTH functions in sync.';

-- DIM_PROPERTY_FEATURES — junk dimension.
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

CREATE TABLE IF NOT EXISTS gold.dim_property_features (
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

-- FACT_LISTING_PRICE — 1 row = 1 observed price version, 1:1 with silver.listing_history.
CREATE TABLE IF NOT EXISTS gold.fact_listing_price (
    listing_key       BIGINT        PRIMARY KEY,   -- maps directly to silver.listing_history.listing_key
    listing_id         BIGINT        NOT NULL,       -- degenerate dimension, traces back to Silver/Bronze

    location_key         BIGINT   NOT NULL REFERENCES gold.dim_location (location_key),
    property_type_key     BIGINT   NOT NULL REFERENCES gold.dim_property_type (property_type_key),
    feature_key           CHAR(32) NOT NULL REFERENCES gold.dim_property_features (feature_key),
    source_key             BIGINT   NOT NULL REFERENCES gold.dim_source (source_key),
    posted_date_key         INTEGER  NOT NULL REFERENCES gold.dim_date (date_key),

    valid_from        TIMESTAMPTZ  NOT NULL,   -- lineage/audit only, not the trend axis
    valid_to          TIMESTAMPTZ,
    is_current        BOOLEAN      NOT NULL,

    price_vnd            NUMERIC(16, 0),          -- NULL when price_is_negotiable=TRUE
    price_per_m2_vnd       NUMERIC(15, 2),          -- copied from Silver, not recomputed
    area_m2                NUMERIC(10, 2),

    bedrooms              SMALLINT,
    floors                SMALLINT,
    length_m              NUMERIC(6, 2),
    width_m                NUMERIC(6, 2),
    street_width_m          NUMERIC(6, 2),

    price_is_negotiable      BOOLEAN NOT NULL,
    price_is_outlier            BOOLEAN NOT NULL,   -- mirrors Silver, filter on this for AVG/SUM
    area_is_undetermined      BOOLEAN NOT NULL,
    area_is_outlier            BOOLEAN NOT NULL,
    has_warning              BOOLEAN NOT NULL,
    is_expired               BOOLEAN NOT NULL
);

COMMENT ON TABLE gold.fact_listing_price IS
    'When computing AVG(price_per_m2_vnd), always filter price_is_negotiable=false and price_is_outlier=false explicitly.';

CREATE INDEX IF NOT EXISTS idx_fact_location            ON gold.fact_listing_price (location_key);
CREATE INDEX IF NOT EXISTS idx_fact_property_type       ON gold.fact_listing_price (property_type_key);
CREATE INDEX IF NOT EXISTS idx_fact_posted_date         ON gold.fact_listing_price (posted_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_listing_id          ON gold.fact_listing_price (listing_id);
CREATE INDEX IF NOT EXISTS idx_fact_current             ON gold.fact_listing_price (is_current) WHERE is_current;

-- ============================================================================
-- 4. BI layer (Metabase) — does NOT touch the underlying Silver/Gold data
-- ============================================================================

-- Old district -> GeoJSON-matching name crosswalk (older snapshot, before
-- district-to-city upgrades)
CREATE TABLE IF NOT EXISTS gold.map_district_geo_crosswalk (
    district_old         VARCHAR(100)  PRIMARY KEY,
    geojson_ten_day_du     VARCHAR(100)  NOT NULL,
    ghi_chu                TEXT
);

-- NOTE: the values below are real Vietnamese administrative place names —
-- intentionally left untranslated, they must match GeoJSON map data exactly.
INSERT INTO gold.map_district_geo_crosswalk (district_old, geojson_ten_day_du, ghi_chu) VALUES
    ('Thành phố Bến Cát',  'Thị xã Bến Cát',  'GeoJSON snapshot predates 2024-05-01'),
    ('Thành phố Dĩ An',    'Thị xã Dĩ An',    'GeoJSON snapshot predates 2020'),
    ('Thành phố Thuận An', 'Thị xã Thuận An', 'GeoJSON snapshot predates 2020'),
    ('Thành phố Tân Uyên', 'Thị xã Tân Uyên', 'GeoJSON snapshot predates 2023'),
    ('Thành phố Phú Mỹ',   'Huyện Phú Mỹ',    'GeoJSON snapshot predates the district-to-city upgrade')
ON CONFLICT (district_old) DO NOTHING;

-- New ward -> GeoJSON-matching name crosswalk (data quality issue from the alonhadat source)
CREATE TABLE IF NOT EXISTS gold.map_ward_geo_crosswalk (
    ward_new              VARCHAR(100)  PRIMARY KEY,
    geojson_ten_day_du     VARCHAR(100)  NOT NULL,
    ghi_chu                TEXT
);

-- NOTE: values below are real Vietnamese administrative place names — kept untranslated.
INSERT INTO gold.map_ward_geo_crosswalk (ward_new, geojson_ten_day_du, ghi_chu) VALUES
    ('Phường Hóc Môn', 'Xã Hóc Môn', 'Official name per Resolution 1685/NQ-UBTVQH15 (2025-07-01) is Xã Hóc Môn')
ON CONFLICT (ward_new) DO NOTHING;

-- Flat view — the primary source for every Metabase Question. Grain = fact_listing_price.
-- Normalizes Unicode to NFC on both sides when matching against GeoJSON (avoids NFC/NFD mismatches).
CREATE OR REPLACE VIEW gold.vw_fact_report AS
SELECT
    f.listing_key, f.listing_id,
    f.price_vnd, f.price_per_m2_vnd, f.area_m2, f.bedrooms, f.floors,
    f.length_m, f.width_m, f.street_width_m,
    f.price_is_negotiable, f.price_is_outlier, f.area_is_undetermined, f.area_is_outlier,
    f.has_warning, f.is_expired, f.is_current, f.valid_from, f.valid_to,

    dd.full_date  AS posted_date,

    dl.province_new, dl.ward_new, dl.province_old, dl.ward_old, dl.district_old, dl.street,

    -- Map key for Metabase region maps (already passed through the crosswalk + NFC where applicable)
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
