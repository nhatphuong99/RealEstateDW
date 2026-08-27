-- ============================================================================
-- sql/004_etl_bronze_to_silver_control.sql
-- Control-plane + staging + quarantine cho DAG 3 (bronze_to_silver).
-- 3 bảng: bronze_file_state (trạng thái parse), listing_staging_batch (landing zone),
-- parse_quarantine (ghi lỗi parse). Idempotent. Phải chạy sau 003_silver_listings_history.sql.
-- ============================================================================

-- 1. Control-plane: trạng thái parse file Bronze
CREATE TABLE IF NOT EXISTS crawl.bronze_file_state (
    s3_key            TEXT PRIMARY KEY,
    source            TEXT NOT NULL,                    -- 'dataset' | 'web'
    status            TEXT NOT NULL DEFAULT 'pending',   -- pending/processing/done/failed
    rows_parsed       INT,
    rows_quarantined  INT,
    discovered_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at      TIMESTAMPTZ,
    last_error        TEXT
);

CREATE INDEX IF NOT EXISTS idx_bronze_file_state_pending
    ON crawl.bronze_file_state (discovered_at)
    WHERE status = 'pending';

COMMENT ON TABLE crawl.bronze_file_state IS
    'Control-plane cho DAG 3 (bronze_to_silver). Khác với crawl.dataset_part_state '
    '(theo dõi trạng thái TẢI file lên S3) — bảng này theo dõi trạng thái PARSE vào Silver.';


-- 2. Staging: landing zone tạm cho 1 lần chạy Spark parse job
CREATE UNLOGGED TABLE IF NOT EXISTS silver.listing_staging_batch (
    listing_id           BIGINT       NOT NULL,
    listing_url           TEXT         NOT NULL,
    source_part           VARCHAR(50)  NOT NULL,
    source_bronze_key     TEXT         NOT NULL,
    crawl_date            TIMESTAMPTZ  NOT NULL,

    title                 TEXT         NOT NULL,
    listing_type           VARCHAR(10)  NOT NULL,
    property_type          VARCHAR(50)  NOT NULL,
    posted_date            DATE         NOT NULL,

    -- price_vnd NULL khi price_is_negotiable=TRUE — đồng bộ quyết định
    -- với silver.listing_history (KHÔNG dùng 0).
    price_vnd              NUMERIC(16, 0),
    price_raw               TEXT,
    price_is_negotiable      BOOLEAN      NOT NULL DEFAULT FALSE,

    area_m2                  NUMERIC(10, 2),
    area_raw                  TEXT,
    area_is_undetermined       BOOLEAN      NOT NULL DEFAULT FALSE,
    area_is_outlier              BOOLEAN      NOT NULL DEFAULT FALSE,   -- MỚI, đồng bộ với silver.listing_history

    length_m                 NUMERIC(6, 2),
    width_m                   NUMERIC(6, 2),
    street_width_m             NUMERIC(6, 2),
    floors                    SMALLINT,
    bedrooms                  SMALLINT,

    orientation               VARCHAR(20),
    legal_status               VARCHAR(50),

    has_dining_room             BOOLEAN,
    has_kitchen                 BOOLEAN,
    has_rooftop                 BOOLEAN,
    has_car_parking              BOOLEAN,
    owner_direct                BOOLEAN,

    is_expired                 BOOLEAN      NOT NULL DEFAULT FALSE,
    has_warning                 BOOLEAN      NOT NULL DEFAULT FALSE,

    address_street_new           VARCHAR(200),
    address_ward_new             VARCHAR(100),
    address_province_new          VARCHAR(100),

    address_old_raw                TEXT,
    address_ward_old               VARCHAR(100),
    address_district_old            VARCHAR(100),

    -- row_hash: GENERATED, gọi CÙNG hàm silver.compute_row_hash() như bên
    -- listing_history (tạo trong 003_silver_listings_history.sql)
    row_hash                CHAR(32)
        GENERATED ALWAYS AS (
            silver.compute_row_hash(price_vnd, price_is_negotiable, is_expired, has_warning, area_m2)
        ) STORED,

    CONSTRAINT chk_staging_price_negotiable_null CHECK (
        (price_is_negotiable = TRUE  AND price_vnd IS NULL)
        OR
        (price_is_negotiable = FALSE AND price_vnd IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_staging_listing_id_crawl_date
    ON silver.listing_staging_batch (listing_id, crawl_date);

COMMENT ON TABLE silver.listing_staging_batch IS
    'Landing zone tạm cho 1 lần chạy Spark parse job. TRUNCATE trước mỗi batch. '
    'UNLOGGED — chấp nhận mất dữ liệu khi crash vì Bronze vẫn immutable, chạy lại ETL là đủ.';

-- 3. Quarantine: bản ghi parse thất bại
CREATE TABLE IF NOT EXISTS silver.parse_quarantine (
    id                 BIGSERIAL    PRIMARY KEY,
    url                 TEXT         NOT NULL,
    crawl_date           TIMESTAMPTZ  NOT NULL,
    source_bronze_key     TEXT         NOT NULL,
    error_reason           TEXT         NOT NULL,   -- vd: "numeric overflow area_m2", "missing price element"
    raw_html                 BYTEA,                   -- giữ lại để debug, khỏi phải đọc lại từ S3
    quarantined_at             TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_parse_quarantine_source_key
    ON silver.parse_quarantine (source_bronze_key);

COMMENT ON TABLE silver.parse_quarantine IS
    'Bản ghi Bronze không parse được (HTML lệch cấu trúc, numeric overflow, thiếu field bắt buộc...). '
    'Ghi lại thay vì crash batch — review thủ công định kỳ để phát hiện thay đổi cấu trúc trang.';
