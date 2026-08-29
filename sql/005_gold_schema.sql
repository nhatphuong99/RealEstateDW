-- ============================================================================
-- sql/005_gold_schema.sql
-- Star Schema Kimball — Fact loại Transaction/Observation-grain.
--
-- Grain fact_listing_price: 1 dòng = 1 version giá ĐÃ QUAN SÁT ĐƯỢC (khớp
-- 1:1 với silver.listing_history, KHÔNG fan-out theo ngày kiểu Periodic
-- Snapshot). Lý do chọn grain này thay vì Periodic Snapshot: crawler chỉ
-- enqueue mỗi URL đúng 1 lần (crawl.detail_queue UNIQUE(url)), thứ tự quét
-- không theo thời gian đăng tin, nhiều tin chỉ được quan sát đúng 1 lần
-- hoặc không bao giờ được quan sát lại — hệ thống KHÔNG có đủ bằng chứng để
-- khẳng định "1 listing còn active vào ngày X" cho MỌI ngày giữa 2 lần
-- crawl. Biểu diễn Periodic Snapshot (1 dòng/ngày) sẽ buộc phải NỘI SUY sự
-- tồn tại của tin trong những ngày không hề được crawl — sai với thực tế đã
-- quan sát được. Transaction/Observation-grain giữ đúng những gì đã biết,
-- KHÔNG suy diễn thêm. Chi tiết lý do chọn kiến trúc: xem
-- kien_truc_tong_hop_he_thong.md mục "Vì sao Transaction/Observation-grain".
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- ----------------------------------------------------------------------------
-- DIM_DATE — Type 0. Role-playing: observed_date_key (thuộc GRAIN của Fact —
-- ngày quan sát version giá này) và posted_date_key (thông tin phụ — ngày
-- site tự ghi nhận đăng tin, KHÔNG thuộc grain) cùng trỏ bảng này.
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
    'Bảng ngày chuẩn Kimball, Type 0. Role-playing: observed_date_key (NGÀY QUAN SÁT — '
    'thuộc grain của fact_listing_price, = valid_from::date) và posted_date_key '
    '(ngày đăng tin gốc — thông tin phụ, KHÔNG thuộc grain). Không tạo view riêng — '
    'tầng BI tự alias khi JOIN 2 lần vào bảng này.';

-- ----------------------------------------------------------------------------
-- DIM_LOCATION — Type 1 (KHÔNG SCD2). Grain: 1 tổ hợp địa chỉ duy nhất.
--
-- Không cần SCD2: đổi địa chỉ giữa các version SCD2 của Silver rất hiếm
-- (xác nhận qua data thực tế), và vì mỗi dòng Fact (grain = listing_key) tự
-- resolve location_key riêng tại thời điểm ETL chạy, lịch sử địa chỉ (nếu
-- có) đã tự nhiên được bảo toàn qua Fact — thêm SCD2 ở Dim sẽ trùng lặp
-- logic không cần thiết.
--
-- province_new và province_old TÁCH RIÊNG (không gộp 1 cột) — có tin đăng
-- tỉnh/thành CŨ khác tỉnh/thành MỚI do sáp nhập địa giới hành chính (VD: Xã
-- Long Điền — cũ thuộc 'Bà Rịa Vũng Tàu', mới thuộc 'Hồ Chí Minh').
-- address_old_raw (text thô, chưa parse) CHỈ lưu ở Silver để audit, KHÔNG
-- đưa xuống Gold.
--
-- QUY ƯỚC: cột chuỗi NOT NULL DEFAULT '' — khớp quy ước tại Silver (xem
-- 003_silver_listings_history.sql) -> UPSERT (INSERT ... ON CONFLICT) hoạt
-- động idempotent đúng nghĩa, không cần chuẩn hóa NULL riêng ở ETL Gold.
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_location (
    location_key    BIGSERIAL     PRIMARY KEY,
    province_new     VARCHAR(100)  NOT NULL DEFAULT '',   -- Hiện tại luôn 'Hồ Chí Minh' (theo is_in_scope()).
    ward_new         VARCHAR(100)  NOT NULL DEFAULT '',   -- Phường/Xã theo địa giới MỚI - khóa nhóm chính.
    province_old      VARCHAR(100)  NOT NULL DEFAULT '',   -- Tỉnh/Thành theo địa giới CŨ - có thể KHÁC province_new.
    ward_old         VARCHAR(100)  NOT NULL DEFAULT '',   -- Phường/Xã theo địa giới CŨ.
    district_old      VARCHAR(100)  NOT NULL DEFAULT '',   -- Quận/Huyện - chỉ có ở địa chỉ cũ.
    street            VARCHAR(200)  NOT NULL DEFAULT '',   -- Mô tả thêm, KHÔNG dùng để GROUP BY.

    CONSTRAINT uq_dim_location UNIQUE (province_new, ward_new, province_old, ward_old, district_old, street)
);

COMMENT ON TABLE gold.dim_location IS
    'Địa điểm, Type 1. GROUP BY ward_new cho phân tích chuẩn theo địa giới mới; '
    'filter theo ward_old/district_old/province_old cho người dùng quen địa chỉ cũ. '
    'address_old_raw KHÔNG có ở Gold, chỉ audit ở Silver.';
COMMENT ON COLUMN gold.dim_location.province_old IS
    'Tỉnh/thành theo địa giới CŨ trước sáp nhập — KHÔNG dùng để lọc scope đồ án '
    '(is_in_scope() ở parser chỉ dùng address_province_new). Có thể khác province_new.';

-- ----------------------------------------------------------------------------
-- DIM_PROPERTY_TYPE — Type 0, đúng 10 tổ hợp cố định (5 property_type x 2 listing_type).
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_property_type (
    property_type_key    BIGSERIAL     PRIMARY KEY,
    property_type_name    VARCHAR(50)   NOT NULL,
    listing_type           VARCHAR(10)   NOT NULL,   -- 'Cần bán' | 'Cho thuê' — giữ nguyên tiếng Việt như Silver.

    CONSTRAINT uq_dim_property_type UNIQUE (property_type_name, listing_type)
);

-- ----------------------------------------------------------------------------
-- DIM_SOURCE — Type 0.
-- ----------------------------------------------------------------------------
CREATE TABLE gold.dim_source (
    source_key    BIGSERIAL     PRIMARY KEY,
    source_name    VARCHAR(20)   NOT NULL,   -- 'dataset' | 'web' — giữ nguyên như crawl.bronze_file_state.source.
    source_part     VARCHAR(50)   NOT NULL,

    CONSTRAINT uq_dim_source UNIQUE (source_name, source_part)
);

COMMENT ON COLUMN gold.dim_source.source_name IS
    'Giữ nguyên giá trị ngắn gọn như crawl.bronze_file_state.source '
    '(quyết định: không dịch sang dataset_cdn/web_crawl). Suy từ prefix source_bronze_key qua '
    'CASE/LIKE trong etl_silver_to_gold.sql — duplicate có chủ đích của logic Python '
    'infer_source_from_bronze_key() (SQL không gọi được hàm Python). Đổi convention đặt tên '
    'S3 key PHẢI sửa đồng bộ CẢ 2 nơi.';

-- ----------------------------------------------------------------------------
-- DIM_PROPERTY_FEATURES — Junk Dimension, Type 1 (KHÔNG SCD2). 7 trường
-- categorical/flag, KHÔNG gồm bedrooms/floors (numeric, range-filter trực
-- tiếp trên Fact). Không SCD2 vì các field này KHÔNG nằm trong 5 trường
-- trigger SCD2 của Silver (price_vnd/price_is_negotiable/is_expired/
-- has_warning/area_m2) — bản thân Silver cũng không lưu lịch sử của chúng,
-- thêm SCD2 ở Gold sẽ tạo ảo giác lịch sử không có thật.
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

-- ============================================================================
-- FACT_LISTING_PRICE — Transaction/Observation-grain.
-- Grain: 1 dòng = 1 version giá ĐÃ QUAN SÁT ĐƯỢC, khớp 1:1 silver.listing_history.
-- ============================================================================
CREATE TABLE gold.fact_listing_price (
    listing_key       BIGINT        PRIMARY KEY,   -- Map thẳng từ silver.listing_history.listing_key.
    listing_id         BIGINT        NOT NULL,       -- Degenerate dimension, trace ngược về Silver/Bronze.
    listing_url         TEXT          NOT NULL,

    -- Khóa ngoại tới các dimension
    location_key         BIGINT   NOT NULL REFERENCES gold.dim_location (location_key),
    property_type_key     BIGINT   NOT NULL REFERENCES gold.dim_property_type (property_type_key),
    feature_key           CHAR(32) NOT NULL REFERENCES gold.dim_property_features (feature_key),
    source_key             BIGINT   NOT NULL REFERENCES gold.dim_source (source_key),

    -- Role-playing date: observed_date_key dùng cho MỌI phân tích xu hướng
    -- (GROUP BY tại tầng BI, khuyến nghị bucket theo tuần/tháng — mật độ
    -- quan sát thưa và không đều, xem comment cột). posted_date_key chỉ là
    -- thông tin phụ (ngày site tự ghi nhận đăng tin).
    observed_date_key           INTEGER NOT NULL REFERENCES gold.dim_date (date_key),
    posted_date_key               INTEGER NOT NULL REFERENCES gold.dim_date (date_key),

    -- Temporal metadata
    valid_from        TIMESTAMPTZ  NOT NULL,   -- = thời điểm quan sát đầu tiên (crawl_date).
    valid_to          TIMESTAMPTZ,             -- NULL ở tuyệt đại đa số dòng — xem COMMENT cột.
    last_seen_at      TIMESTAMPTZ  NOT NULL,   -- Lần xác nhận gần nhất (có thể = valid_from).
    is_current        BOOLEAN      NOT NULL,

    -- Cờ chất lượng quan sát: giúp lọc/đánh giá độ tin cậy khi phân tích.
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
    'Transaction/Observation-grain Fact. Grain: 1 dòng = 1 version giá đã quan sát được — '
    'khớp 1:1 silver.listing_history, KHÔNG fan-out theo ngày. Khi tính AVG(price_per_m2_vnd) '
    'nên lọc price_is_negotiable=false và price_is_outlier=false tường minh (AVG/SUM tự bỏ NULL '
    'nên không bắt buộc, nhưng lọc rõ cho dễ đọc query).';
COMMENT ON COLUMN gold.fact_listing_price.price_vnd IS
    'NULL khi price_is_negotiable=TRUE — nhất quán với silver.listing_history, '
    'KHÔNG map sang 0, tránh rủi ro quên filter price_is_negotiable khi tính AVG/SUM.';
COMMENT ON COLUMN gold.fact_listing_price.price_is_outlier IS
    'Mirror từ silver.listing_history.price_is_outlier — TRUE khi price_vnd/area_m2 > 5 tỷ/m2. '
    'price_vnd KHÔNG bị null hóa — lọc bằng cờ này khi tính AVG/dashboard thay vì loại bỏ dữ liệu.';
COMMENT ON COLUMN gold.fact_listing_price.area_is_outlier IS
    'Mirror từ silver.listing_history.area_is_outlier — TRUE khi area_m2 gốc >10.000m2 hoặc <3m2, đã null hóa ở Silver.';
COMMENT ON COLUMN gold.fact_listing_price.valid_to IS
    'NULL ở tuyệt đại đa số dòng — KHÔNG có nghĩa "tin vẫn đang active", mà có nghĩa "chưa từng '
    'có lần quan sát thứ 2 để xác nhận thay đổi hay không" (crawl.detail_queue chỉ enqueue mỗi '
    'URL đúng 1 lần qua UNIQUE(url) — phần lớn tin không bao giờ được crawl lại). '
    'TUYỆT ĐỐI KHÔNG suy diễn "còn hiệu lực tới hiện tại" từ NULL này.';
COMMENT ON COLUMN gold.fact_listing_price.is_current IS
    'Nghĩa CHÍNH XÁC: "chưa từng phát hiện version mới hơn thay thế". KHÔNG đảm bảo tin thực sự '
    'vẫn còn tồn tại trên site — vì phần lớn chưa từng được crawl lại để kiểm chứng '
    '(is_reconfirmed=FALSE).';
COMMENT ON COLUMN gold.fact_listing_price.is_reconfirmed IS
    'TRUE khi listing có ≥2 lần quan sát (last_seen_at > valid_from), nghĩa là có bằng chứng '
    'xác nhận thực tế (không chỉ suy diễn từ 1 lần crawl duy nhất). Dùng để lọc khi cần độ tin '
    'cậy cao hơn, hoặc hiển thị KPI "% dữ liệu có kiểm chứng lại" trên dashboard — minh bạch về '
    'giới hạn độ phủ của crawler.';
COMMENT ON COLUMN gold.fact_listing_price.observed_date_key IS
    'Ngày quan sát ĐẦU TIÊN (= valid_from::date). Dùng làm trục thời gian chính cho mọi phân tích '
    'xu hướng — nên GROUP BY theo tuần/tháng, không theo ngày (mật độ quan sát thưa và không đều, '
    'không phản ánh true daily inventory — xem lý do chọn grain ở đầu file).';
COMMENT ON COLUMN gold.fact_listing_price.posted_date_key IS
    'Ngày đăng tin (site tự ghi nhận) — thông tin phụ, KHÔNG dùng làm trục phân tích xu hướng chính.';

-- Index phục vụ truy vấn chính: giá theo Phường-Xã/Loại hình/Thời gian
CREATE INDEX idx_fact_location            ON gold.fact_listing_price (location_key);
CREATE INDEX idx_fact_property_type       ON gold.fact_listing_price (property_type_key);
CREATE INDEX idx_fact_observed_date       ON gold.fact_listing_price (observed_date_key);
CREATE INDEX idx_fact_posted_date         ON gold.fact_listing_price (posted_date_key);
CREATE INDEX idx_fact_listing_id          ON gold.fact_listing_price (listing_id);
CREATE INDEX idx_fact_current             ON gold.fact_listing_price (is_current) WHERE is_current;
