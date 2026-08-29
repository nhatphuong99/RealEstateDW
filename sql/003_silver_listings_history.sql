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
    'Công thức row_hash duy nhất cho Silver layer, dùng chung cho listing_history và staging_batch.';

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

    -- Cờ giá/m2 bất thường: TRUE khi price_vnd/area_m2 > 5 tỷ/m2. price_vnd
    -- GIỮ NGUYÊN (không null hóa, khác area_is_outlier) — tránh đụng CHECK
    -- constraint chk_price_negotiable_null và tránh lẫn với "Thỏa thuận" thật
    -- (price_value=0). Do parser tính (_detect_price_outlier), dùng area_m2
    -- ĐÃ sanitize — KHÔNG làm GENERATED vì phụ thuộc area_m2 sau khi đã bị
    -- null hóa bởi _sanitize_area(), không tự suy lại được từ 2 cột lưu sẵn.
    price_is_outlier BOOLEAN NOT NULL DEFAULT FALSE,

    -- Kích thước (đã sanitize ở tầng parser: null hóa nếu ngoài ngưỡng hợp lý vật lý)
    length_m NUMERIC(6,2),
    width_m NUMERIC(6,2),
    street_width_m NUMERIC(6,2),
    floors SMALLINT,
    bedrooms SMALLINT,

    -- Đặc điểm (chuỗi tự do, KHÔNG enum cứng — "" khi thiếu, xem quy ước cột chuỗi bên dưới)
    orientation VARCHAR(20) NOT NULL DEFAULT '',
    legal_status VARCHAR(50) NOT NULL DEFAULT '',

    -- Tiện ích (tri-state: TRUE = có icon check | NULL = không xác định — site không có ký hiệu phủ định)
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

    -- Địa chỉ cũ (address_old_raw chỉ audit ở Silver, KHÔNG đưa xuống Gold)
    address_old_raw TEXT NOT NULL DEFAULT '',
    address_ward_old VARCHAR(100) NOT NULL DEFAULT '',
    address_district_old VARCHAR(100) NOT NULL DEFAULT '',
    address_province_old VARCHAR(100) NOT NULL DEFAULT '',   -- tỉnh/thành cũ, CÓ THỂ khác address_province_new — xem comment cột

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
    'Lịch sử tin đăng BĐS (Silver layer). Grain: 1 dòng = 1 phiên bản giá của 1 tin đăng. '
    'SCD Type 2 áp dụng trên 5 trường: price_vnd, price_is_negotiable, is_expired, has_warning, area_m2.';

COMMENT ON COLUMN silver.listing_history.price_vnd IS
    'NULL khi price_is_negotiable=TRUE (KHÔNG dùng 0 — xem CHECK constraint chk_price_negotiable_null). '
    'NULL giúp AVG()/SUM() ở Gold tự loại trừ tin thỏa thuận mà không cần filter thủ công.';
COMMENT ON COLUMN silver.listing_history.price_is_outlier IS
    'TRUE khi price_vnd/area_m2 (đã sanitize) > 5 tỷ/m2 — ngưỡng đối chiếu benchmark thực tế '
    '(căn hộ cao cấp HCMC ~55-85 triệu/m2, đất mặt tiền đắt nhất trung tâm Q1 ~1-2 tỷ/m2). '
    'price_vnd GIỮ NGUYÊN, KHÔNG null hóa — lọc bằng cờ này ở tầng Gold/dashboard, không xóa dữ liệu.';
COMMENT ON COLUMN silver.listing_history.area_is_outlier IS
    'TRUE khi area_m2 gốc parse được nhưng > 10.000m2 (ngoài phạm vi đồ án, không gồm đất nền) '
    'HOẶC < 3m2 (nghi field khác bị nhầm vào ô diện tích) — đã bị null hóa tại tầng parser (_sanitize_area).';
COMMENT ON COLUMN silver.listing_history.row_hash IS
    'GENERATED STORED, gọi silver.compute_row_hash() — 1 nguồn sự thật duy nhất, dùng chung với '
    'silver.listing_staging_batch. Parser Spark/Python KHÔNG tự tính MD5.';
COMMENT ON COLUMN silver.listing_history.source_bronze_key IS
    'S3 key chính xác của file Bronze sinh ra version này — dùng để debug/truy vết khi SCD2 merge sai.';
COMMENT ON COLUMN silver.listing_history.address_province_old IS
    'Tỉnh/thành theo địa giới CŨ — có thể KHÁC address_province_new khi tin đăng thuộc khu vực bị '
    'sáp nhập địa giới hành chính (VD: Xã Long Điền — cũ thuộc ''Bà Rịa Vũng Tàu'', mới thuộc '
    '''Hồ Chí Minh''). TUYỆT ĐỐI KHÔNG dùng cột này để lọc scope (is_in_scope() chỉ dùng address_province_new).';
COMMENT ON COLUMN silver.listing_history.crawl_date IS
    'Mốc nguồn (Bronze): thời điểm HTML thực tế được crawl về. Khác với valid_from (mốc nghiệp vụ SCD2).';
COMMENT ON COLUMN silver.listing_history.valid_from IS
    'Mốc SCD2: = crawl_date của lần crawl đầu tiên phát hiện ra tổ hợp giá trị (row_hash) này. '
    'Cũng chính là mốc "quan sát đầu tiên" dùng làm observed_date_key ở Gold (Transaction/Observation-grain).';
COMMENT ON COLUMN silver.listing_history.valid_to IS
    'NULL ở tuyệt đại đa số dòng — KHÔNG có nghĩa "tin vẫn còn hiệu lực tới hiện tại", mà có nghĩa '
    '"chưa từng có lần quan sát thứ 2 để xác nhận thay đổi" (crawl.detail_queue chỉ enqueue mỗi URL '
    'đúng 1 lần qua UNIQUE(url) — không phải mọi tin đều được crawl lại). Xem chi tiết ở '
    'kien_truc_tong_hop_he_thong.md mục "Giới hạn độ phủ crawler".';
COMMENT ON COLUMN silver.listing_history.last_seen_at IS
    'Cập nhật mỗi lần crawl lại mà row_hash không đổi - KHÔNG sinh phiên bản mới, chỉ update tại chỗ trên bản hiện tại.';

-- Quy ước cột kiểu CHUỖI: NOT NULL DEFAULT '' (thay vì NULL) khi thiếu dữ liệu
-- — áp dụng cho orientation/legal_status/address_* (title/listing_type/
-- property_type luôn có giá trị thật, không nằm trong quy ước này). Ràng
-- buộc NOT NULL DEFAULT '' khai báo TRỰC TIẾP ở DDL (không chỉ ở tầng parser
-- qua _text_or_empty()) để nếu parser có sai sót gửi NULL, INSERT sẽ báo lỗi
-- NGAY tại Bronze->Silver (nơi dễ debug) thay vì âm thầm trôi xuống Gold rồi
-- làm rớt dòng khỏi JOIN dim_location/dim_property_features (2 bảng đó cũng
-- NOT NULL DEFAULT '', so sánh '' = NULL luôn cho kết quả UNKNOWN trong SQL).
-- Trường SỐ (price_vnd, area_m2...) và BOOLEAN nullable (has_*...) KHÔNG áp
-- dụng quy ước này — giữ nguyên NULL đúng ngữ nghĩa "thiếu"/"không xác định".

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
