-- ============================================================================
-- sql/queries/etl_silver_to_gold.sql
-- ETL Silver -> Gold: full-refresh idempotent, chạy trong 1 transaction.
--
-- Silver và Gold cùng 1 Postgres (postgres-dw) -> transform thẳng bằng SQL,
-- KHÔNG kéo dữ liệu ra Spark/Python (khác Bronze->Silver, vốn bắt buộc cần
-- Spark để parse HTML thô ở quy mô lớn - CPU-bound, lý do không áp dụng ở
-- đây). Xem kien_truc_tong_hop_he_thong.md.
--
-- Thứ tự BẮT BUỘC: nạp 5 Dim trước (Type 0/1, idempotent qua
-- ON CONFLICT DO NOTHING), rồi mới nạp Fact (JOIN lấy surrogate key).
--
-- Fact dùng ON CONFLICT DO UPDATE (KHÔNG phải DO NOTHING): silver.listing_history
-- không bất biến hoàn toàn -- merge_scd2_listing_history.sql có thể UPDATE
-- is_current/valid_to của các dòng đã tồn tại (khi phát hiện version giá
-- mới) -> Gold phải phản ánh lại đúng, không chỉ insert-once.
--
-- PHÒNG THỦ (quan trọng): toàn bộ cột CHUỖI feed vào dim_location/
-- dim_property_features được bọc COALESCE(..., '') ở CẢ bước nạp Dim LẪN
-- bước JOIN của Fact, dù silver.listing_history đã có NOT NULL DEFAULT ''
-- ở DDL (003). Lý do bọc 2 lớp: nếu vì bất kỳ nguyên nhân gì (regression ở
-- parser, dữ liệu cũ còn sót từ trước khi thêm ràng buộc DDL...) mà 1 cột
-- chuỗi thực sự NULL, KHÔNG bọc COALESCE sẽ khiến INSERT INTO dim_location
-- ném lỗi NOT NULL violation NGAY LẬP TỨC -> abort TOÀN BỘ transaction 6
-- bước (kể cả các Dim khác đã insert đúng) -> fact_listing_price rớt về 0
-- dòng dù silver.listing_history có đầy đủ dữ liệu (đúng dạng triệu chứng
-- "row_count_match: expected=N, actual=0"). Bọc COALESCE biến 1 lỗi cứng
-- (transaction-abort) thành 1 dòng dim_location hợp lệ dạng ''/'' — vẫn có
-- thể lệch dữ liệu (dòng ghép sai nhóm ''), nhưng KHÔNG làm sập cả batch;
-- sql/queries/diagnose_gold_join_loss.sql dùng để phát hiện các dòng bị
-- COALESCE che (so khớp bản gốc IS NULL) nếu cần điều tra thêm.
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------
-- 1. DIM_DATE - lịch liên tục, phủ từ ngày nhỏ nhất đến lớn nhất xuất
--    hiện ở CẢ posted_date, valid_from lẫn last_seen_at (dùng last_seen_at
--    vì last_seen_at có thể muộn hơn valid_from khi tin được kiểm chứng
--    lại nhiều lần — is_reconfirmed=TRUE). Lịch liên tục (không sparse) để
--    dashboard vẽ trend không bị đứt gãy giữa các ngày không có version mới.
-- ----------------------------------------------------------------------
INSERT INTO gold.dim_date (date_key, full_date, day, month, quarter, year)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d,
    EXTRACT(DAY FROM d)::SMALLINT,
    EXTRACT(MONTH FROM d)::SMALLINT,
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(YEAR FROM d)::SMALLINT
FROM generate_series(
    (SELECT LEAST(MIN(posted_date), MIN(valid_from::DATE)) FROM silver.listing_history),
    (SELECT GREATEST(MAX(posted_date), MAX(valid_from::DATE), MAX(last_seen_at::DATE)) FROM silver.listing_history),
    INTERVAL '1 day'
) AS d
ON CONFLICT (date_key) DO NOTHING;

-- ----------------------------------------------------------------------
-- 2. DIM_LOCATION - Type 1 (không SCD2). COALESCE phòng thủ — xem giải
--    thích ở đầu file.
-- ----------------------------------------------------------------------
INSERT INTO gold.dim_location (province_new, ward_new, province_old, ward_old, district_old, street)
SELECT DISTINCT
    COALESCE(address_province_new, ''),
    COALESCE(address_ward_new, ''),
    COALESCE(address_province_old, ''),
    COALESCE(address_ward_old, ''),
    COALESCE(address_district_old, ''),
    COALESCE(address_street_new, '')
FROM silver.listing_history
ON CONFLICT (province_new, ward_new, province_old, ward_old, district_old, street) DO NOTHING;

-- ----------------------------------------------------------------------
-- 3. DIM_PROPERTY_TYPE - đúng 10 tổ hợp cố định (5 property_type x 2
--    listing_type). Không cần COALESCE: property_type/listing_type NOT
--    NULL vô điều kiện ở Silver (không thuộc quy ước "" — luôn có giá trị
--    thật, xem is_in_scope()).
-- ----------------------------------------------------------------------
INSERT INTO gold.dim_property_type (property_type_name, listing_type)
SELECT DISTINCT property_type, listing_type
FROM silver.listing_history
ON CONFLICT (property_type_name, listing_type) DO NOTHING;

-- ----------------------------------------------------------------------
-- 4. DIM_SOURCE - suy source_name từ prefix source_bronze_key.
--
-- QUAN TRỌNG: đây là bản DUPLICATE CÓ CHỦ ĐÍCH của logic Python
-- parser.bronze_to_silver_core.infer_source_from_bronze_key() -- SQL
-- không gọi được hàm Python. Nếu sau này đổi convention đặt tên S3 key
-- (bronze/dataset/... | bronze/web/...) PHẢI sửa đồng bộ CẢ 2 NƠI
-- (hàm Python đó + khối CASE này, lặp lại 2 lần trong file này: ở đây
-- và ở bước 6 khi JOIN Fact).
-- ----------------------------------------------------------------------
INSERT INTO gold.dim_source (source_name, source_part)
SELECT DISTINCT
    CASE
        WHEN source_bronze_key LIKE 'bronze/dataset/%' THEN 'dataset'
        WHEN source_bronze_key LIKE 'bronze/web/%' THEN 'web'
        ELSE NULL  -- không khớp prefix nào -> lộ ra qua diagnose_gold_join_loss.sql
    END AS source_name,
    source_part
FROM silver.listing_history
ON CONFLICT (source_name, source_part) DO NOTHING;

-- ----------------------------------------------------------------------
-- 5. DIM_PROPERTY_FEATURES - feature_key GENERATED STORED (Postgres tự
--    tính qua gold.compute_feature_key()), KHÔNG insert cột này tay.
--    COALESCE phòng thủ cho orientation/legal_status — xem đầu file.
-- ----------------------------------------------------------------------
INSERT INTO gold.dim_property_features (
    orientation, legal_status, has_dining_room, has_kitchen,
    has_rooftop, has_car_parking, owner_direct
)
SELECT DISTINCT
    COALESCE(orientation, ''), COALESCE(legal_status, ''),
    has_dining_room, has_kitchen, has_rooftop, has_car_parking, owner_direct
FROM silver.listing_history
ON CONFLICT (feature_key) DO NOTHING;

-- ----------------------------------------------------------------------
-- 6. FACT_LISTING_PRICE - UPSERT (không phải insert-only, xem lý do ở
--    đầu file). JOIN lấy surrogate key từ dim_location/dim_property_type/
--    dim_source vừa nạp — điều kiện JOIN phải dùng CÙNG COALESCE như bước
--    2/5, nếu không dim đã có '' nhưng Silver vẫn đưa NULL vào so sánh
--    ('' = NULL luôn UNKNOWN trong SQL) sẽ làm JOIN rớt mất dòng đó dù
--    dim tương ứng đã tồn tại đúng.
--
--    feature_key KHÔNG join dim_property_features mà gọi lại
--    gold.compute_feature_key() trực tiếp -- hàm IMMUTABLE, NULL-safe (có
--    COALESCE bên trong), tránh lỗi NULL-in-JOIN khi các cột has_*/
--    owner_direct là BOOLEAN nullable (NULL = NULL luôn UNKNOWN trong SQL,
--    JOIN theo cột sẽ làm rớt mất các dòng có cột NULL).
-- ----------------------------------------------------------------------
INSERT INTO gold.fact_listing_price (
    listing_key, listing_id, listing_url,
    location_key, property_type_key, feature_key, source_key,
    observed_date_key, posted_date_key,
    valid_from, valid_to, last_seen_at, is_current,
    price_vnd, price_per_m2_vnd, area_m2,
    bedrooms, floors, length_m, width_m, street_width_m,
    price_is_negotiable, price_is_outlier, area_is_undetermined, area_is_outlier,
    has_warning, is_expired
)
SELECT
    h.listing_key,
    h.listing_id,
    h.listing_url,
    loc.location_key,
    pt.property_type_key,
    gold.compute_feature_key(
        COALESCE(h.orientation, ''), COALESCE(h.legal_status, ''), h.has_dining_room, h.has_kitchen,
        h.has_rooftop, h.has_car_parking, h.owner_direct
    ) AS feature_key,
    src.source_key,
    TO_CHAR(h.valid_from::DATE, 'YYYYMMDD')::INTEGER AS observed_date_key,
    TO_CHAR(h.posted_date, 'YYYYMMDD')::INTEGER AS posted_date_key,
    h.valid_from,
    h.valid_to,
    h.last_seen_at,
    h.is_current,
    h.price_vnd,
    h.price_per_m2_vnd,
    h.area_m2,
    h.bedrooms,
    h.floors,
    h.length_m,
    h.width_m,
    h.street_width_m,
    h.price_is_negotiable,
    h.price_is_outlier,
    h.area_is_undetermined,
    h.area_is_outlier,
    h.has_warning,
    h.is_expired
FROM silver.listing_history h
JOIN gold.dim_location loc
    ON loc.province_new = COALESCE(h.address_province_new, '')
   AND loc.ward_new = COALESCE(h.address_ward_new, '')
   AND loc.province_old = COALESCE(h.address_province_old, '')
   AND loc.ward_old = COALESCE(h.address_ward_old, '')
   AND loc.district_old = COALESCE(h.address_district_old, '')
   AND loc.street = COALESCE(h.address_street_new, '')
JOIN gold.dim_property_type pt
    ON pt.property_type_name = h.property_type
   AND pt.listing_type = h.listing_type
JOIN gold.dim_source src
    ON src.source_name = CASE
        WHEN h.source_bronze_key LIKE 'bronze/dataset/%' THEN 'dataset'
        WHEN h.source_bronze_key LIKE 'bronze/web/%' THEN 'web'
        ELSE NULL
    END
   AND src.source_part = h.source_part
ON CONFLICT (listing_key) DO UPDATE SET
    listing_id                 = EXCLUDED.listing_id,
    listing_url                = EXCLUDED.listing_url,
    location_key                = EXCLUDED.location_key,
    property_type_key           = EXCLUDED.property_type_key,
    feature_key                  = EXCLUDED.feature_key,
    source_key                   = EXCLUDED.source_key,
    observed_date_key              = EXCLUDED.observed_date_key,
    posted_date_key                = EXCLUDED.posted_date_key,
    valid_from                       = EXCLUDED.valid_from,
    valid_to                          = EXCLUDED.valid_to,
    last_seen_at                       = EXCLUDED.last_seen_at,
    is_current                          = EXCLUDED.is_current,
    price_vnd                            = EXCLUDED.price_vnd,
    price_per_m2_vnd                      = EXCLUDED.price_per_m2_vnd,
    area_m2                                = EXCLUDED.area_m2,
    bedrooms                                = EXCLUDED.bedrooms,
    floors                                   = EXCLUDED.floors,
    length_m                                  = EXCLUDED.length_m,
    width_m                                    = EXCLUDED.width_m,
    street_width_m                              = EXCLUDED.street_width_m,
    price_is_negotiable                          = EXCLUDED.price_is_negotiable,
    price_is_outlier                              = EXCLUDED.price_is_outlier,
    area_is_undetermined                           = EXCLUDED.area_is_undetermined,
    area_is_outlier                                 = EXCLUDED.area_is_outlier,
    has_warning                                      = EXCLUDED.has_warning,
    is_expired                                        = EXCLUDED.is_expired;

COMMIT;
