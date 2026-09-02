-- ============================================================================
-- sql/queries/merge_scd2_listing_history.sql
-- Merge SCD Type 2: silver.listing_staging_batch -> silver.listing_history.
-- Chạy sau mỗi lần Spark ETL (Bronze->Silver) ghi xong 1 batch vào staging_batch.
--
-- Thiết kế: 3 bước UPDATE/INSERT tuần tự, KHÔNG gộp vào 1 câu MERGE (Postgres
-- MERGE không xử lý tốt kiểu "đóng dòng cũ + mở dòng mới" cùng lúc).
--
-- QUAN TRỌNG: kết quả LAG/LEAD được vật chất hóa 1 LẦN DUY NHẤT vào bảng tạm
-- (scd2_ordered/scd2_change_points/scd2_same_hash) TRƯỚC khi chạy 3 bước.
-- Nếu để mỗi bước tự tính lại CTE riêng, Bước 1 sẽ đổi is_current trên
-- listing_history TRƯỚC khi Bước 2/3 chạy -> CTE tính lại ở bước sau sẽ đọc
-- nhầm dữ liệu đã bị Bước 1 sửa. Bảng tạm tránh được lỗi này.
--
-- Toàn bộ script chạy trong 1 transaction để đảm bảo tính nguyên tử.
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------
-- Bảng tạm 1: combined (staging + anchor) + prev_hash qua LAG()
-- ----------------------------------------------------------------------
DROP TABLE IF EXISTS scd2_ordered;

CREATE TEMP TABLE scd2_ordered AS
WITH combined AS (
    -- Toàn bộ quan sát MỚI trong batch (nguồn: Spark ETL Bronze->Silver)
    SELECT
        listing_id, listing_url, source_part, source_bronze_key, crawl_date,
        row_hash, 'staging'::TEXT AS origin,
        title, listing_type, property_type, posted_date,
        price_vnd, price_raw, price_is_negotiable, price_is_outlier,
        area_m2, area_raw, area_is_undetermined, area_is_outlier,
        length_m, width_m, street_width_m, floors, bedrooms,
        orientation, legal_status,
        has_dining_room, has_kitchen, has_rooftop, has_car_parking, owner_direct,
        is_expired, has_warning,
        address_street_new, address_ward_new, address_province_new,
        address_old_raw, address_ward_old, address_district_old, address_province_old
    FROM silver.listing_staging_batch

    UNION ALL

    -- Toàn bộ version đã có trong Silver (KHÔNG chỉ is_current), dùng làm
    -- "mốc" (anchor) để LAG() biết prev_hash cho từng dòng staging. Lấy cả
    -- lịch sử để rerun idempotent — nếu chỉ lấy is_current, staging trùng
    -- 1 version cũ sẽ bị LAG() hiểu nhầm "lần đầu xuất hiện", tạo bản sao lỗi.
    SELECT
        listing_id, listing_url, source_part, source_bronze_key,
        valid_from AS crawl_date,
        row_hash, 'anchor'::TEXT AS origin,
        title, listing_type, property_type, posted_date,
        price_vnd, price_raw, price_is_negotiable, price_is_outlier,
        area_m2, area_raw, area_is_undetermined, area_is_outlier,
        length_m, width_m, street_width_m, floors, bedrooms,
        orientation, legal_status,
        has_dining_room, has_kitchen, has_rooftop, has_car_parking, owner_direct,
        is_expired, has_warning,
        address_street_new, address_ward_new, address_province_new,
        address_old_raw, address_ward_old, address_district_old, address_province_old
    FROM silver.listing_history
)
SELECT
    combined.*,
    LAG(row_hash) OVER (
        PARTITION BY listing_id
        ORDER BY crawl_date, origin  -- tie-break: 'anchor' < 'staging' (alphabet) -> anchor luôn đứng trước khi trùng crawl_date
    ) AS prev_hash
FROM combined;

-- ----------------------------------------------------------------------
-- Bảng tạm 2: change_points — chỉ dòng STAGING thực sự đổi hash so với
-- dòng liền trước (kể cả so với anchor). next_change_crawl_date/is_latest
-- tính TRÊN TẬP CON change_points (không tính trên scd2_ordered đầy đủ),
-- để valid_to nhảy thẳng tới điểm đổi kế tiếp, bỏ qua các quan sát hash
-- không đổi ở giữa.
-- ----------------------------------------------------------------------
DROP TABLE IF EXISTS scd2_change_points;

CREATE TEMP TABLE scd2_change_points AS
SELECT
    o.*,
    LEAD(o.crawl_date) OVER (PARTITION BY o.listing_id ORDER BY o.crawl_date) AS next_change_crawl_date,
    ROW_NUMBER() OVER (PARTITION BY o.listing_id ORDER BY o.crawl_date DESC) = 1 AS is_latest
FROM scd2_ordered o
WHERE o.origin = 'staging'
  AND o.prev_hash IS DISTINCT FROM o.row_hash;

-- ----------------------------------------------------------------------
-- Bảng tạm 3: same_hash_rows — dòng staging KHÔNG đổi hash, chỉ cần cập
-- nhật last_seen_at (không sinh version mới).
-- ----------------------------------------------------------------------
DROP TABLE IF EXISTS scd2_same_hash;

CREATE TEMP TABLE scd2_same_hash AS
SELECT listing_id, MAX(crawl_date) AS max_crawl_date
FROM scd2_ordered
WHERE origin = 'staging'
  AND prev_hash IS NOT DISTINCT FROM row_hash
GROUP BY listing_id;

-- ----------------------------------------------------------------------
-- Bước 1: đóng version is_current bị thay thế (chỉ listing_id có đổi hash
-- trong batch này). valid_to = điểm đổi ĐẦU TIÊN của listing_id đó.
-- ----------------------------------------------------------------------
UPDATE silver.listing_history h
SET valid_to = cp.first_change_crawl_date,
    is_current = FALSE
FROM (
    SELECT listing_id, MIN(crawl_date) AS first_change_crawl_date
    FROM scd2_change_points
    GROUP BY listing_id
) cp
WHERE h.listing_id = cp.listing_id
  AND h.is_current;

-- ----------------------------------------------------------------------
-- Bước 2: insert toàn bộ change point thành version mới.
-- is_current chỉ TRUE cho change point mới nhất (is_latest) của mỗi
-- listing_id; valid_to = next_change_crawl_date (NULL cho bản mới nhất).
-- KHÔNG insert row_hash (GENERATED STORED, Postgres tự tính).
-- ----------------------------------------------------------------------
INSERT INTO silver.listing_history (
    listing_id, listing_url, source_part, source_bronze_key,
    valid_from, valid_to, is_current, last_seen_at,
    title, listing_type, property_type, posted_date,
    price_vnd, price_raw, price_is_negotiable, price_is_outlier,
    area_m2, area_raw, area_is_undetermined, area_is_outlier,
    length_m, width_m, street_width_m, floors, bedrooms,
    orientation, legal_status,
    has_dining_room, has_kitchen, has_rooftop, has_car_parking, owner_direct,
    is_expired, has_warning,
    address_street_new, address_ward_new, address_province_new,
    address_old_raw, address_ward_old, address_district_old, address_province_old
)
SELECT
    listing_id, listing_url, source_part, source_bronze_key,
    crawl_date AS valid_from,
    next_change_crawl_date AS valid_to,
    is_latest AS is_current,
    crawl_date AS last_seen_at,
    title, listing_type, property_type, posted_date,
    price_vnd, price_raw, price_is_negotiable, price_is_outlier,
    area_m2, area_raw, area_is_undetermined, area_is_outlier,
    length_m, width_m, street_width_m, floors, bedrooms,
    orientation, legal_status,
    has_dining_room, has_kitchen, has_rooftop, has_car_parking, owner_direct,
    is_expired, has_warning,
    address_street_new, address_ward_new, address_province_new,
    address_old_raw, address_ward_old, address_district_old, address_province_old
FROM scd2_change_points;

-- ----------------------------------------------------------------------
-- Bước 3: cập nhật last_seen_at cho các listing_id KHÔNG đổi hash trong
-- batch (đúng nguyên tắc "duplicate trong Silver = bug" -> không insert
-- version mới, chỉ xác nhận lần crawl gần nhất). Đây cũng là bằng chứng
-- duy nhất khiến is_reconfirmed=TRUE ở Gold (last_seen_at > valid_from).
-- ----------------------------------------------------------------------
UPDATE silver.listing_history h
SET last_seen_at = s.max_crawl_date
FROM scd2_same_hash s
WHERE h.listing_id = s.listing_id
  AND h.is_current
  AND s.max_crawl_date > h.last_seen_at;

COMMIT;

-- Bảng tạm (scd2_ordered/scd2_change_points/scd2_same_hash) tự động biến
-- mất khi session psql/kết nối kết thúc — không cần DROP thủ công ở cuối.
