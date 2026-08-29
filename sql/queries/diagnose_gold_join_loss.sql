-- ============================================================================
-- sql/queries/diagnose_gold_join_loss.sql
-- CÔNG CỤ CHẨN ĐOÁN — chạy khi validate_gold_load báo row_count_match lệch
-- (đặc biệt actual=0) mà chưa rõ nguyên nhân. Đếm riêng số dòng
-- silver.listing_history KHÔNG khớp được với TỪNG dimension trong
-- etl_silver_to_gold.sql bước 6, để biết chính xác JOIN nào đang làm rớt
-- dòng thay vì phải đoán.
--
-- CÁCH DÙNG: chạy SAU khi etl_silver_to_gold.sql đã chạy xong (bước 1-5 đã
-- nạp Dim), TRƯỚC hoặc SAU bước 6 đều được (không phụ thuộc Fact đã có gì).
-- Đọc từng dòng kết quả: "orphan_count" > 0 ở dim nào -> đó là nơi cần sửa.
-- ============================================================================

-- 1. Có bao nhiêu dòng Silver không khớp được gold.dim_location?
--    (dùng cùng điều kiện JOIN như etl_silver_to_gold.sql bước 6)
SELECT
    'dim_location' AS join_target,
    COUNT(*) AS orphan_count,
    COUNT(*) FILTER (
        WHERE h.address_province_new IS NULL OR h.address_ward_new IS NULL
           OR h.address_province_old IS NULL OR h.address_ward_old IS NULL
           OR h.address_district_old IS NULL OR h.address_street_new IS NULL
    ) AS orphan_with_real_null   -- >0 nghĩa là Silver có NULL thật (không phải '') ở cột địa chỉ
FROM silver.listing_history h
LEFT JOIN gold.dim_location loc
    ON loc.province_new = COALESCE(h.address_province_new, '')
   AND loc.ward_new = COALESCE(h.address_ward_new, '')
   AND loc.province_old = COALESCE(h.address_province_old, '')
   AND loc.ward_old = COALESCE(h.address_ward_old, '')
   AND loc.district_old = COALESCE(h.address_district_old, '')
   AND loc.street = COALESCE(h.address_street_new, '')
WHERE loc.location_key IS NULL

UNION ALL

-- 2. Có bao nhiêu dòng Silver không khớp được gold.dim_property_type?
SELECT
    'dim_property_type' AS join_target,
    COUNT(*) AS orphan_count,
    COUNT(*) FILTER (WHERE h.property_type IS NULL OR h.listing_type IS NULL) AS orphan_with_real_null
FROM silver.listing_history h
LEFT JOIN gold.dim_property_type pt
    ON pt.property_type_name = h.property_type
   AND pt.listing_type = h.listing_type
WHERE pt.property_type_key IS NULL

UNION ALL

-- 3. Có bao nhiêu dòng Silver không khớp được gold.dim_source? (thường do
--    source_bronze_key không khớp prefix 'bronze/dataset/' hay 'bronze/web/'
--    nào — xem cột sample_unmatched_prefix để biết prefix thật đang là gì)
SELECT
    'dim_source' AS join_target,
    COUNT(*) AS orphan_count,
    0 AS orphan_with_real_null
FROM silver.listing_history h
LEFT JOIN gold.dim_source src
    ON src.source_name = CASE
        WHEN h.source_bronze_key LIKE 'bronze/dataset/%' THEN 'dataset'
        WHEN h.source_bronze_key LIKE 'bronze/web/%' THEN 'web'
        ELSE NULL
    END
   AND src.source_part = h.source_part
WHERE src.source_key IS NULL

UNION ALL

-- 4. Tổng dòng khớp được CẢ BA dimension cùng lúc — đây mới là số dòng
--    thật sự sẽ được INSERT vào Fact. Nếu = 0 dù (1)+(2)+(3) đều = 0 (tức
--    từng dim riêng lẻ đều khớp), khả năng cao vấn đề nằm ở phần code
--    Python (silver_to_gold_io.py không thực sự execute file SQL đang xem,
--    hoặc kết nối sai DSN/schema search_path) — không phải lỗi logic SQL.
SELECT
    'joined_all_3_dims' AS join_target,
    COUNT(*) AS orphan_count,
    0 AS orphan_with_real_null
FROM silver.listing_history h
JOIN gold.dim_location loc
    ON loc.province_new = COALESCE(h.address_province_new, '')
   AND loc.ward_new = COALESCE(h.address_ward_new, '')
   AND loc.province_old = COALESCE(h.address_province_old, '')
   AND loc.ward_old = COALESCE(h.address_ward_old, '')
   AND loc.district_old = COALESCE(h.address_district_old, '')
   AND loc.street = COALESCE(h.address_street_new, '')
JOIN gold.dim_property_type pt
    ON pt.property_type_name = h.property_type AND pt.listing_type = h.listing_type
JOIN gold.dim_source src
    ON src.source_name = CASE
        WHEN h.source_bronze_key LIKE 'bronze/dataset/%' THEN 'dataset'
        WHEN h.source_bronze_key LIKE 'bronze/web/%' THEN 'web'
        ELSE NULL
    END
   AND src.source_part = h.source_part;

-- ----------------------------------------------------------------------
-- Nếu (1)/(2)/(3) đều = 0 nhưng gold.fact_listing_price VẪN 0 dòng sau khi
-- chạy etl_silver_to_gold.sql: kiểm tra thêm 2 khả năng phổ biến nhất
-- (không phải lỗi logic JOIN, mà lỗi vận hành/schema-drift):
--
-- a) Chạy 2 câu dưới đây để xác nhận gold.dim_location/gold.fact_listing_price
--    THỰC SỰ có đúng các cột mới (province_new, price_is_outlier,
--    observed_date_key) -- nếu lệch, nghĩa là DB đang chạy 1 phiên bản
--    005_gold_schema.sql CŨ (province/price_valid_from_date_key), phải
--    DROP SCHEMA gold CASCADE rồi chạy lại 005_gold_schema.sql MỚI trước
--    khi chạy lại etl_silver_to_gold.sql:
--
--       SELECT column_name FROM information_schema.columns
--       WHERE table_schema='gold' AND table_name='dim_location' ORDER BY column_name;
--
--       SELECT column_name FROM information_schema.columns
--       WHERE table_schema='gold' AND table_name='fact_listing_price' ORDER BY column_name;
--
-- b) Xác nhận parser/silver_to_gold_io.py đang đọc ĐÚNG file đang xem (kiểm
--    tra _ETL_SQL_PATH trỏ đúng sql/queries/etl_silver_to_gold.sql, không
--    phải bản cache/copy cũ ở đường dẫn khác trong image Airflow).
-- ----------------------------------------------------------------------
