-- ============================================================================
-- sql/queries/diagnose_gold_join_loss.sql
-- CÔNG CỤ CHẨN ĐOÁN — chạy khi validate_gold_load báo row_count_match lệch
-- (đặc biệt actual=0). Đếm riêng số dòng silver.listing_history không khớp
-- được với từng dimension ở etl_silver_to_gold.sql bước 6, để biết chính
-- xác JOIN nào đang làm rớt dòng thay vì phải đoán.
--
-- CÁCH DÙNG: chạy sau khi etl_silver_to_gold.sql chạy xong (bước 1-5 đã nạp
-- Dim). Đọc "orphan_count" > 0 ở dim nào -> đó là nơi cần sửa.
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

-- 3. Có bao nhiêu dòng Silver không khớp được gold.dim_source? 
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

-- 4. Tổng dòng khớp được cả 3 dimension — số dòng thật sự sẽ INSERT vào
--    Fact. Nếu = 0 dù (1)+(2)+(3) đều = 0, khả năng cao lỗi ở code Python
--    (không execute đúng file SQL, hoặc sai DSN/schema search_path).
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

