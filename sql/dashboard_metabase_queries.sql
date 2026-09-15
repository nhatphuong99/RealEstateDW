-- ============================================================================
-- sql/dashboard_metabase_queries.sql
-- Consolidated SQL for the Metabase dashboard (HCMC real estate — 3 tabs, 13 cards).
-- REFERENCE ONLY — hand-copied into each Metabase Question's SQL editor —
-- NOT run by psql, NOT part of any ETL DAG.
--
-- Field Filters (keep these names consistent everywhere):
--   {{ngay_dang}}       -> posted_date        -> Date range
--   {{loai_tin}}        -> listing_type       -> Dropdown
--   {{loai_hinh_bds}}   -> property_type_name -> Dropdown
--   {{khu_vuc_phuong}}  -> ward_new_map_key   -> Dropdown
--   {{khu_vuc_quan}}    -> district_old_map_key -> Dropdown
--
-- Standard filter set (every price-related card):
--   price_is_negotiable = FALSE, price_is_outlier = FALSE,
--   area_is_outlier = FALSE, area_is_undetermined = FALSE
-- is_current = TRUE is required on EVERY card — avoids double-counting SCD2 versions.
-- ROUND(..., 2) on every currency/area value.
-- ============================================================================


-- ============================================================================
-- TAB 1 — OVERVIEW
-- ============================================================================

-- Card 1 — Total tracked listings (Number)
SELECT COUNT(*) AS tong_so_tin
FROM gold.vw_fact_report
WHERE is_current = TRUE
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
;

-- Card 2 — City-wide average price/m² (Number, suffix " triệu/m²")
SELECT ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
;

-- Card 3 — City-wide median price/m² (Number, suffix " triệu/m²")
SELECT ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)::NUMERIC / 1000000.0, 2) AS gia_trung_vi_trieu_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
;

-- Card 4 — Average price/m² map by Ward (Map -> Region Map -> HCMC_Ward_New_Map)
-- Region field = ward_new_map_key | Metric field = gia_tb_trieu_m2
SELECT ward_new_map_key,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2,
       COUNT(*) AS so_tin
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
  AND ward_new_map_key <> ''
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
GROUP BY ward_new_map_key
HAVING COUNT(*) >= 5
ORDER BY gia_tb_trieu_m2 DESC
;

-- Card 5 — Average price/m² map by District (Map -> Region Map -> HCMC_District_Old_Map)
-- Region field = district_old_map_key | Metric field = gia_tb_trieu_m2
SELECT district_old_map_key,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2,
       COUNT(*) AS so_tin
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
  AND district_old_map_key <> ''
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
GROUP BY district_old_map_key
HAVING COUNT(*) >= 5
ORDER BY gia_tb_trieu_m2 DESC
;


-- ============================================================================
-- TAB 2 — BY REGION
-- ============================================================================

-- Card 6 — Top 10 highest-priced wards (Table)
SELECT ward_new_map_key AS phuong_xa,
       COUNT(*) AS so_tin,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)::NUMERIC / 1000000.0, 2) AS gia_trung_vi_trieu_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
  AND ward_new_map_key <> ''
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_phuong}} ]]
GROUP BY ward_new_map_key
HAVING COUNT(*) >= 5
ORDER BY gia_tb_trieu_m2 DESC
LIMIT 10
;

-- Card 7 — Top 10 lowest-priced wards (Table)
SELECT ward_new_map_key AS phuong_xa,
       COUNT(*) AS so_tin,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)::NUMERIC / 1000000.0, 2) AS gia_trung_vi_trieu_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
  AND ward_new_map_key <> ''
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_phuong}} ]]
GROUP BY ward_new_map_key
HAVING COUNT(*) >= 5
ORDER BY gia_tb_trieu_m2 ASC
LIMIT 10
;

-- Card 8 — Summary table by old District (Table)
SELECT district_old_map_key AS quan_huyen,
       COUNT(*) AS so_tin,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)::NUMERIC / 1000000.0, 2) AS gia_trung_vi_trieu_m2,
       ROUND(AVG(area_m2), 2) AS dien_tich_tb_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
  AND district_old_map_key <> ''
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_quan}} ]]
GROUP BY district_old_map_key
ORDER BY gia_tb_trieu_m2 DESC
;


-- ============================================================================
-- TAB 3 — TRENDS & DISTRIBUTION
-- ============================================================================

-- Card 9 — Monthly average price/m² trend, by property type (Line chart)
SELECT DATE_TRUNC('month', posted_date)::date AS thang,
       property_type_name,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_quan}} ]]
GROUP BY 1, 2
HAVING COUNT(*) >= 5
ORDER BY 1
;

-- Card 10 — Monthly listing volume (Bar chart)
SELECT DATE_TRUNC('month', posted_date)::date AS thang,
       COUNT(*) AS so_tin
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_quan}} ]]
GROUP BY 1
ORDER BY 1
;

-- Card 11 — Listing distribution by property type (Row chart)
-- No price_is_.../area_is_... filters: this only counts listings, no price/area math.
SELECT property_type_name,
       COUNT(*) AS so_tin
FROM gold.vw_fact_report
WHERE is_current = TRUE
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_quan}} ]]
GROUP BY property_type_name
ORDER BY so_tin DESC
;

-- Card 12 — For-sale vs. for-rent ratio (Donut chart)
SELECT listing_type,
       COUNT(*) AS so_tin
FROM gold.vw_fact_report
WHERE is_current = TRUE
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_quan}} ]]
GROUP BY listing_type
;

-- Card 13 — Average price/m² by property type (Bar chart)
SELECT property_type_name,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_quan}} ]]
GROUP BY property_type_name
ORDER BY gia_tb_trieu_m2 DESC
;
