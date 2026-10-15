-- analytics/sql/cards/q3_gross_yield.sql
-- Tab Q3 "Gross yield theo quận". Gross yield = 12 x median monthly rent per m2 / median sale price per m2 (docs/metrics.md section 3).
-- Cell = (region_group, district_old, property_type_name); a cell counts only if BOTH sale and rent have n >= 30.
-- Only 3 property types: Căn hộ chung cư, Nhà mặt tiền, Nhà trong hẻm. A district-level estimate, not the same asset.
-- Both sides are VND/m2, so the ratio needs no unit conversion. Display: sale = million VND/m2, rent x 12 = million VND/m2/year.
-- Field Filters (Dropdown) mapped to analytics.vw_listing_latest:
--   loai_hinh -> property_type_name, nguon -> source_name, vung -> region_group
-- Do not alias analytics.vw_listing_latest. Every card repeats the cell CTE.
-- To test in DataGrip: remove the optional [[AND ...]] filter lines or replace them with real conditions.

-- CARD 1 "Tổng quan theo loại hình": median yield of qualified districts per property type (Table, 3 rows); no loai_hinh variable, do not connect the property-type dashboard filter
WITH cell AS (
    SELECT
        region_group,
        district_old,
        property_type_name,
        COUNT(*) FILTER (WHERE deal_type = 'sale') AS n_ban,
        COUNT(*) FILTER (WHERE deal_type = 'rent') AS n_thue,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE deal_type = 'sale') AS median_ban_vnd,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE deal_type = 'rent') AS median_thue_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND property_type_name IN ('Căn hộ chung cư', 'Nhà mặt tiền', 'Nhà trong hẻm')
      AND district_old IS NOT NULL
      [[AND {{nguon}}]]
      [[AND {{vung}}]]
    GROUP BY region_group, district_old, property_type_name
),
qualified AS (
    SELECT *, 12 * median_thue_vnd / median_ban_vnd AS gross_yield
    FROM cell
    WHERE n_ban >= 30 AND n_thue >= 30
)
SELECT
    property_type_name AS loai_hinh,
    COUNT(*) AS so_quan_dat,
    ROUND((100 * percentile_cont(0.5) WITHIN GROUP (ORDER BY gross_yield))::numeric, 2) AS yield_median_pct,
    ROUND((100 * MIN(gross_yield))::numeric, 2) AS yield_min_pct,
    ROUND((100 * MAX(gross_yield))::numeric, 2) AS yield_max_pct
FROM qualified
GROUP BY property_type_name
ORDER BY property_type_name;

-- CARD 2 "Gross yield theo quận": yield by district (vertical Bar, sorted descending); set loai_hinh Required, default "Căn hộ chung cư"
WITH cell AS (
    SELECT
        region_group,
        district_old,
        property_type_name,
        COUNT(*) FILTER (WHERE deal_type = 'sale') AS n_ban,
        COUNT(*) FILTER (WHERE deal_type = 'rent') AS n_thue,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE deal_type = 'sale') AS median_ban_vnd,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE deal_type = 'rent') AS median_thue_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND property_type_name IN ('Căn hộ chung cư', 'Nhà mặt tiền', 'Nhà trong hẻm')
      AND district_old IS NOT NULL
      [[AND {{loai_hinh}}]]
      [[AND {{nguon}}]]
      [[AND {{vung}}]]
    GROUP BY region_group, district_old, property_type_name
)
SELECT
    district_old AS quan_huyen,
    ROUND((100 * 12 * median_thue_vnd / median_ban_vnd)::numeric, 2) AS gross_yield_pct
FROM cell
WHERE n_ban >= 30 AND n_thue >= 30
ORDER BY 12 * median_thue_vnd / median_ban_vnd DESC;

-- CARD 3 "Bảng quận": district table (sale median, rent x 12, yield, n sale, n rent) (Table)
WITH cell AS (
    SELECT
        region_group,
        district_old,
        property_type_name,
        COUNT(*) FILTER (WHERE deal_type = 'sale') AS n_ban,
        COUNT(*) FILTER (WHERE deal_type = 'rent') AS n_thue,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE deal_type = 'sale') AS median_ban_vnd,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE deal_type = 'rent') AS median_thue_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND property_type_name IN ('Căn hộ chung cư', 'Nhà mặt tiền', 'Nhà trong hẻm')
      AND district_old IS NOT NULL
      [[AND {{loai_hinh}}]]
      [[AND {{nguon}}]]
      [[AND {{vung}}]]
    GROUP BY region_group, district_old, property_type_name
)
SELECT
    property_type_name AS loai_hinh,
    district_old AS quan_huyen,
    ROUND((median_ban_vnd / 1e6)::numeric, 2) AS median_ban_trieu_m2,
    ROUND((12 * median_thue_vnd / 1e6)::numeric, 2) AS median_thue_x12_trieu_m2_nam,
    ROUND((100 * 12 * median_thue_vnd / median_ban_vnd)::numeric, 2) AS gross_yield_pct,
    n_ban,
    n_thue
FROM cell
WHERE n_ban >= 30 AND n_thue >= 30
ORDER BY property_type_name, 12 * median_thue_vnd / median_ban_vnd DESC;
