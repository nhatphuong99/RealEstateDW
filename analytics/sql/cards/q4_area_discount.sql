-- analytics/sql/cards/q4_area_discount.sql
-- Tab Q4 "Chiết khấu giá/m² theo diện tích" (docs/metrics.md section 2).
-- Sale only (deal_type = 'sale' is hard-coded, no listing-type filter): Q4 serves buyers, and mixing rent would mix price units.
-- Cell = (region_group, deal_type, property_type_name, district_old, area_band).
-- Discount = median(band) / median(base band 2_40_60) - 1, within the SAME property type x district.
-- A cell counts only if both the band and the base band have n >= 30. Headline = median discount of the qualified cells.
-- Field Filters (Dropdown) mapped to analytics.vw_listing_latest:
--   loai_hinh -> property_type_name, quan -> district_old
-- Do not alias analytics.vw_listing_latest. Every card repeats the CTEs.
-- To test in DataGrip: remove the optional [[AND ...]] filter lines or replace them with real conditions.

-- CARD 1 "Chiết khấu giá/m² theo nhóm diện tích": discount by area band vs the 40-60 m2 band (Bar)
WITH cell AS (
    SELECT
        region_group, deal_type, property_type_name, district_old, area_band,
        COUNT(*) AS n,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd) AS median_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND area_band IS NOT NULL
      AND district_old IS NOT NULL
      AND deal_type = 'sale'
      [[AND {{loai_hinh}}]]
      [[AND {{quan}}]]
    GROUP BY region_group, deal_type, property_type_name, district_old, area_band
),
base AS (
    SELECT region_group, deal_type, property_type_name, district_old, median_vnd AS base_vnd
    FROM cell
    WHERE area_band = '2_40_60' AND n >= 30
),
qualified AS (
    SELECT c.area_band, c.median_vnd / b.base_vnd - 1 AS discount
    FROM cell c
    JOIN base b USING (region_group, deal_type, property_type_name, district_old)
    WHERE c.area_band <> '2_40_60' AND c.n >= 30
)
SELECT
    CASE area_band
        WHEN '1_lt_40' THEN 'Dưới 40 m²' WHEN '3_60_100' THEN '60 – 100 m²'
        WHEN '4_ge_100' THEN 'Từ 100 m² trở lên'
    END AS nhom_dien_tich,
    COUNT(*) AS so_o,
    ROUND((100 * percentile_cont(0.5) WITHIN GROUP (ORDER BY discount))::numeric, 2) AS chiet_khau_median_pct
FROM qualified
GROUP BY area_band
ORDER BY area_band;

-- CARD 2 "Chi tiết theo loại hình × quận × nhóm diện tích": detail table sorted by property type, district, band (Table)
WITH cell AS (
    SELECT
        region_group, deal_type, property_type_name, district_old, area_band,
        COUNT(*) AS n,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd) AS median_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND area_band IS NOT NULL
      AND district_old IS NOT NULL
      AND deal_type = 'sale'
      [[AND {{loai_hinh}}]]
      [[AND {{quan}}]]
    GROUP BY region_group, deal_type, property_type_name, district_old, area_band
),
base AS (
    SELECT region_group, deal_type, property_type_name, district_old,
           n AS n_co_so, median_vnd AS base_vnd
    FROM cell
    WHERE area_band = '2_40_60' AND n >= 30
)
SELECT
    c.property_type_name AS loai_hinh,
    c.district_old AS quan_huyen,
    CASE c.area_band
        WHEN '1_lt_40' THEN 'Dưới 40 m²' WHEN '3_60_100' THEN '60 – 100 m²'
        WHEN '4_ge_100' THEN 'Từ 100 m² trở lên'
    END AS nhom_dien_tich,
    ROUND((c.median_vnd / 1e6)::numeric, 2) AS median_trieu_m2,
    ROUND((b.base_vnd / 1e6)::numeric, 2) AS co_so_trieu_m2,
    ROUND((100 * (c.median_vnd / b.base_vnd - 1))::numeric, 2) AS chiet_khau_pct,
    c.n,
    b.n_co_so
FROM cell c
JOIN base b USING (region_group, deal_type, property_type_name, district_old)
WHERE c.area_band <> '2_40_60' AND c.n >= 30
ORDER BY c.property_type_name, c.district_old, c.area_band;

-- CARD 3 "Chiết khấu theo loại hình": discount by property type (clustered Bar: X = area band, series = property type)
-- No loai_hinh variable (only quan): the pooled figure is dominated by street houses, and apartments can move the other way.
WITH cell AS (
    SELECT
        region_group, deal_type, property_type_name, district_old, area_band,
        COUNT(*) AS n,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd) AS median_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND area_band IS NOT NULL
      AND district_old IS NOT NULL
      AND deal_type = 'sale'
      [[AND {{quan}}]]
    GROUP BY region_group, deal_type, property_type_name, district_old, area_band
),
base AS (
    SELECT region_group, deal_type, property_type_name, district_old, median_vnd AS base_vnd
    FROM cell
    WHERE area_band = '2_40_60' AND n >= 30
),
qualified AS (
    SELECT c.property_type_name, c.area_band, c.median_vnd / b.base_vnd - 1 AS discount
    FROM cell c
    JOIN base b USING (region_group, deal_type, property_type_name, district_old)
    WHERE c.area_band <> '2_40_60' AND c.n >= 30
)
SELECT
    property_type_name AS loai_hinh,
    CASE area_band
        WHEN '1_lt_40' THEN 'Dưới 40 m²' WHEN '3_60_100' THEN '60 – 100 m²'
        WHEN '4_ge_100' THEN 'Từ 100 m² trở lên'
    END AS nhom_dien_tich,
    COUNT(*) AS so_o,
    ROUND((100 * percentile_cont(0.5) WITHIN GROUP (ORDER BY discount))::numeric, 2) AS chiet_khau_median_pct
FROM qualified
GROUP BY property_type_name, area_band
-- Order by area band first: Metabase orders the X axis by first appearance
ORDER BY area_band, property_type_name;
