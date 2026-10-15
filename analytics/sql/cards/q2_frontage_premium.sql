-- analytics/sql/cards/q2_frontage_premium.sql
-- Tab Q2 "Mặt tiền đắt hơn hẻm bao nhiêu % cùng phường". Sale only (docs/metrics.md section 3).
-- Cell = (ward_new, area_band); premium = median(Nhà mặt tiền) / median(Nhà trong hẻm) - 1.
-- A cell counts only if BOTH groups have n >= 30. Headline = median of the qualified cells' premiums (unweighted).
-- Field Filters (Dropdown) mapped to analytics.vw_listing_latest:
--   vung -> region_group, nhom_dien_tich -> area_band
-- Do not alias analytics.vw_listing_latest. Every card repeats the cell CTE (Metabase cards cannot share CTEs).
-- To test in DataGrip: remove the optional [[AND ...]] filter lines or replace them with real conditions.

-- CARD 1 "Premium median": headline premium (Number, %)
WITH cell AS (
    SELECT
        ward_new,
        area_band,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS n_mt,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS n_hem,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS median_mt_vnd,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS median_hem_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND deal_type = 'sale'
      AND property_type_name IN ('Nhà mặt tiền', 'Nhà trong hẻm')
      AND ward_new IS NOT NULL
      AND area_band IS NOT NULL
      [[AND {{vung}}]]
      [[AND {{nhom_dien_tich}}]]
    GROUP BY ward_new, area_band
),
qualified AS (
    SELECT *, median_mt_vnd / median_hem_vnd - 1 AS premium
    FROM cell
    WHERE n_mt >= 30 AND n_hem >= 30
)
SELECT ROUND((100 * percentile_cont(0.5) WITHIN GROUP (ORDER BY premium))::numeric, 2) AS premium_median_pct
FROM qualified;

-- CARD 2 "Số ô đủ mẫu": number of qualified cells (Number)
WITH cell AS (
    SELECT
        ward_new,
        area_band,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS n_mt,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS n_hem
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND deal_type = 'sale'
      AND property_type_name IN ('Nhà mặt tiền', 'Nhà trong hẻm')
      AND ward_new IS NOT NULL
      AND area_band IS NOT NULL
      [[AND {{vung}}]]
      [[AND {{nhom_dien_tich}}]]
    GROUP BY ward_new, area_band
)
SELECT COUNT(*) AS so_o_dat
FROM cell
WHERE n_mt >= 30 AND n_hem >= 30;

-- CARD 2B "Số phường đủ mẫu": number of qualified wards (Number)
WITH cell AS (
    SELECT
        ward_new,
        area_band,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS n_mt,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS n_hem
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND deal_type = 'sale'
      AND property_type_name IN ('Nhà mặt tiền', 'Nhà trong hẻm')
      AND ward_new IS NOT NULL
      AND area_band IS NOT NULL
      [[AND {{vung}}]]
      [[AND {{nhom_dien_tich}}]]
    GROUP BY ward_new, area_band
)
SELECT COUNT(DISTINCT ward_new) AS so_phuong_dat
FROM cell
WHERE n_mt >= 30 AND n_hem >= 30;

-- CARD 3 "5 ô có premium cao nhất": top 5 cells by premium (Table: ward, area band, n, premium %)
WITH cell AS (
    SELECT
        ward_new,
        area_band,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS n_mt,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS n_hem,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS median_mt_vnd,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS median_hem_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND deal_type = 'sale'
      AND property_type_name IN ('Nhà mặt tiền', 'Nhà trong hẻm')
      AND ward_new IS NOT NULL
      AND area_band IS NOT NULL
      [[AND {{vung}}]]
      [[AND {{nhom_dien_tich}}]]
    GROUP BY ward_new, area_band
)
SELECT
    ward_new AS phuong,
    CASE area_band
        WHEN '1_lt_40' THEN 'Dưới 40 m²' WHEN '2_40_60' THEN '40 – 60 m²'
        WHEN '3_60_100' THEN '60 – 100 m²' WHEN '4_ge_100' THEN 'Từ 100 m² trở lên'
    END AS nhom_dien_tich,
    n_mt,
    n_hem,
    ROUND((100 * (median_mt_vnd / median_hem_vnd - 1))::numeric, 2) AS premium_pct
FROM cell
WHERE n_mt >= 30 AND n_hem >= 30
ORDER BY median_mt_vnd / median_hem_vnd DESC
LIMIT 5;

-- CARD 4 "Premium theo nhóm diện tích": premium by area band (Bar; X = nhom_dien_tich)
-- Grouped by area band, not district: ward_new (merged ward) is not nested in district_old,
-- so a ward has no single district.
WITH cell AS (
    SELECT
        ward_new,
        area_band,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS n_mt,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS n_hem,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS median_mt_vnd,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS median_hem_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND deal_type = 'sale'
      AND property_type_name IN ('Nhà mặt tiền', 'Nhà trong hẻm')
      AND ward_new IS NOT NULL
      AND area_band IS NOT NULL
      [[AND {{vung}}]]
      [[AND {{nhom_dien_tich}}]]
    GROUP BY ward_new, area_band
),
qualified AS (
    SELECT *, median_mt_vnd / median_hem_vnd - 1 AS premium
    FROM cell
    WHERE n_mt >= 30 AND n_hem >= 30
)
SELECT
    CASE area_band
        WHEN '1_lt_40' THEN 'Dưới 40 m²' WHEN '2_40_60' THEN '40 – 60 m²'
        WHEN '3_60_100' THEN '60 – 100 m²' WHEN '4_ge_100' THEN 'Từ 100 m² trở lên'
    END AS nhom_dien_tich,
    COUNT(*) AS so_phuong,
    ROUND((100 * percentile_cont(0.5) WITHIN GROUP (ORDER BY premium))::numeric, 2) AS premium_median_pct
FROM qualified
GROUP BY area_band
ORDER BY area_band;

-- CARD 5 "Bảng phường × nhóm diện tích": ward x area band table sorted by premium descending (Table)
WITH cell AS (
    SELECT
        ward_new,
        area_band,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS n_mt,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS n_hem,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS median_mt_vnd,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS median_hem_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND deal_type = 'sale'
      AND property_type_name IN ('Nhà mặt tiền', 'Nhà trong hẻm')
      AND ward_new IS NOT NULL
      AND area_band IS NOT NULL
      [[AND {{vung}}]]
      [[AND {{nhom_dien_tich}}]]
    GROUP BY ward_new, area_band
)
SELECT
    ward_new AS phuong,
    CASE area_band
        WHEN '1_lt_40' THEN 'Dưới 40 m²' WHEN '2_40_60' THEN '40 – 60 m²'
        WHEN '3_60_100' THEN '60 – 100 m²' WHEN '4_ge_100' THEN 'Từ 100 m² trở lên'
    END AS nhom_dien_tich,
    ROUND((median_mt_vnd / 1e6)::numeric, 2) AS median_mat_tien_trieu_m2,
    ROUND((median_hem_vnd / 1e6)::numeric, 2) AS median_hem_trieu_m2,
    ROUND((100 * (median_mt_vnd / median_hem_vnd - 1))::numeric, 2) AS premium_pct,
    n_mt,
    n_hem
FROM cell
WHERE n_mt >= 30 AND n_hem >= 30
ORDER BY median_mt_vnd / median_hem_vnd DESC;
