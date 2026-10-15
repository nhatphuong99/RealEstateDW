-- analytics/sql/cards/q1_price_level.sql
-- Tab Q1 "Mặt bằng giá/m²": median, p25, p75, n; groups with n < 30 are hidden (docs/metrics.md section 3).
-- Field Filters (Dropdown) mapped to analytics.vw_listing_latest:
--   loai_tin -> listing_type, loai_hinh -> property_type_name, nguon -> source_name, vung -> region_group
-- Do not alias analytics.vw_listing_latest. Always GROUP BY deal_type so sale and rent are never mixed.
-- Display units: sale = million VND/m2, rent = thousand VND/m2/month.
-- To test in DataGrip: remove the optional [[AND ...]] filter lines or replace them with real conditions.

-- CARD 1 "Tổng quan": one row per listing type (Table)
WITH base AS (
    SELECT
        listing_type,
        deal_type,
        COUNT(*) AS n,
        percentile_cont(0.25) WITHIN GROUP (ORDER BY price_per_m2_vnd) AS p25_vnd,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY price_per_m2_vnd) AS median_vnd,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY price_per_m2_vnd) AS p75_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      [[AND {{loai_tin}}]]
      [[AND {{loai_hinh}}]]
      [[AND {{nguon}}]]
      [[AND {{vung}}]]
    GROUP BY listing_type, deal_type
)
SELECT
    listing_type AS loai_tin,
    n,
    CASE WHEN n >= 30 THEN ROUND((median_vnd / CASE WHEN deal_type = 'sale' THEN 1e6 ELSE 1e3 END)::numeric, 2) END AS median,
    CASE WHEN n >= 30 THEN ROUND((p25_vnd / CASE WHEN deal_type = 'sale' THEN 1e6 ELSE 1e3 END)::numeric, 2) END AS p25,
    CASE WHEN n >= 30 THEN ROUND((p75_vnd / CASE WHEN deal_type = 'sale' THEN 1e6 ELSE 1e3 END)::numeric, 2) END AS p75,
    CASE WHEN deal_type = 'sale' THEN 'triệu VND/m²' ELSE 'nghìn VND/m²/tháng' END AS don_vi
FROM base
ORDER BY listing_type;

-- CARD 2 "Bảng quận": district table sorted by median descending (Table); only groups with n >= 30
WITH base AS (
    SELECT
        listing_type,
        deal_type,
        district_old,
        COUNT(*) AS n,
        percentile_cont(0.25) WITHIN GROUP (ORDER BY price_per_m2_vnd) AS p25_vnd,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY price_per_m2_vnd) AS median_vnd,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY price_per_m2_vnd) AS p75_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      [[AND {{loai_tin}}]]
      [[AND {{loai_hinh}}]]
      [[AND {{nguon}}]]
      [[AND {{vung}}]]
    GROUP BY listing_type, deal_type, district_old
    HAVING COUNT(*) >= 30
)
SELECT
    listing_type AS loai_tin,
    district_old AS quan_huyen,
    n,
    ROUND((median_vnd / CASE WHEN deal_type = 'sale' THEN 1e6 ELSE 1e3 END)::numeric, 2) AS median,
    ROUND((p25_vnd / CASE WHEN deal_type = 'sale' THEN 1e6 ELSE 1e3 END)::numeric, 2) AS p25,
    ROUND((p75_vnd / CASE WHEN deal_type = 'sale' THEN 1e6 ELSE 1e3 END)::numeric, 2) AS p75,
    CASE WHEN deal_type = 'sale' THEN 'triệu VND/m²' ELSE 'nghìn VND/m²/tháng' END AS don_vi
FROM base
ORDER BY listing_type, median_vnd DESC;

-- CARD 3 "Bản đồ phường: median giá/m² bán": ward region map (post-merger ward GeoJSON in maps/)
-- Sale only (wards matter for frontage, alley and villa); set loai_hinh Required, default "Nhà trong hẻm"
SELECT
    ward_new_map_key AS phuong,
    COUNT(*) AS n,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd) / 1000000)::numeric, 2) AS median_trieu_m2
FROM analytics.vw_listing_latest
WHERE is_analysis_ready
  AND deal_type = 'sale'
  [[AND {{loai_hinh}}]]
  [[AND {{nguon}}]]
  [[AND {{vung}}]]
GROUP BY ward_new_map_key
HAVING COUNT(*) >= 30
ORDER BY median_trieu_m2 DESC;
