-- analytics/qa/05_median_by_group.sql
-- Mục F của checklist: median/p25/p75 giá/m² bán theo loại hình x quận, nhóm n >= 30.
-- Xuất kết quả thành analytics/qa/_out/median_by_group.csv để so với pandas (recompute_median.py).
SELECT
    property_type_name,
    district_old,
    COUNT(*) AS n,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY price_per_m2_vnd) AS p25,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY price_per_m2_vnd) AS median,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY price_per_m2_vnd) AS p75
FROM analytics.vw_listing_latest
WHERE is_analysis_ready
  AND deal_type = 'sale'
GROUP BY property_type_name, district_old
HAVING COUNT(*) >= 30
ORDER BY property_type_name, district_old;
