-- analytics/qa/03_reconcile.sql
-- Mục D của checklist: đối chiếu số view với docs/00_discovery.md. Chỉ đọc.
-- Chạy từng khối, ghi kết quả; chưa điền số vào metrics.md trước khi đối chiếu xong.
-- QA được phép đọc silver.listing_history (chỉ ở file này); view analytics không phụ thuộc Silver.

-- D1. Số dòng và số listing
SELECT
    (SELECT COUNT(*) FROM gold.vw_fact_report) AS gold_rows,
    (SELECT COUNT(DISTINCT listing_id) FROM gold.vw_fact_report) AS gold_listings,
    (SELECT COUNT(*) FROM analytics.vw_listing_latest) AS latest_rows,
    (SELECT COUNT(*) FROM analytics.vw_listing_versions) AS version_rows;

-- D1b. Kiểm tra CASE trong view: không được có deal_type NULL; region_group phải đủ 3 nhóm
-- (nếu mọi dòng rơi vào hcm_old thì ILIKE không khớp tên tỉnh, kiểm tra Unicode NFC/NFD)
SELECT deal_type, region_group, COUNT(*) AS n_listings
FROM analytics.vw_listing_latest
GROUP BY 1, 2
ORDER BY 1, 2;

-- D2a. Dòng Gold bị loại vì trùng hash, theo nguồn
SELECT f.source_name, COUNT(*) AS rows_removed, COUNT(DISTINCT f.listing_id) AS listings_affected
FROM gold.vw_fact_report AS f
LEFT JOIN analytics.vw_listing_versions AS v ON v.listing_key = f.listing_key
WHERE v.listing_key IS NULL
GROUP BY 1
ORDER BY 2 DESC;

-- D2b. Số dòng bị loại trên mỗi listing (kỳ vọng gần hết là 1)
SELECT rows_removed_per_listing, COUNT(*) AS n_listings
FROM (
    SELECT f.listing_id, COUNT(*) AS rows_removed_per_listing
    FROM gold.vw_fact_report AS f
    LEFT JOIN analytics.vw_listing_versions AS v ON v.listing_key = f.listing_key
    WHERE v.listing_key IS NULL
    GROUP BY f.listing_id
) AS t
GROUP BY 1
ORDER BY 1;

-- D3. Bản mới nhất không phải is_current, và dòng có valid_to < valid_from
SELECT
    (SELECT COUNT(*)
     FROM analytics.vw_listing_latest AS l
     JOIN gold.vw_fact_report AS g ON g.listing_key = l.listing_key
     WHERE NOT g.is_current) AS latest_not_current,
    (SELECT COUNT(*) FROM gold.vw_fact_report WHERE valid_to < valid_from) AS rows_valid_to_lt_from;

-- D4a. Số dòng theo quy tắc, tính lại từ điều kiện thô (độc lập với cột exclude_reason), cơ sở: bản mới nhất + usable
SELECT
    COUNT(*) FILTER (WHERE deal_type = 'sale' AND price_per_m2_vnd < 1000000) AS r1_sale_lt_1m,
    COUNT(*) FILTER (WHERE deal_type = 'sale'
                       AND price_per_m2_vnd >= 1000000 AND price_per_m2_vnd < 10000000) AS r2_sale_1m_10m,
    COUNT(*) FILTER (WHERE deal_type = 'sale' AND price_per_m2_vnd >= 1000000000) AS r3_ultra_prime,
    COUNT(*) FILTER (WHERE deal_type = 'rent' AND price_per_m2_vnd >= 10000000) AS r4_rent_ge_10m,
    COUNT(*) FILTER (WHERE deal_type = 'rent' AND price_vnd >= 1000000000) AS r5_commercial,
    COUNT(*) FILTER (WHERE deal_type = 'rent' AND price_per_m2_vnd < 10000) AS r6_rent_lt_10k,
    COUNT(*) FILTER (WHERE length_m > 0 AND width_m > 0 AND length_m * width_m >= 20
                       AND ((area_m2 / (length_m * width_m) >= 8 AND area_m2 / (length_m * width_m) < 12)
                         OR (area_m2 / (length_m * width_m) >= 80 AND area_m2 / (length_m * width_m) < 120))) AS r7_area_suspect,
    COUNT(*) FILTER (WHERE deal_type = 'rent' AND price_per_m2_vnd >= 10000000
                       AND price_vnd >= 1000000000) AS overlap_r4_r5
FROM analytics.vw_listing_latest
WHERE is_usable;

-- D4b. Cột cờ của view phải khớp D4a (r3, r5, r7) và phân bố exclude_reason
SELECT
    COUNT(*) FILTER (WHERE is_ultra_prime) AS flag_ultra_prime,
    COUNT(*) FILTER (WHERE is_commercial_building) AS flag_commercial,
    COUNT(*) FILTER (WHERE area_suspect) AS flag_area_suspect,
    COUNT(*) FILTER (WHERE is_commercial_building AND exclude_reason IS NOT NULL) AS commercial_also_excluded,
    COUNT(*) FILTER (WHERE is_usable) AS usable,
    COUNT(*) FILTER (WHERE is_analysis_ready) AS analysis_ready
FROM analytics.vw_listing_latest
WHERE is_usable;

SELECT COALESCE(exclude_reason, '(giữ)') AS exclude_reason, COUNT(*) AS n
FROM analytics.vw_listing_latest
WHERE is_usable
GROUP BY 1
ORDER BY 2 DESC;

-- D5a. vw_crawl_coverage: tổng quan
SELECT
    MIN(crawl_date) AS first_day,
    MAX(crawl_date) AS last_day,
    COUNT(*) AS total_days,
    COUNT(*) FILTER (WHERE coverage_level = 'full') AS full_days,
    COUNT(*) FILTER (WHERE coverage_level = 'partial') AS partial_days,
    COUNT(*) FILTER (WHERE is_missing_day) AS none_days,
    SUM(run_count) AS total_runs,
    SUM(detail_pages_done) AS total_detail_pages
FROM analytics.vw_crawl_coverage;

-- D5b. Ngày có run 'recovered' hoặc 'incomplete' (có thể làm run_count cao hơn thực tế)
SELECT
    (started_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS crawl_date,
    COUNT(*) AS runs,
    COUNT(*) FILTER (WHERE stopped_reason IN ('recovered', 'incomplete')) AS recovered_or_incomplete,
    MAX(ended_at - started_at) AS max_duration
FROM pipeline.run_state
GROUP BY 1
HAVING COUNT(*) FILTER (WHERE stopped_reason IN ('recovered', 'incomplete')) > 0
ORDER BY 1;

-- D6. row_hash tính lại so với Silver (kỳ vọng mismatches = 0 và rows_compared = version_rows)
SELECT COUNT(*) AS rows_compared,
       COUNT(*) FILTER (WHERE v.row_hash <> s.row_hash) AS mismatches
FROM analytics.vw_listing_versions AS v
JOIN silver.listing_history AS s ON s.listing_key = v.listing_key;

-- D7. Các tin thuê giá/m² < 10.000 (R6): mở từng URL để chốt giữ/bỏ quy tắc
SELECT l.listing_key, l.property_type_name, l.price_vnd, l.area_m2, l.price_per_m2_vnd, s.listing_url
FROM analytics.vw_listing_latest AS l
JOIN silver.listing_history AS s ON s.listing_key = l.listing_key
WHERE l.deal_type = 'rent' AND l.is_usable AND l.price_per_m2_vnd < 10000
ORDER BY l.price_per_m2_vnd;

-- D8a. R5 (thuê, giá >= 1 tỷ/tháng) trên các cơ sở đếm khác nhau, so với 118 của E5
SELECT
    (SELECT COUNT(*) FROM gold.vw_fact_report
      WHERE listing_type = 'Cho thuê' AND price_vnd >= 1000000000) AS all_rows,
    (SELECT COUNT(*) FROM gold.vw_fact_report
      WHERE listing_type = 'Cho thuê' AND price_vnd >= 1000000000
        AND price_per_m2_vnd IS NOT NULL AND NOT price_is_negotiable AND NOT price_is_outlier) AS all_rows_usable,
    (SELECT COUNT(*) FROM analytics.vw_listing_latest
      WHERE deal_type = 'rent' AND price_vnd >= 1000000000) AS latest_all,
    (SELECT COUNT(*) FROM analytics.vw_listing_latest
      WHERE deal_type = 'rent' AND price_vnd >= 1000000000 AND is_usable) AS latest_usable;

-- D8b. R6: kích thước của 4 tin (có dài/ngang không? area_suspect có bắt được không?)
SELECT listing_key, area_m2, length_m, width_m,
       ROUND(area_m2 / NULLIF(length_m * width_m, 0), 2) AS area_ratio,
       area_suspect, exclude_reason
FROM analytics.vw_listing_latest
WHERE deal_type = 'rent' AND is_usable AND price_per_m2_vnd < 10000
ORDER BY listing_key;
