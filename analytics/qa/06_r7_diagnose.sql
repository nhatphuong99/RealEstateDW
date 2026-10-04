-- analytics/qa/06_r7_diagnose.sql
-- Chẩn đoán R7 (area_suspect) sau spot-check E3. Chỉ đọc.
-- (1) và (2) tính từ điều kiện thô nên vẫn dùng được sau khi đổi ngưỡng dài x ngang.

-- (1) Tin có tỷ lệ thô nằm trong [8,12) hoặc [80,120), theo diện tích mặt bằng suy ra từ dài x ngang
WITH cand AS (
    SELECT l.*,
           l.length_m * l.width_m AS dim_area,
           CASE
               WHEN l.area_m2 / (l.length_m * l.width_m) >= 8 AND l.area_m2 / (l.length_m * l.width_m) < 12 THEN '8-12'
               WHEN l.area_m2 / (l.length_m * l.width_m) >= 80 AND l.area_m2 / (l.length_m * l.width_m) < 120 THEN '80-120'
           END AS ratio_band
    FROM analytics.vw_listing_latest AS l
    WHERE l.is_usable AND l.length_m > 0 AND l.width_m > 0
)
SELECT
    ratio_band,
    CASE
        WHEN dim_area < 5 THEN '1_lt_5'
        WHEN dim_area < 10 THEN '2_5_10'
        WHEN dim_area < 20 THEN '3_10_20'
        ELSE '4_ge_20'
    END AS dim_area_bucket,
    COUNT(*) AS n,
    COUNT(*) FILTER (WHERE deal_type = 'sale') AS n_sale,
    COUNT(*) FILTER (WHERE area_suspect) AS n_flagged
FROM cand
WHERE ratio_band IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;

-- (2) Các tin đang bị gắn cờ, theo số tầng (floors): nhà nhiều tầng thường có diện tích là tổng diện tích sàn
SELECT
    CASE
        WHEN floors IS NULL THEN '0_null'
        WHEN floors <= 4 THEN '1_floors_1_4'
        ELSE '2_floors_5_plus'
    END AS floors_group,
    COUNT(*) AS n_flagged,
    COUNT(*) FILTER (WHERE deal_type = 'sale') AS n_sale
FROM analytics.vw_listing_latest
WHERE area_suspect AND is_usable
GROUP BY 1
ORDER BY 1;

-- (3) Mẫu 15 tin đang bị gắn cờ (MD5 cố định): xem tiêu đề để biết lỗi dấu thập phân hay diện tích sàn
SELECT l.listing_key, l.deal_type, l.property_type_name, l.area_m2, l.length_m, l.width_m,
       ROUND(l.area_ratio, 2) AS area_ratio, l.floors,
       s.area_raw, s.title, s.listing_url
FROM analytics.vw_listing_latest AS l
JOIN silver.listing_history AS s ON s.listing_key = l.listing_key
WHERE l.area_suspect AND l.is_usable
ORDER BY MD5(l.listing_key::TEXT)
LIMIT 15;
