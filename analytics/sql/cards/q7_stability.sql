-- analytics/sql/cards/q7_stability.sql
-- Tab Q7 "Độ ổn định giá theo tuần". Chỉ tin bán, hai nguồn tách riêng, không nối đường.
-- Field Filter (Dropdown) map vào analytics.vw_listing_latest:
--   {{loai_hinh}} -> property_type_name, {{vung}} -> region_group
-- Mặc định nên đặt loai_hinh = "Nhà trong hẻm", vung = hcm_old để cơ cấu loại hình và vùng không làm median trôi
-- (QA 2026-10-04: median web tuần 08-31 giảm vì tỷ trọng HCM cũ giảm còn 67%, xem CARD 0b).
-- KHÔNG đặt alias cho analytics.vw_listing_latest ở card có Field Filter.
-- Chạy thử trên DataGrip: xóa các dòng [[AND {{...}}]] hoặc thay bằng điều kiện thật.
--
-- CỬA SỔ THỜI GIAN: một chỗ duy nhất trong CTE params của mỗi card.
--   Phương án A (đang dùng, tuần ISO đầy đủ): dataset 2026-06-29 -> 2026-08-02 (5 tuần), web 2026-08-24 -> 2026-09-13 (3 tuần).
--   Phương án B (đúng ngày Day 1 ghi): ds_den = 2026-07-26 (4 tuần), web_den = 2026-09-07 (tuần cuối chỉ có 1 ngày).
--   Kiểm tra độ nhạy: ds_den = 2026-08-09 để thêm tuần 08-03 (bất thường).
-- Ghi chú: nguon = nguồn của bản mới nhất; listing dataset được web gặp lại tính vào web với posted_date mới (hạn chế 13).

-- CARD 0 (chỉ QA, không đưa lên dashboard): số tin theo tuần và nguồn trên toàn bộ posted_date
SELECT
    date_trunc('week', posted_date)::date AS tuan,
    source_name AS nguon,
    COUNT(*) AS n_listing,
    COUNT(*) FILTER (WHERE is_analysis_ready AND deal_type = 'sale') AS n_ban_dung_duoc
FROM analytics.vw_listing_latest
GROUP BY 1, 2
ORDER BY 2, 1;

-- CARD 0b (chỉ QA, không đưa lên dashboard): median thay đổi do cơ cấu vùng hay do giá?
-- So median_tat_ca với median_hcm_cu (chỉ tin thuộc TP.HCM cũ) và ty_trong_hcm_cu_pct theo tuần
WITH params AS (
    SELECT DATE '2026-06-29' AS ds_tu, DATE '2026-08-02' AS ds_den,
           DATE '2026-08-24' AS web_tu, DATE '2026-09-13' AS web_den
)
SELECT
    date_trunc('week', posted_date)::date AS tuan,
    source_name AS nguon,
    COUNT(*) AS n,
    ROUND(100.0 * COUNT(*) FILTER (WHERE region_group = 'hcm_old') / COUNT(*), 1) AS ty_trong_hcm_cu_pct,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd) / 1000000)::numeric, 2) AS median_tat_ca,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
           FILTER (WHERE region_group = 'hcm_old') / 1000000)::numeric, 2) AS median_hcm_cu
FROM analytics.vw_listing_latest
CROSS JOIN params
WHERE is_analysis_ready
  AND deal_type = 'sale'
  AND (
        (source_name = 'dataset' AND posted_date BETWEEN ds_tu AND ds_den)
     OR (source_name = 'web' AND posted_date BETWEEN web_tu AND web_den)
  )
  [[AND {{loai_hinh}}]]
GROUP BY 1, 2
HAVING COUNT(*) >= 30
ORDER BY 2, 1;

-- CARD 1: "Median giá/m² theo tuần" (Line, tuan trục X, nguon là chuỗi; ẩn tuần n < 30)
WITH params AS (
    SELECT DATE '2026-06-29' AS ds_tu, DATE '2026-08-02' AS ds_den,
           DATE '2026-08-24' AS web_tu, DATE '2026-09-13' AS web_den
)
SELECT
    date_trunc('week', posted_date)::date AS tuan,
    source_name AS nguon,
    COUNT(*) AS n,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd) / 1000000)::numeric, 2) AS median_trieu_m2
FROM analytics.vw_listing_latest
CROSS JOIN params
WHERE is_analysis_ready
  AND deal_type = 'sale'
  AND (
        (source_name = 'dataset' AND posted_date BETWEEN ds_tu AND ds_den)
     OR (source_name = 'web' AND posted_date BETWEEN web_tu AND web_den)
  )
  [[AND {{loai_hinh}}]]
  [[AND {{vung}}]]
GROUP BY 1, 2
HAVING COUNT(*) >= 30
ORDER BY 1, 2;

-- CARD 2: "Số tin mỗi tuần; tuần có ngày thiếu crawl" (Bar xếp chồng: n_tuan_du_ngay, n_tuan_thieu_ngay)
-- Mặt nạ chỉ áp cho nguồn web: tuần có ít nhất một ngày is_missing_day nằm trong cửa sổ web
WITH params AS (
    SELECT DATE '2026-06-29' AS ds_tu, DATE '2026-08-02' AS ds_den,
           DATE '2026-08-24' AS web_tu, DATE '2026-09-13' AS web_den
),
weekly AS (
    SELECT
        date_trunc('week', posted_date)::date AS tuan,
        source_name AS nguon,
        COUNT(*) AS n
    FROM analytics.vw_listing_latest
    CROSS JOIN params
    WHERE is_analysis_ready
      AND deal_type = 'sale'
      AND (
            (source_name = 'dataset' AND posted_date BETWEEN ds_tu AND ds_den)
         OR (source_name = 'web' AND posted_date BETWEEN web_tu AND web_den)
      )
      [[AND {{loai_hinh}}]]
      [[AND {{vung}}]]
    GROUP BY 1, 2
)
SELECT
    w.tuan,
    w.nguon,
    CASE WHEN m.thieu THEN 0 ELSE w.n END AS n_tuan_du_ngay,
    CASE WHEN m.thieu THEN w.n ELSE 0 END AS n_tuan_thieu_ngay
FROM weekly AS w
CROSS JOIN params AS p
CROSS JOIN LATERAL (
    SELECT (w.nguon = 'web' AND EXISTS (
        SELECT 1
        FROM analytics.vw_crawl_coverage AS c
        WHERE date_trunc('week', c.crawl_date)::date = w.tuan
          AND c.is_missing_day
          AND c.crawl_date BETWEEN p.web_tu AND p.web_den
    )) AS thieu
) AS m
ORDER BY w.tuan, w.nguon;
