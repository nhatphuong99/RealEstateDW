-- analytics/sql/cards/q7_stability.sql
-- Tab Q7 "Độ ổn định giá theo tuần". Sale only; the two sources are separate series and never joined into one line.
-- Field Filters (Dropdown) mapped to analytics.vw_listing_latest:
--   loai_hinh -> property_type_name, vung -> region_group
-- Defaults: loai_hinh = "Nhà trong hẻm", vung = hcm_old, so property-type and region mix do not move the median (see CARD 0b).
-- Do not alias analytics.vw_listing_latest on cards with Field Filters.
-- To test in DataGrip: remove the optional [[AND ...]] filter lines or replace them with real conditions.
--
-- Time window: defined once per card in its params CTE (full ISO weeks inside each source's coverage).
-- nguon = source of the latest version: a dataset listing revisited by the web counts as web with the new posted_date
-- (docs/metrics.md section 10).

-- CARD 0 (QA only, not on the dashboard): listings per week and source over all posted_date
SELECT
    date_trunc('week', posted_date)::date AS tuan,
    source_name AS nguon,
    COUNT(*) AS n_listing,
    COUNT(*) FILTER (WHERE is_analysis_ready AND deal_type = 'sale') AS n_ban_dung_duoc
FROM analytics.vw_listing_latest
GROUP BY 1, 2
ORDER BY 2, 1;

-- CARD 0b (QA only, not on the dashboard): is a median change driven by region mix or by price?
-- Compare median_tat_ca with median_hcm_cu, and ty_trong_hcm_cu_pct, per week
WITH params AS (
    SELECT DATE '2026-06-29' AS ds_tu, DATE '2026-08-02' AS ds_den,
           DATE '2026-08-24' AS web_tu, DATE '2026-09-13' AS web_den
)
SELECT
    date_trunc('week', posted_date)::date AS tuan,
    source_name AS nguon,
    COUNT(*) AS n,
    ROUND(100.0 * COUNT(*) FILTER (WHERE region_group = 'hcm_old') / COUNT(*), 2) AS ty_trong_hcm_cu_pct,
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

-- CARD 1 "Median giá/m² theo tuần": weekly median (Line; X = tuan, series = nguon); weeks with n < 30 hidden
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

-- CARD 2 "Số tin mỗi tuần; tuần có ngày thiếu crawl": listings per week (stacked Bar: n_tuan_du_ngay, n_tuan_thieu_ngay)
-- The mask applies to web only: a week with at least one is_missing_day inside the web window
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
