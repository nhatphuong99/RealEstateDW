-- analytics/sql/cards/q6_reliability.sql
-- Tab Q6 "Độ tin cậy dữ liệu" (docs/dashboard_v2_wireframe.md). Each CARD block is one Metabase SQL question
-- (database real_estate_dw). No Field Filters on this tab; every block runs as-is in DataGrip.

-- CARD 1 "Ngày không có lần chạy": days with no run (Number)
SELECT COUNT(*) FILTER (WHERE is_missing_day) AS ngay_thieu
FROM analytics.vw_crawl_coverage;

-- CARD 2 "Ngày chạy thiếu một phần": days with fewer than 20 runs (Number)
SELECT COUNT(*) FILTER (WHERE is_partial_day) AS ngay_partial
FROM analytics.vw_crawl_coverage;

-- CARD 3 "Version trùng hash đã loại": duplicate-hash versions removed = Gold rows - vw_listing_versions rows (Number)
SELECT
    (SELECT COUNT(*) FROM gold.vw_fact_report)
  - (SELECT COUNT(*) FROM analytics.vw_listing_versions) AS version_trung_hash;

-- CARD 4 "Số lần chạy mỗi ngày": runs per day (stacked Bar: day_du green, thieu_mot_phan yellow, khong_chay red)
-- khong_chay = 24 is a marker height for days with no run (DAG 2 runs hourly, so at most 24 runs per day)
SELECT
    crawl_date,
    CASE WHEN coverage_level = 'full' THEN run_count ELSE 0 END AS day_du,
    CASE WHEN coverage_level = 'partial' THEN run_count ELSE 0 END AS thieu_mot_phan,
    CASE WHEN is_missing_day THEN 24 ELSE 0 END AS khong_chay
FROM analytics.vw_crawl_coverage
ORDER BY crawl_date;

-- CARD 5 "Trang chi tiết đã tải so với version web mới vào Gold": pages fetched vs new web versions in Gold (Line or Bar, two series)
-- Version day = Vietnam date of valid_from (same result in DataGrip and Metabase)
SELECT
    c.crawl_date,
    c.detail_pages_done AS trang_chi_tiet_da_tai,
    COALESCE(v.n, 0) AS version_web_moi
FROM analytics.vw_crawl_coverage AS c
LEFT JOIN (
    SELECT (valid_from AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS crawl_date, COUNT(*) AS n
    FROM analytics.vw_listing_versions
    WHERE source_name = 'web'
    GROUP BY 1
) AS v ON v.crawl_date = c.crawl_date
ORDER BY c.crawl_date;

-- CARD 6 "Lý do dừng của các lần chạy DAG 2": stop reason of DAG 2 runs (Pie or Bar)
SELECT COALESCE(stopped_reason, '(chưa kết thúc)') AS ly_do_dung, COUNT(*) AS so_lan_chay
FROM pipeline.run_state
GROUP BY 1
ORDER BY 2 DESC;

-- CARD 7 "Tỷ lệ hết hạn theo nguồn của bản mới nhất": expiry rate by source of the latest version (Table)
-- Note on the card: the web never re-crawls old URLs, so web is_expired is always FALSE; do not compare the two sources
SELECT
    source_name AS nguon,
    COUNT(*) AS so_listing,
    COUNT(*) FILTER (WHERE is_expired) AS het_han,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_expired) / COUNT(*), 2) AS ty_le_het_han_pct
FROM analytics.vw_listing_latest
GROUP BY 1
ORDER BY 1;
