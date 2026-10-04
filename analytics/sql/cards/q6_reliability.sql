-- analytics/sql/cards/q6_reliability.sql
-- Tab Q6 "Độ tin cậy dữ liệu" (docs/dashboard_v2_wireframe.md). Mỗi khối CARD là một Question Metabase
-- (New -> SQL query, database real_estate_dw). Không dùng Field Filter ở tab này.
-- Chạy thử trên DataGrip được nguyên văn.

-- CARD 1: "Ngày không có lần chạy" (Number)
SELECT COUNT(*) FILTER (WHERE is_missing_day) AS ngay_thieu
FROM analytics.vw_crawl_coverage;

-- CARD 2: "Ngày chạy thiếu một phần (dưới 20 lần)" (Number)
SELECT COUNT(*) FILTER (WHERE is_partial_day) AS ngay_partial
FROM analytics.vw_crawl_coverage;

-- CARD 3: "Version trùng hash đã loại" (Number) = dòng Gold trừ dòng vw_listing_versions
SELECT
    (SELECT COUNT(*) FROM gold.vw_fact_report)
  - (SELECT COUNT(*) FROM analytics.vw_listing_versions) AS version_trung_hash;

-- CARD 4: "Số lần chạy mỗi ngày" (Bar, xếp chồng: day_du xanh, thieu_mot_phan vàng, khong_chay đỏ)
-- khong_chay = 24 là chiều cao đánh dấu ngày không chạy (DAG 2 chạy hằng giờ nên tối đa 24 lần/ngày)
SELECT
    crawl_date,
    CASE WHEN coverage_level = 'full' THEN run_count ELSE 0 END AS day_du,
    CASE WHEN coverage_level = 'partial' THEN run_count ELSE 0 END AS thieu_mot_phan,
    CASE WHEN is_missing_day THEN 24 ELSE 0 END AS khong_chay
FROM analytics.vw_crawl_coverage
ORDER BY crawl_date;

-- CARD 5: "Trang chi tiết đã tải so với version web mới vào Gold" (Line hoặc Bar, hai chuỗi)
-- Ngày version = ngày VN của valid_from (ép múi giờ để không lệch giữa DataGrip và Metabase)
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

-- CARD 6: "Lý do dừng của các lần chạy DAG 2" (Pie hoặc Bar)
SELECT COALESCE(stopped_reason, '(chưa kết thúc)') AS ly_do_dung, COUNT(*) AS so_lan_chay
FROM pipeline.run_state
GROUP BY 1
ORDER BY 2 DESC;

-- CARD 7: "Tỷ lệ hết hạn theo nguồn của bản mới nhất" (Table)
-- Lưu ý ghi trên card: web không crawl lại URL cũ nên is_expired của web luôn FALSE, không so giữa hai nguồn
SELECT
    source_name AS nguon,
    COUNT(*) AS so_listing,
    COUNT(*) FILTER (WHERE is_expired) AS het_han,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_expired) / COUNT(*), 1) AS ty_le_het_han_pct
FROM analytics.vw_listing_latest
GROUP BY 1
ORDER BY 1;
