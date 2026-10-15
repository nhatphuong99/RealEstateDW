-- analytics/sql/qa/q5_sizing.sql
-- Đo cỡ mẫu Q5 trước khi dựng card. Chỉ đọc analytics.vw_listing_versions.
-- Cặp = hai version liền kề của cùng listing_id, sắp theo valid_from (không dùng valid_to, is_current).
-- Chạy từng khối một trong DataGrip (bôi đen khối rồi Ctrl+Enter).

-- KHỐI 1: số cặp theo hướng nguồn và số cặp đổi giá
WITH ordered AS (
    SELECT
        listing_id,
        valid_from,
        source_name,
        price_vnd,
        LAG(valid_from)  OVER w AS prev_valid_from,
        LAG(source_name) OVER w AS prev_source,
        LAG(price_vnd)   OVER w AS prev_price_vnd
    FROM analytics.vw_listing_versions
    WINDOW w AS (PARTITION BY listing_id ORDER BY valid_from)
)
SELECT
    prev_source || ' -> ' || source_name AS cap_nguon,
    COUNT(*) AS so_cap,
    COUNT(*) FILTER (WHERE valid_from = prev_valid_from) AS so_cap_cung_thoi_diem,
    COUNT(*) FILTER (WHERE price_vnd IS NOT NULL AND prev_price_vnd IS NOT NULL) AS so_cap_co_gia,
    COUNT(*) FILTER (WHERE price_vnd IS NOT NULL AND prev_price_vnd IS NOT NULL
                       AND price_vnd <> prev_price_vnd) AS so_cap_doi_gia,
    COUNT(*) FILTER (WHERE price_vnd IS NOT NULL AND prev_price_vnd IS NOT NULL
                       AND price_vnd < prev_price_vnd) AS so_cap_giam_gia
FROM ordered
WHERE prev_valid_from IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- KHỐI 2: phân phối % đổi giá của cặp dataset -> web có đổi giá (tìm giá trị bất thường trước khi đặt giới hạn)
WITH ordered AS (
    SELECT
        listing_id,
        source_name,
        price_vnd,
        LAG(source_name) OVER w AS prev_source,
        LAG(price_vnd)   OVER w AS prev_price_vnd
    FROM analytics.vw_listing_versions
    WINDOW w AS (PARTITION BY listing_id ORDER BY valid_from)
),
changed AS (
    SELECT (price_vnd - prev_price_vnd) / prev_price_vnd AS pct
    FROM ordered
    WHERE prev_source = 'dataset' AND source_name = 'web'
      AND price_vnd IS NOT NULL AND prev_price_vnd IS NOT NULL
      AND prev_price_vnd > 0 AND price_vnd <> prev_price_vnd
)
SELECT
    COUNT(*) AS n,
    ROUND((100 * MIN(pct))::numeric, 1) AS min_pct,
    ROUND((100 * percentile_cont(0.01) WITHIN GROUP (ORDER BY pct))::numeric, 1) AS p01,
    ROUND((100 * percentile_cont(0.25) WITHIN GROUP (ORDER BY pct))::numeric, 1) AS p25,
    ROUND((100 * percentile_cont(0.50) WITHIN GROUP (ORDER BY pct))::numeric, 1) AS p50,
    ROUND((100 * percentile_cont(0.75) WITHIN GROUP (ORDER BY pct))::numeric, 1) AS p75,
    ROUND((100 * percentile_cont(0.99) WITHIN GROUP (ORDER BY pct))::numeric, 1) AS p99,
    ROUND((100 * MAX(pct))::numeric, 1) AS max_pct
FROM changed;

-- KHỐI 3: khoảng cách ngày giữa hai lần quan sát (ngày theo múi giờ VN, cùng cách với Q6 CARD 5)
WITH ordered AS (
    SELECT
        source_name,
        (valid_from AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS ngay,
        LAG((valid_from AT TIME ZONE 'Asia/Ho_Chi_Minh')::date) OVER w AS prev_ngay,
        LAG(source_name) OVER w AS prev_source
    FROM analytics.vw_listing_versions
    WINDOW w AS (PARTITION BY listing_id ORDER BY valid_from)
)
SELECT
    MIN(ngay - prev_ngay) AS gap_min,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY ngay - prev_ngay) AS gap_median,
    MAX(ngay - prev_ngay) AS gap_max,
    COUNT(*) AS so_cap
FROM ordered
WHERE prev_source = 'dataset' AND source_name = 'web';
