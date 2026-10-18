-- analytics/qa/q5_sizing.sql
-- Q5 sample sizing before building the cards. Read-only on analytics.vw_listing_versions.
-- Pair = two adjacent versions of the same listing_id ordered by valid_from (valid_to / is_current not used).
-- Run one block at a time in DataGrip (select the block, Ctrl+Enter).

-- BLOCK 1: pairs by source direction, and counts of price changes
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

-- BLOCK 2: % price change distribution of dataset -> web pairs with a change (look for extreme values before setting chart limits)
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

-- BLOCK 3: day gap between the two observations (Vietnam time zone dates, same as Q6 CARD 5)
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
