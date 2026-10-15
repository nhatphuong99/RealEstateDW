-- analytics/sql/cards/q5_price_change.sql
-- Tab Q5 "Listing được web gặp lại có đổi giá không". Reads analytics.vw_listing_versions (consecutive duplicate hashes already removed).
-- Pair = previous (dataset) and next (web) version of the same listing_id; only pairs with BOTH prices > 0.
-- Price change = price_vnd <> prev_price_vnd. pct = (new - old) / old, computed here (not the view's price_change_pct).
-- The sample only has pairs with different hashes, so there is NO "share of revisited listings that changed price",
-- and it does not represent the market (docs/metrics.md sections 7 and 10).
-- Field Filter (Dropdown) mapped to analytics.vw_listing_latest: loai_tin -> listing_type
-- Do not alias analytics.vw_listing_latest (alias v on vw_listing_versions is fine). Every card repeats the pairs CTE.
-- To test in DataGrip: remove the optional [[AND ...]] line or replace it with a real condition.

-- CARD 1 "Số cặp có đổi giá": number of pairs with a price change (Number)
WITH pairs AS (
    SELECT
        v.days_since_prev_version AS gap_ngay,
        v.price_vnd,
        v.prev_price_vnd,
        (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd AS pct
    FROM analytics.vw_listing_versions v
    JOIN analytics.vw_listing_latest
      ON analytics.vw_listing_latest.listing_id = v.listing_id
    WHERE v.prev_source_name = 'dataset'
      AND v.source_name = 'web'
      AND v.price_vnd > 0
      AND v.prev_price_vnd > 0
      [[AND {{loai_tin}}]]
)
SELECT COUNT(*) AS so_cap_doi_gia
FROM pairs
WHERE price_vnd <> prev_price_vnd;

-- CARD 2 "Tỷ lệ cặp giảm giá": share of decreases among pairs with a change (Number, %); hidden when n < 30
WITH pairs AS (
    SELECT
        v.days_since_prev_version AS gap_ngay,
        v.price_vnd,
        v.prev_price_vnd,
        (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd AS pct
    FROM analytics.vw_listing_versions v
    JOIN analytics.vw_listing_latest
      ON analytics.vw_listing_latest.listing_id = v.listing_id
    WHERE v.prev_source_name = 'dataset'
      AND v.source_name = 'web'
      AND v.price_vnd > 0
      AND v.prev_price_vnd > 0
      [[AND {{loai_tin}}]]
)
SELECT
    CASE WHEN COUNT(*) >= 30
         THEN ROUND((100.0 * COUNT(*) FILTER (WHERE pct < 0) / COUNT(*))::numeric, 2)
    END AS ty_le_giam_pct
FROM pairs
WHERE price_vnd <> prev_price_vnd;

-- CARD 3 "Phân vị % đổi giá": one-row table n, p25, median, p75; hidden when n < 30; no mean (Table)
WITH pairs AS (
    SELECT
        v.days_since_prev_version AS gap_ngay,
        v.price_vnd,
        v.prev_price_vnd,
        (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd AS pct
    FROM analytics.vw_listing_versions v
    JOIN analytics.vw_listing_latest
      ON analytics.vw_listing_latest.listing_id = v.listing_id
    WHERE v.prev_source_name = 'dataset'
      AND v.source_name = 'web'
      AND v.price_vnd > 0
      AND v.prev_price_vnd > 0
      [[AND {{loai_tin}}]]
)
SELECT
    COUNT(*) AS n,
    CASE WHEN COUNT(*) >= 30 THEN ROUND((100 * percentile_cont(0.25) WITHIN GROUP (ORDER BY pct))::numeric, 2) END AS p25_pct,
    CASE WHEN COUNT(*) >= 30 THEN ROUND((100 * percentile_cont(0.50) WITHIN GROUP (ORDER BY pct))::numeric, 2) END AS median_pct,
    CASE WHEN COUNT(*) >= 30 THEN ROUND((100 * percentile_cont(0.75) WITHIN GROUP (ORDER BY pct))::numeric, 2) END AS p75_pct
FROM pairs
WHERE price_vnd <> prev_price_vnd;

-- CARD 4 "Phân bố mức đổi giá": distribution of price changes (Bar; X = nhom_doi_gia); open-ended tails expose outliers
-- Symmetric tails (<= -50%, >= +50%) match block C of sql/qa/q5_qa.sql. Always 9 buckets (empty = 0);
-- the card is hidden when changed pairs < 30. ORDER BY nhom_no: Metabase orders the X axis by first appearance.
WITH pairs AS (
    SELECT
        v.days_since_prev_version AS gap_ngay,
        v.price_vnd,
        v.prev_price_vnd,
        (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd AS pct
    FROM analytics.vw_listing_versions v
    JOIN analytics.vw_listing_latest
      ON analytics.vw_listing_latest.listing_id = v.listing_id
    WHERE v.prev_source_name = 'dataset'
      AND v.source_name = 'web'
      AND v.price_vnd > 0
      AND v.prev_price_vnd > 0
      [[AND {{loai_tin}}]]
),
changed AS (
    SELECT
        pct,
        CASE
            WHEN pct <= -0.50 THEN 1
            WHEN pct <= -0.20 THEN 2
            WHEN pct <= -0.10 THEN 3
            WHEN pct <= -0.05 THEN 4
            WHEN pct <= -0.02 THEN 5
            WHEN pct <   0    THEN 6
            WHEN pct <=  0.10 THEN 7
            WHEN pct <   0.50 THEN 8
            ELSE 9
        END AS nhom_no
    FROM pairs
    WHERE price_vnd <> prev_price_vnd
),
bucket (nhom_no, nhom_doi_gia) AS (
    VALUES (1, 'Giảm ≥50%'), (2, 'Giảm 20–50%'), (3, 'Giảm 10–20%'),
           (4, 'Giảm 5–10%'), (5, 'Giảm 2–5%'), (6, 'Giảm <2%'),
           (7, 'Tăng <10%'), (8, 'Tăng 10–50%'), (9, 'Tăng ≥50%')
)
SELECT
    b.nhom_doi_gia,
    COUNT(c.pct) AS so_cap
FROM bucket b
LEFT JOIN changed c USING (nhom_no)
WHERE (SELECT COUNT(*) FROM changed) >= 30
GROUP BY b.nhom_no, b.nhom_doi_gia
ORDER BY b.nhom_no;

-- CARD 5 "Khoảng cách giữa hai lần quan sát": gap between the two observations (Table); median hidden when the group's changed pairs < 30
-- so_cap = all pairs with both prices > 0 (different hash for any reason); no change rate (the sample is filtered by hash).
WITH pairs AS (
    SELECT
        v.days_since_prev_version AS gap_ngay,
        v.price_vnd,
        v.prev_price_vnd,
        (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd AS pct
    FROM analytics.vw_listing_versions v
    JOIN analytics.vw_listing_latest
      ON analytics.vw_listing_latest.listing_id = v.listing_id
    WHERE v.prev_source_name = 'dataset'
      AND v.source_name = 'web'
      AND v.price_vnd > 0
      AND v.prev_price_vnd > 0
      [[AND {{loai_tin}}]]
),
grp AS (
    SELECT
        pct, price_vnd, prev_price_vnd,
        CASE WHEN gap_ngay < 30 THEN 1 WHEN gap_ngay < 60 THEN 2 WHEN gap_ngay < 90 THEN 3 ELSE 4 END AS nhom_no
    FROM pairs
)
SELECT
    CASE nhom_no WHEN 1 THEN 'Dưới 30 ngày' WHEN 2 THEN '30 – 59 ngày' WHEN 3 THEN '60 – 89 ngày' ELSE 'Từ 90 ngày' END AS khoang_cach,
    COUNT(*) AS so_cap,
    COUNT(*) FILTER (WHERE price_vnd <> prev_price_vnd) AS so_cap_doi_gia,
    CASE WHEN COUNT(*) FILTER (WHERE price_vnd <> prev_price_vnd) >= 30
         THEN ROUND((100 * percentile_cont(0.5) WITHIN GROUP (ORDER BY pct) FILTER (WHERE price_vnd <> prev_price_vnd))::numeric, 2)
    END AS median_pct_doi_gia
FROM grp
GROUP BY nhom_no
ORDER BY nhom_no;
