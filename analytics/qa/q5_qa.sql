-- analytics/qa/q5_qa.sql
-- Q5 QA and diagnostics (read-only). Run one block at a time in DataGrip (select the block, Ctrl+Enter).

-- BLOCK A: reconcile with sizing. Expected: so_cap_doi_gia_sizing and so_giam_sizing match q5_sizing.sql block 1;
-- so_cap_doi_gia_card (CARD 1, zero prices excluded) = so_cap_doi_gia_sizing - so_doi_gia_co_gia_0.
SELECT
    COUNT(*) AS so_cap,
    COUNT(*) FILTER (WHERE price_vnd IS NOT NULL AND prev_price_vnd IS NOT NULL AND price_vnd <> prev_price_vnd) AS so_cap_doi_gia_sizing,
    COUNT(*) FILTER (WHERE price_vnd IS NOT NULL AND prev_price_vnd IS NOT NULL AND price_vnd < prev_price_vnd) AS so_giam_sizing,
    COUNT(*) FILTER (WHERE price_vnd = 0 OR prev_price_vnd = 0) AS so_cap_co_gia_0,
    COUNT(*) FILTER (WHERE (price_vnd = 0 OR prev_price_vnd = 0) AND price_vnd <> prev_price_vnd) AS so_doi_gia_co_gia_0,
    COUNT(*) FILTER (WHERE price_vnd > 0 AND prev_price_vnd > 0 AND price_vnd <> prev_price_vnd) AS so_cap_doi_gia_card
FROM analytics.vw_listing_versions
WHERE prev_source_name = 'dataset' AND source_name = 'web';

-- BLOCK B: versions per listing. Expected: no listing has 3 or more versions; listings with 2 versions = rows of vw_listing_versions - listings.
SELECT so_version, COUNT(*) AS so_listing
FROM (SELECT listing_id, COUNT(*) AS so_version FROM analytics.vw_listing_versions GROUP BY listing_id) t
GROUP BY so_version
ORDER BY so_version;

-- BLOCK C: tail classification of changed pairs (zero prices excluded), to judge how heavy the tails are before adding exclusion rules.
WITH p AS (
    SELECT (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd AS pct
    FROM analytics.vw_listing_versions v
    WHERE v.prev_source_name = 'dataset' AND v.source_name = 'web'
      AND v.price_vnd > 0 AND v.prev_price_vnd > 0 AND v.price_vnd <> v.prev_price_vnd
)
SELECT
    COUNT(*) AS so_cap_doi_gia,
    COUNT(*) FILTER (WHERE pct <= -0.5) AS giam_tu_50,
    COUNT(*) FILTER (WHERE pct >= 0.5) AS tang_tu_50,
    COUNT(*) FILTER (WHERE pct >= 9) AS tang_tu_10_lan,
    COUNT(*) FILTER (WHERE pct <= -0.9) AS giam_tu_90
FROM p;

-- BLOCK D: the 15 pairs with the largest |% change|, for manual URL checks. Do not alias vw_listing_latest.
SELECT
    v.listing_id,
    v.prev_price_vnd,
    v.price_vnd,
    ROUND((100 * (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd)::numeric, 1) AS pct,
    v.days_since_prev_version AS gap_ngay,
    analytics.vw_listing_latest.listing_type,
    analytics.vw_listing_latest.property_type_name,
    analytics.vw_listing_latest.price_per_m2_vnd
FROM analytics.vw_listing_versions v
JOIN analytics.vw_listing_latest ON analytics.vw_listing_latest.listing_id = v.listing_id
WHERE v.prev_source_name = 'dataset' AND v.source_name = 'web'
  AND v.price_vnd > 0 AND v.prev_price_vnd > 0 AND v.price_vnd <> v.prev_price_vnd
ORDER BY ABS((v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd) DESC
LIMIT 15;

-- BLOCK E: sample size by listing type (sets the default of the Loại Tin filter on tab Q5)
SELECT
    analytics.vw_listing_latest.listing_type,
    COUNT(*) AS so_cap,
    COUNT(*) FILTER (WHERE v.price_vnd <> v.prev_price_vnd) AS so_cap_doi_gia,
    COUNT(*) FILTER (WHERE v.price_vnd < v.prev_price_vnd) AS so_giam
FROM analytics.vw_listing_versions v
JOIN analytics.vw_listing_latest ON analytics.vw_listing_latest.listing_id = v.listing_id
WHERE v.prev_source_name = 'dataset' AND v.source_name = 'web'
  AND v.price_vnd > 0 AND v.prev_price_vnd > 0
GROUP BY analytics.vw_listing_latest.listing_type
ORDER BY 1;

-- BLOCK F: unit of the view's price_change_pct (ratio or percent); exactly one error should be 0.
-- Expected: sai_so_ty_le = 0 (ratio); if neither error is 0, investigate.
SELECT
    MAX(ABS(price_change_pct - (price_vnd - prev_price_vnd) / prev_price_vnd)) AS sai_so_ty_le,
    MAX(ABS(price_change_pct - 100 * (price_vnd - prev_price_vnd) / prev_price_vnd)) AS sai_so_phan_tram
FROM analytics.vw_listing_versions
WHERE prev_source_name = 'dataset' AND source_name = 'web' AND prev_price_vnd > 0 AND price_change_pct IS NOT NULL;

-- BLOCK G: median day gap from the view (days_since_prev_version). Expected: matches q5_sizing.sql block 3 within 1 day.
SELECT
    MIN(days_since_prev_version) AS gap_min,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY days_since_prev_version) AS gap_median,
    MAX(days_since_prev_version) AS gap_max
FROM analytics.vw_listing_versions
WHERE prev_source_name = 'dataset' AND source_name = 'web';

-- BLOCK H: the 15 largest decreases (lower tail).
-- dien_tich_suy_ra_m2 = new price / latest price per m2: an inferred area for judging whether the price is plausible, not source data.
SELECT
    v.listing_id,
    v.prev_price_vnd,
    v.price_vnd,
    ROUND((100 * (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd)::numeric, 2) AS pct,
    v.days_since_prev_version AS gap_ngay,
    analytics.vw_listing_latest.listing_type,
    analytics.vw_listing_latest.property_type_name,
    analytics.vw_listing_latest.price_per_m2_vnd,
    ROUND((v.price_vnd / NULLIF(analytics.vw_listing_latest.price_per_m2_vnd, 0))::numeric, 1) AS dien_tich_suy_ra_m2
FROM analytics.vw_listing_versions v
JOIN analytics.vw_listing_latest ON analytics.vw_listing_latest.listing_id = v.listing_id
WHERE v.prev_source_name = 'dataset' AND v.source_name = 'web'
  AND v.price_vnd > 0 AND v.prev_price_vnd > 0 AND v.price_vnd <> v.prev_price_vnd
ORDER BY (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd
LIMIT 15;

-- BLOCK I: round-number ratios. Counts pairs whose new/old price is exactly 10, 100, 1000 (or the inverse) and exactly 2.
-- Many round ratios point to data-entry errors, not market moves.
WITH p AS (
    SELECT ROUND(v.price_vnd / v.prev_price_vnd, 4) AS ty_so
    FROM analytics.vw_listing_versions v
    WHERE v.prev_source_name = 'dataset' AND v.source_name = 'web'
      AND v.price_vnd > 0 AND v.prev_price_vnd > 0 AND v.price_vnd <> v.prev_price_vnd
)
SELECT
    COUNT(*) FILTER (WHERE ty_so IN (10, 100, 1000)) AS tang_dung_10_100_1000_lan,
    COUNT(*) FILTER (WHERE ty_so IN (0.1, 0.01, 0.001)) AS giam_dung_10_100_1000_lan,
    COUNT(*) FILTER (WHERE ty_so = 2) AS tang_dung_2_lan,
    COUNT(*) FILTER (WHERE ty_so = 0.5) AS giam_dung_1_nua
FROM p;


-- BLOCK J: classify changed pairs by data-error signal (existing columns only, Gold untouched).
-- sua_don_vi: old/new (or new/old) price equals the inferred area within 0.5%: a "X tỷ / m²" label entered as a total price, or the reverse.
-- go_nham_so_0: new/old price is exactly 10, 100, 1000 or the inverse.
-- con_lai: no error signal (includes sale <-> rent type changes, see block K0).
WITH p AS (
    SELECT
        analytics.vw_listing_latest.listing_type,
        v.price_vnd / v.prev_price_vnd AS ty_so,
        v.price_vnd / NULLIF(analytics.vw_listing_latest.price_per_m2_vnd, 0) AS dien_tich
    FROM analytics.vw_listing_versions v
    JOIN analytics.vw_listing_latest ON analytics.vw_listing_latest.listing_id = v.listing_id
    WHERE v.prev_source_name = 'dataset' AND v.source_name = 'web'
      AND v.price_vnd > 0 AND v.prev_price_vnd > 0 AND v.price_vnd <> v.prev_price_vnd
), c AS (
    SELECT listing_type,
           CASE
               WHEN dien_tich > 1 AND ABS(1 / ty_so - dien_tich) <= 0.005 * dien_tich THEN 'sua_don_vi'
               WHEN dien_tich > 1 AND ABS(ty_so - dien_tich) <= 0.005 * dien_tich THEN 'sua_don_vi'
               WHEN ROUND(ty_so, 4) IN (10, 100, 1000, 0.1, 0.01, 0.001) THEN 'go_nham_so_0'
               ELSE 'con_lai'
           END AS nhom
    FROM p
)
SELECT listing_type, nhom, COUNT(*) AS so_cap
FROM c
GROUP BY listing_type, nhom
ORDER BY listing_type, nhom;

-- BLOCK L: sensitivity. Percentiles of % change for all pairs vs pairs without an error signal from block J.
-- The reduced set also drops pairs with no inferred area, so its n can be a few pairs below con_lai in block J.
-- If median and decrease share barely move, keep the cards and document the limit; otherwise add an exclusion rule.
WITH p AS (
    SELECT
        (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd AS pct,
        v.price_vnd / v.prev_price_vnd AS ty_so,
        v.price_vnd / NULLIF(analytics.vw_listing_latest.price_per_m2_vnd, 0) AS dien_tich
    FROM analytics.vw_listing_versions v
    JOIN analytics.vw_listing_latest ON analytics.vw_listing_latest.listing_id = v.listing_id
    WHERE v.prev_source_name = 'dataset' AND v.source_name = 'web'
      AND v.price_vnd > 0 AND v.prev_price_vnd > 0 AND v.price_vnd <> v.prev_price_vnd
), f AS (
    SELECT pct,
           (   (dien_tich > 1 AND ABS(1 / ty_so - dien_tich) <= 0.005 * dien_tich)
            OR (dien_tich > 1 AND ABS(ty_so - dien_tich) <= 0.005 * dien_tich)
            OR ROUND(ty_so, 4) IN (10, 100, 1000, 0.1, 0.01, 0.001)) AS co_loi
    FROM p
)
SELECT 'tat_ca' AS tap, COUNT(*) AS n,
       ROUND((100 * percentile_cont(0.25) WITHIN GROUP (ORDER BY pct))::numeric, 1) AS p25_pct,
       ROUND((100 * percentile_cont(0.50) WITHIN GROUP (ORDER BY pct))::numeric, 1) AS median_pct,
       ROUND((100 * percentile_cont(0.75) WITHIN GROUP (ORDER BY pct))::numeric, 1) AS p75_pct,
       ROUND((100.0 * COUNT(*) FILTER (WHERE pct < 0) / COUNT(*))::numeric, 1) AS ty_le_giam_pct
FROM f
UNION ALL
SELECT 'bo_cap_co_dau_hieu_loi', COUNT(*),
       ROUND((100 * percentile_cont(0.25) WITHIN GROUP (ORDER BY pct))::numeric, 1),
       ROUND((100 * percentile_cont(0.50) WITHIN GROUP (ORDER BY pct))::numeric, 1),
       ROUND((100 * percentile_cont(0.75) WITHIN GROUP (ORDER BY pct))::numeric, 1),
       ROUND((100.0 * COUNT(*) FILTER (WHERE pct < 0) / COUNT(*))::numeric, 1)
FROM f WHERE co_loi IS FALSE;  -- pairs without an inferred area (co_loi NULL) are also excluded here

-- BLOCK K0: find a per-version listing type column (to separate sale <-> rent type changes). Reads the view definition only.
SELECT pg_get_viewdef('analytics.vw_listing_versions'::regclass, true) AS dinh_nghia_view;
