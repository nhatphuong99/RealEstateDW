-- analytics/sql/qa/recheck_flags_q1.sql
-- Re-checks (1) R1-R7 counts and is_analysis_ready, (2) total n of the districts in Q1 CARD 2.
-- Read-only on analytics.vw_listing_latest. Run one block at a time in DataGrip (select the block, Ctrl+Enter).

-- BLOCK A: recount R1-R7 on the latest versions (is_usable basis, same as QA D4a).
-- Compare with docs/metrics.md section 9.2: each count may change by at most the number of Gold rows added since that snapshot.
SELECT
    COUNT(*) AS so_listing,
    COUNT(*) FILTER (WHERE is_usable) AS usable,
    COUNT(*) FILTER (WHERE is_analysis_ready) AS analysis_ready,
    COUNT(*) FILTER (WHERE is_usable AND exclude_reason = 'sale_ppm2_lt_1m') AS r1_loai,
    COUNT(*) FILTER (WHERE is_usable AND deal_type = 'sale' AND price_per_m2_vnd >= 1e6 AND price_per_m2_vnd < 1e7) AS r2_giu_1_den_10_trieu,
    COUNT(*) FILTER (WHERE is_usable AND is_ultra_prime) AS r3_ultra_prime,
    COUNT(*) FILTER (WHERE is_usable AND exclude_reason = 'rent_ppm2_ge_10m') AS r4_loai,
    COUNT(*) FILTER (WHERE is_usable AND is_commercial_building) AS r5_tong_nha_thuong_mai,
    COUNT(*) FILTER (WHERE is_usable AND exclude_reason = 'rent_ppm2_lt_10k') AS r6_loai,
    COUNT(*) FILTER (WHERE area_suspect) AS r7_gan_co,
    COUNT(*) FILTER (WHERE is_usable AND exclude_reason = 'area_suspect') AS r7_bi_loai
FROM analytics.vw_listing_latest;

-- BLOCK B: internal identity. Expected: chenh = loai_theo_ly_do + tm_khong_bi_loai, i.e. sai_lech = 0.
SELECT
    COUNT(*) FILTER (WHERE is_usable) - COUNT(*) FILTER (WHERE is_analysis_ready) AS chenh,
    COUNT(*) FILTER (WHERE is_usable AND exclude_reason IS NOT NULL) AS loai_theo_ly_do,
    COUNT(*) FILTER (WHERE is_usable AND exclude_reason IS NULL AND is_commercial_building) AS tm_khong_bi_loai,
    (COUNT(*) FILTER (WHERE is_usable) - COUNT(*) FILTER (WHERE is_analysis_ready))
      - COUNT(*) FILTER (WHERE is_usable AND exclude_reason IS NOT NULL)
      - COUNT(*) FILTER (WHERE is_usable AND exclude_reason IS NULL AND is_commercial_building) AS sai_lech
FROM analytics.vw_listing_latest;

-- BLOCK C: total n of the districts in Q1 CARD 2 (Cần bán + Nhà trong hẻm + hcm_old, the default filters).
-- Expected: tong_n = tong_n_hien + tong_n_an + n_khong_quan (hidden districts and rows without a district explain the gap).
WITH g AS (
    SELECT district_old, COUNT(*) AS n
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND listing_type = 'Cần bán'
      AND property_type_name = 'Nhà trong hẻm'
      AND region_group = 'hcm_old'
    GROUP BY district_old
)
SELECT
    COUNT(*) FILTER (WHERE n >= 30) AS so_quan_hien,
    SUM(n) FILTER (WHERE n >= 30) AS tong_n_hien,
    COUNT(*) FILTER (WHERE n < 30 AND district_old IS NOT NULL) AS so_quan_an,
    COALESCE(SUM(n) FILTER (WHERE n < 30 AND district_old IS NOT NULL), 0) AS tong_n_an,
    COALESCE(SUM(n) FILTER (WHERE district_old IS NULL), 0) AS n_khong_quan,
    SUM(n) AS tong_n
FROM g;

-- BLOCK D: the 8 smallest district groups (same filters): shows which districts are hidden and the row without a district (district_old empty).
SELECT district_old, COUNT(*) AS n, COUNT(*) >= 30 AS hien_thi
FROM analytics.vw_listing_latest
WHERE is_analysis_ready
  AND listing_type = 'Cần bán'
  AND property_type_name = 'Nhà trong hẻm'
  AND region_group = 'hcm_old'
GROUP BY district_old
ORDER BY n
LIMIT 8;
