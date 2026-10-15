-- analytics/sql/01_create_base_views.sql
-- Creates schema analytics and its 3 views. Read-only on gold.vw_fact_report and pipeline.run_state.
-- Idempotent (CREATE OR REPLACE). Run from the project root (PowerShell):
--   $env:PGCLIENTENCODING = 'UTF8'
--   psql "$env:POSTGRES_DW_DSN_LOCAL" -v ON_ERROR_STOP=1 -f analytics/sql/01_create_base_views.sql
-- If a view's column names, order or types change: DROP VIEW analytics.<name>; then re-run.
--
-- Sources:
--   gold.vw_fact_report has no row_hash, so it is recomputed here with the same formula as
--   silver.compute_row_hash (price_vnd, price_is_negotiable, is_expired, has_warning, area_m2).
--   pipeline.run_state holds DAG 2 runs only.

CREATE SCHEMA IF NOT EXISTS analytics;

-- ---------------------------------------------------------------------------
-- 1. vw_listing_latest: one row per listing (the row with MAX(valid_from)) plus flags and filter rules.
--    is_current / valid_to are not used: the SCD2 merge can invert them (docs/00_discovery.md section 3.1).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.vw_listing_latest AS
WITH latest AS (
    SELECT DISTINCT ON (f.listing_id) f.*
    FROM gold.vw_fact_report AS f
    ORDER BY f.listing_id, f.valid_from DESC, f.listing_key DESC
),
step1 AS (
    SELECT
        l.*,
        CASE l.listing_type
            WHEN 'Cần bán' THEN 'sale'
            WHEN 'Cho thuê' THEN 'rent'
        END AS deal_type,
        -- Region group by old province (province_new is always TP.HCM)
        CASE
            WHEN l.province_old ILIKE '%Bà Rịa%' THEN 'brvt_old'
            WHEN l.province_old ILIKE '%Bình Dương%' THEN 'binh_duong_old'
            ELSE 'hcm_old'
        END AS region_group,
        -- area_m2 / (length_m x width_m), only for plausible dimensions:
        -- length x width < 20 m2 is treated as unreliable (e.g. 1 x 1), see qa/06_r7_diagnose.sql
        CASE
            WHEN l.length_m > 0 AND l.width_m > 0 AND l.length_m * l.width_m >= 20
                THEN l.area_m2 / (l.length_m * l.width_m)
        END AS area_ratio,
        CASE
            WHEN l.area_m2 IS NULL THEN NULL
            WHEN l.area_m2 < 40 THEN '1_lt_40'
            WHEN l.area_m2 < 60 THEN '2_40_60'
            WHEN l.area_m2 < 100 THEN '3_60_100'
            ELSE '4_ge_100'
        END AS area_band,
        -- Usable per the Gold flags; NULL flags count as FALSE
        (
            l.price_per_m2_vnd IS NOT NULL
            AND NOT COALESCE(l.price_is_negotiable, FALSE)
            AND NOT COALESCE(l.price_is_outlier, FALSE)
        ) AS is_usable
    FROM latest AS l
),
step2 AS (
    SELECT
        s.*,
        COALESCE(s.area_ratio >= 8 AND s.area_ratio < 12, FALSE)
            OR COALESCE(s.area_ratio >= 80 AND s.area_ratio < 120, FALSE) AS area_suspect,
        COALESCE(s.deal_type = 'rent' AND s.price_vnd >= 1000000000, FALSE) AS is_commercial_building,
        COALESCE(s.deal_type = 'sale' AND s.price_per_m2_vnd >= 1000000000, FALSE) AS is_ultra_prime
    FROM step1 AS s
)
SELECT
    t.*,
    -- Priority order: the first matching rule wins (docs/metrics.md section 4)
    CASE
        WHEN t.deal_type = 'sale' AND t.price_per_m2_vnd < 1000000 THEN 'sale_ppm2_lt_1m'
        WHEN t.deal_type = 'rent' AND t.price_per_m2_vnd >= 10000000 THEN 'rent_ppm2_ge_10m'
        WHEN t.deal_type = 'rent' AND t.price_per_m2_vnd < 10000 THEN 'rent_ppm2_lt_10k'
        WHEN t.area_suspect THEN 'area_suspect'
    END AS exclude_reason,
    -- Single filter column for all price/m2 metrics
    (
        t.is_usable
        AND NOT t.is_commercial_building
        AND NOT t.area_suspect
        AND NOT COALESCE(
            (t.deal_type = 'sale' AND t.price_per_m2_vnd < 1000000)
            OR (t.deal_type = 'rent' AND (t.price_per_m2_vnd >= 10000000 OR t.price_per_m2_vnd < 10000)),
            FALSE
        )
    ) AS is_analysis_ready
FROM step2 AS t;

-- ---------------------------------------------------------------------------
-- 2. vw_listing_versions: version history without consecutive duplicate hashes.
--    Ordered by valid_from (valid_to / is_current are not trusted).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.vw_listing_versions AS
WITH hashed AS (
    SELECT
        f.listing_key,
        f.listing_id,
        f.valid_from,
        f.source_name,
        f.posted_date,
        f.is_expired,
        f.price_vnd,
        -- Same formula as silver.compute_row_hash (change both together)
        MD5(
            COALESCE(f.price_vnd::TEXT, 'NULL') || '|' ||
            f.price_is_negotiable::TEXT || '|' ||
            f.is_expired::TEXT || '|' ||
            f.has_warning::TEXT || '|' ||
            COALESCE(f.area_m2::TEXT, 'NULL')
        ) AS row_hash
    FROM gold.vw_fact_report AS f
),
ordered AS (
    SELECT
        h.*,
        LAG(h.row_hash) OVER w AS prev_row_hash
    FROM hashed AS h
    WINDOW w AS (PARTITION BY h.listing_id ORDER BY h.valid_from, h.listing_key)
),
kept AS (
    -- Keep the first version of each run of identical hashes
    SELECT *
    FROM ordered
    WHERE prev_row_hash IS NULL OR row_hash IS DISTINCT FROM prev_row_hash
),
numbered AS (
    SELECT
        k.listing_key,
        k.listing_id,
        ROW_NUMBER() OVER w AS version_no,
        k.valid_from,
        k.row_hash,
        k.source_name,
        k.posted_date,
        k.is_expired,
        k.price_vnd,
        LAG(k.price_vnd) OVER w AS prev_price_vnd,
        LAG(k.source_name) OVER w AS prev_source_name,
        LAG(k.valid_from) OVER w AS prev_valid_from
    FROM kept AS k
    WINDOW w AS (PARTITION BY k.listing_id ORDER BY k.valid_from, k.listing_key)
)
SELECT
    n.listing_key,
    n.listing_id,
    n.version_no,
    n.valid_from,
    n.row_hash,
    n.source_name,
    n.posted_date,
    n.is_expired,
    n.price_vnd,
    n.prev_price_vnd,
    n.prev_source_name,
    -- Vietnam time zone keeps results identical in DataGrip and Metabase (session time zone may differ)
    ((n.valid_from AT TIME ZONE 'Asia/Ho_Chi_Minh')::date
        - (n.prev_valid_from AT TIME ZONE 'Asia/Ho_Chi_Minh')::date) AS days_since_prev_version,
    (
        n.prev_price_vnd IS NOT NULL
        AND n.price_vnd IS NOT NULL
        AND n.price_vnd <> n.prev_price_vnd
    ) AS is_price_change,
    (n.prev_source_name IS NOT NULL AND n.source_name IS DISTINCT FROM n.prev_source_name) AS is_cross_source,
    CASE
        WHEN n.prev_price_vnd > 0 AND n.price_vnd IS NOT NULL
            THEN (n.price_vnd - n.prev_price_vnd)::numeric / n.prev_price_vnd
    END AS price_change_pct
FROM numbered AS n;

-- ---------------------------------------------------------------------------
-- 3. vw_crawl_coverage: DAG 2 runs per day from pipeline.run_state, including days with 0 runs.
--    Asia/Ho_Chi_Minh time zone; used as the "missing data day" mask.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.vw_crawl_coverage AS
WITH params AS (
    SELECT 20 AS full_day_min_runs   -- runs per day that count as a full day (DAG runs hourly)
),
runs AS (
    SELECT
        (r.started_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS crawl_date,
        COUNT(*) AS run_count,
        SUM(r.detail_pages_done) AS detail_pages_done
    FROM pipeline.run_state AS r
    GROUP BY 1
),
bounds AS (
    SELECT MIN(crawl_date) AS first_day, MAX(crawl_date) AS last_day
    FROM runs
),
days AS (
    SELECT gs::date AS crawl_date
    FROM bounds AS b,
         generate_series(b.first_day, b.last_day, INTERVAL '1 day') AS gs
)
SELECT
    d.crawl_date,
    COALESCE(r.run_count, 0)::int AS run_count,
    COALESCE(r.detail_pages_done, 0) AS detail_pages_done,
    CASE
        WHEN COALESCE(r.run_count, 0) = 0 THEN 'none'
        WHEN r.run_count < p.full_day_min_runs THEN 'partial'
        ELSE 'full'
    END AS coverage_level,
    (COALESCE(r.run_count, 0) = 0) AS is_missing_day,
    (COALESCE(r.run_count, 0) BETWEEN 1 AND p.full_day_min_runs - 1) AS is_partial_day
FROM days AS d
CROSS JOIN params AS p
LEFT JOIN runs AS r ON r.crawl_date = d.crawl_date;
