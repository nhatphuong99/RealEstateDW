-- ============================================================================
-- sql/queries/merge_scd2_listing_history.sql
-- SCD Type 2 merge: silver.listing_staging_batch -> silver.listing_history.
-- Runs after every Spark ETL (Bronze->Silver) writes a batch into staging_batch.
--
-- Design: 3 sequential UPDATE/INSERT steps, NOT combined into a single
-- MERGE statement (Postgres's MERGE doesn't handle "close the old row +
-- open a new one" well in one pass).
--
-- IMPORTANT: the LAG/LEAD results are materialized ONCE into temp tables
-- (scd2_ordered/scd2_change_points/scd2_same_hash) BEFORE running the 3
-- steps. If each step recomputed its own CTE instead, step 1 would flip
-- is_current on listing_history BEFORE steps 2/3 run -> a CTE recomputed
-- afterward would read data already mutated by step 1. The temp tables
-- avoid this class of bug.
--
-- The whole script runs in a single transaction for atomicity.
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------
-- Temp table 1: combined (staging + anchor) + prev_hash via LAG()
-- ----------------------------------------------------------------------
DROP TABLE IF EXISTS scd2_ordered;

CREATE TEMP TABLE scd2_ordered AS
WITH combined AS (
    -- Every NEW observation in this batch (source: the Spark Bronze->Silver ETL)
    SELECT
        listing_id, listing_url, source_part, source_bronze_key, crawl_date,
        row_hash, 'staging'::TEXT AS origin,
        title, listing_type, property_type, posted_date,
        price_vnd, price_raw, price_is_negotiable, price_is_outlier,
        area_m2, area_raw, area_is_undetermined, area_is_outlier,
        length_m, width_m, street_width_m, floors, bedrooms,
        orientation, legal_status,
        has_dining_room, has_kitchen, has_rooftop, has_car_parking, owner_direct,
        is_expired, has_warning,
        address_street_new, address_ward_new, address_province_new,
        address_old_raw, address_ward_old, address_district_old, address_province_old
    FROM silver.listing_staging_batch

    UNION ALL

    -- Every version already present in Silver (NOT just is_current), used
    -- as the "anchor" so LAG() knows the prev_hash for each staging row.
    -- Full history (not just is_current) is needed for idempotent
    -- re-runs — if only is_current were used, a staging row matching an
    -- older version would be misread by LAG() as "first ever appearance",
    -- creating an incorrect duplicate.
    SELECT
        listing_id, listing_url, source_part, source_bronze_key,
        valid_from AS crawl_date,
        row_hash, 'anchor'::TEXT AS origin,
        title, listing_type, property_type, posted_date,
        price_vnd, price_raw, price_is_negotiable, price_is_outlier,
        area_m2, area_raw, area_is_undetermined, area_is_outlier,
        length_m, width_m, street_width_m, floors, bedrooms,
        orientation, legal_status,
        has_dining_room, has_kitchen, has_rooftop, has_car_parking, owner_direct,
        is_expired, has_warning,
        address_street_new, address_ward_new, address_province_new,
        address_old_raw, address_ward_old, address_district_old, address_province_old
    FROM silver.listing_history
)
SELECT
    combined.*,
    LAG(row_hash) OVER (
        PARTITION BY listing_id
        ORDER BY crawl_date, origin  -- tie-break: 'anchor' < 'staging' alphabetically -> anchor always comes first on a tied crawl_date
    ) AS prev_hash
FROM combined;

-- ----------------------------------------------------------------------
-- Temp table 2: change_points — only STAGING rows whose hash genuinely
-- differs from the immediately preceding row (including vs. the anchor).
-- next_change_crawl_date/is_latest are computed OVER the change_points
-- SUBSET (not the full scd2_ordered), so valid_to jumps straight to the
-- next actual change point, skipping over unchanged observations in between.
-- ----------------------------------------------------------------------
DROP TABLE IF EXISTS scd2_change_points;

CREATE TEMP TABLE scd2_change_points AS
SELECT
    o.*,
    LEAD(o.crawl_date) OVER (PARTITION BY o.listing_id ORDER BY o.crawl_date) AS next_change_crawl_date,
    ROW_NUMBER() OVER (PARTITION BY o.listing_id ORDER BY o.crawl_date DESC) = 1 AS is_latest
FROM scd2_ordered o
WHERE o.origin = 'staging'
  AND o.prev_hash IS DISTINCT FROM o.row_hash;

-- ----------------------------------------------------------------------
-- Temp table 3: same_hash_rows — staging rows whose hash did NOT change,
-- only need last_seen_at bumped (no new version).
-- ----------------------------------------------------------------------
DROP TABLE IF EXISTS scd2_same_hash;

CREATE TEMP TABLE scd2_same_hash AS
SELECT listing_id, MAX(crawl_date) AS max_crawl_date
FROM scd2_ordered
WHERE origin = 'staging'
  AND prev_hash IS NOT DISTINCT FROM row_hash
GROUP BY listing_id;

-- ----------------------------------------------------------------------
-- Step 1: close out the is_current version being replaced (only for
-- listing_ids with a hash change in this batch). valid_to = that
-- listing_id's FIRST change point.
-- ----------------------------------------------------------------------
UPDATE silver.listing_history h
SET valid_to = cp.first_change_crawl_date,
    is_current = FALSE
FROM (
    SELECT listing_id, MIN(crawl_date) AS first_change_crawl_date
    FROM scd2_change_points
    GROUP BY listing_id
) cp
WHERE h.listing_id = cp.listing_id
  AND h.is_current;

-- ----------------------------------------------------------------------
-- Step 2: insert every change point as a new version.
-- is_current is TRUE only for the most recent change point (is_latest) of
-- each listing_id; valid_to = next_change_crawl_date (NULL for the latest
-- version). row_hash is NOT inserted (GENERATED STORED, computed by Postgres).
-- ----------------------------------------------------------------------
INSERT INTO silver.listing_history (
    listing_id, listing_url, source_part, source_bronze_key,
    valid_from, valid_to, is_current, last_seen_at,
    title, listing_type, property_type, posted_date,
    price_vnd, price_raw, price_is_negotiable, price_is_outlier,
    area_m2, area_raw, area_is_undetermined, area_is_outlier,
    length_m, width_m, street_width_m, floors, bedrooms,
    orientation, legal_status,
    has_dining_room, has_kitchen, has_rooftop, has_car_parking, owner_direct,
    is_expired, has_warning,
    address_street_new, address_ward_new, address_province_new,
    address_old_raw, address_ward_old, address_district_old, address_province_old
)
SELECT
    listing_id, listing_url, source_part, source_bronze_key,
    crawl_date AS valid_from,
    next_change_crawl_date AS valid_to,
    is_latest AS is_current,
    crawl_date AS last_seen_at,
    title, listing_type, property_type, posted_date,
    price_vnd, price_raw, price_is_negotiable, price_is_outlier,
    area_m2, area_raw, area_is_undetermined, area_is_outlier,
    length_m, width_m, street_width_m, floors, bedrooms,
    orientation, legal_status,
    has_dining_room, has_kitchen, has_rooftop, has_car_parking, owner_direct,
    is_expired, has_warning,
    address_street_new, address_ward_new, address_province_new,
    address_old_raw, address_ward_old, address_district_old, address_province_old
FROM scd2_change_points;

-- ----------------------------------------------------------------------
-- Step 3: bump last_seen_at for listing_ids whose hash did NOT change in
-- this batch (no new version inserted, just confirms the latest crawl time).
-- ----------------------------------------------------------------------
UPDATE silver.listing_history h
SET last_seen_at = s.max_crawl_date
FROM scd2_same_hash s
WHERE h.listing_id = s.listing_id
  AND h.is_current
  AND s.max_crawl_date > h.last_seen_at;

COMMIT;

-- Temp tables (scd2_ordered/scd2_change_points/scd2_same_hash) are dropped
-- automatically when the psql/connection session ends — no manual DROP needed.
