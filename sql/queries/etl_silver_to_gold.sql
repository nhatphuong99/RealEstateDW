-- ============================================================================
-- sql/queries/etl_silver_to_gold.sql
-- ETL Silver -> Gold: idempotent full-refresh, runs in a single transaction.
-- "Full-refresh" = every run scans the ENTIRE silver.listing_history, not
-- incremental by time. NOT truncate-and-reload (no TRUNCATE here) — the 5
-- Dims are insert-only via ON CONFLICT DO NOTHING (surrogate keys stay
-- stable across runs); the Fact uses ON CONFLICT DO UPDATE (upsert)
-- because silver.listing_history is not immutable —
-- merge_scd2_listing_history.sql can UPDATE is_current/valid_to on an
-- existing row, and Gold must reflect that correctly, not just insert-once.
--
-- Silver and Gold live in the same Postgres -> transform directly in SQL,
-- no need to pull data out into Spark/Python (unlike Bronze->Silver, which
-- needs Spark to parse raw HTML).
--
-- Order matters: load the 5 Dims first (idempotent via ON CONFLICT DO
-- NOTHING), then load the Fact (JOIN to pick up surrogate keys).
--
-- DEFENSIVE CODING: every STRING column feeding dim_location/
-- dim_property_features is wrapped in COALESCE(..., '') both when loading
-- the Dims and when JOINing the Fact, even though Silver already enforces
-- NOT NULL DEFAULT ''. If a string column is ever genuinely NULL
-- (regression/legacy data), skipping the COALESCE would make the INSERT
-- raise a NOT NULL violation -> aborts the whole 6-step transaction ->
-- fact_listing_price drops to 0 rows even though Silver has plenty of
-- data (the "row_count_match: expected=N, actual=0" symptom). COALESCE
-- turns that hard failure into one valid ''/'' dim row instead — it won't
-- crash the batch, but data can still be off; use
-- diagnose_gold_join_loss.sql to investigate if needed.
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------
-- 1. DIM_DATE - continuous calendar, spanning the min to max posted_date in Silver
-- ----------------------------------------------------------------------
INSERT INTO gold.dim_date (date_key, full_date)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d
FROM generate_series(
    (SELECT MIN(posted_date) FROM silver.listing_history),
    (SELECT MAX(posted_date) FROM silver.listing_history),
    INTERVAL '1 day'
) AS d
ON CONFLICT (date_key) DO NOTHING;

-- ----------------------------------------------------------------------
-- 2. DIM_LOCATION - defensive COALESCE.
-- ----------------------------------------------------------------------
INSERT INTO gold.dim_location (province_new, ward_new, province_old, ward_old, district_old, street)
SELECT DISTINCT
    COALESCE(address_province_new, ''),
    COALESCE(address_ward_new, ''),
    COALESCE(address_province_old, ''),
    COALESCE(address_ward_old, ''),
    COALESCE(address_district_old, ''),
    COALESCE(address_street_new, '')
FROM silver.listing_history
ON CONFLICT (province_new, ward_new, province_old, ward_old, district_old, street) DO NOTHING;

-- ----------------------------------------------------------------------
-- 3. DIM_PROPERTY_TYPE - exactly 10 fixed combinations. No COALESCE needed:
--    property_type/listing_type are unconditionally NOT NULL in Silver.
-- ----------------------------------------------------------------------
INSERT INTO gold.dim_property_type (property_type_name, listing_type)
SELECT DISTINCT property_type, listing_type
FROM silver.listing_history
ON CONFLICT (property_type_name, listing_type) DO NOTHING;

-- ----------------------------------------------------------------------
-- 4. DIM_SOURCE - infer source_name from the source_bronze_key prefix via
-- gold.infer_source_from_bronze_key() (schema_full.sql) — the single
-- source of truth on the SQL side, reused in step 6's Fact JOIN and in
-- diagnose_gold_join_loss.sql. Must still stay in sync with the Python
-- function infer_source_from_bronze_key() (parser/bronze_to_silver_core.py)
-- — changing the S3 key convention requires updating BOTH functions (SQL
-- + Python) in sync, since SQL and Python are separate runtimes and can't
-- share one function.
-- ----------------------------------------------------------------------
INSERT INTO gold.dim_source (source_name, source_part)
SELECT DISTINCT
    gold.infer_source_from_bronze_key(source_bronze_key) AS source_name,
    source_part
FROM silver.listing_history
ON CONFLICT (source_name, source_part) DO NOTHING;

-- ----------------------------------------------------------------------
-- 5. DIM_PROPERTY_FEATURES - feature_key is GENERATED STORED (computed by
--    Postgres via gold.compute_feature_key()), NEVER insert this column
--    manually. Defensive COALESCE for orientation/legal_status — see the
--    file header.
-- ----------------------------------------------------------------------
INSERT INTO gold.dim_property_features (
    orientation, legal_status, has_dining_room, has_kitchen,
    has_rooftop, has_car_parking, owner_direct
)
SELECT DISTINCT
    COALESCE(orientation, ''), COALESCE(legal_status, ''),
    has_dining_room, has_kitchen, has_rooftop, has_car_parking, owner_direct
FROM silver.listing_history
ON CONFLICT (feature_key) DO NOTHING;

-- ----------------------------------------------------------------------
-- 6. FACT_LISTING_PRICE - UPSERT.
--    Joins pick up the surrogate keys from the Dims just loaded — the
--    JOIN conditions use the SAME COALESCE as steps 2/5; otherwise a Dim
--    row with '' could fail to match Silver's NULL (`'' = NULL` is always
--    UNKNOWN), dropping rows even though the Dim row already exists.
--
--    feature_key is recomputed by calling gold.compute_feature_key()
--    directly instead of JOINing dim_property_features — the function is
--    IMMUTABLE and NULL-safe, avoiding dropped rows when has_*/owner_direct
--    are nullable BOOLEANs.
-- ----------------------------------------------------------------------
INSERT INTO gold.fact_listing_price (
    listing_key, listing_id,
    location_key, property_type_key, feature_key, source_key,
    posted_date_key,
    valid_from, valid_to, is_current,
    price_vnd, price_per_m2_vnd, area_m2,
    bedrooms, floors, length_m, width_m, street_width_m,
    price_is_negotiable, price_is_outlier, area_is_undetermined, area_is_outlier,
    has_warning, is_expired
)
SELECT
    h.listing_key,
    h.listing_id,
    loc.location_key,
    pt.property_type_key,
    gold.compute_feature_key(
        COALESCE(h.orientation, ''), COALESCE(h.legal_status, ''), h.has_dining_room, h.has_kitchen,
        h.has_rooftop, h.has_car_parking, h.owner_direct
    ) AS feature_key,
    src.source_key,
    TO_CHAR(h.posted_date, 'YYYYMMDD')::INTEGER AS posted_date_key,
    h.valid_from,
    h.valid_to,
    h.is_current,
    h.price_vnd,
    h.price_per_m2_vnd,
    h.area_m2,
    h.bedrooms,
    h.floors,
    h.length_m,
    h.width_m,
    h.street_width_m,
    h.price_is_negotiable,
    h.price_is_outlier,
    h.area_is_undetermined,
    h.area_is_outlier,
    h.has_warning,
    h.is_expired
FROM silver.listing_history h
JOIN gold.dim_location loc
    ON loc.province_new = COALESCE(h.address_province_new, '')
   AND loc.ward_new = COALESCE(h.address_ward_new, '')
   AND loc.province_old = COALESCE(h.address_province_old, '')
   AND loc.ward_old = COALESCE(h.address_ward_old, '')
   AND loc.district_old = COALESCE(h.address_district_old, '')
   AND loc.street = COALESCE(h.address_street_new, '')
JOIN gold.dim_property_type pt
    ON pt.property_type_name = h.property_type
   AND pt.listing_type = h.listing_type
JOIN gold.dim_source src
    ON src.source_name = gold.infer_source_from_bronze_key(h.source_bronze_key)
   AND src.source_part = h.source_part
ON CONFLICT (listing_key) DO UPDATE SET
    listing_id                 = EXCLUDED.listing_id,
    location_key                = EXCLUDED.location_key,
    property_type_key           = EXCLUDED.property_type_key,
    feature_key                  = EXCLUDED.feature_key,
    source_key                   = EXCLUDED.source_key,
    posted_date_key                = EXCLUDED.posted_date_key,
    valid_from                       = EXCLUDED.valid_from,
    valid_to                          = EXCLUDED.valid_to,
    is_current                          = EXCLUDED.is_current,
    price_vnd                            = EXCLUDED.price_vnd,
    price_per_m2_vnd                      = EXCLUDED.price_per_m2_vnd,
    area_m2                                = EXCLUDED.area_m2,
    bedrooms                                = EXCLUDED.bedrooms,
    floors                                   = EXCLUDED.floors,
    length_m                                  = EXCLUDED.length_m,
    width_m                                    = EXCLUDED.width_m,
    street_width_m                              = EXCLUDED.street_width_m,
    price_is_negotiable                          = EXCLUDED.price_is_negotiable,
    price_is_outlier                              = EXCLUDED.price_is_outlier,
    area_is_undetermined                           = EXCLUDED.area_is_undetermined,
    area_is_outlier                                 = EXCLUDED.area_is_outlier,
    has_warning                                      = EXCLUDED.has_warning,
    is_expired                                        = EXCLUDED.is_expired;

COMMIT;
