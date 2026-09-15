-- ============================================================================
-- sql/queries/diagnose_gold_join_loss.sql
-- DIAGNOSTIC TOOL — run when validate_gold_load reports row_count_match is
-- off (especially actual=0). Counts, per dimension, how many
-- silver.listing_history rows fail to join in etl_silver_to_gold.sql's
-- step 6, so you know exactly which JOIN is dropping rows instead of
-- guessing.
--
-- USAGE: run after etl_silver_to_gold.sql has finished (steps 1-5 already
-- loaded the Dims). Any dim with "orphan_count" > 0 is where to look.
-- ============================================================================

-- 1. How many Silver rows fail to match gold.dim_location?
--    (uses the same JOIN condition as etl_silver_to_gold.sql step 6)
SELECT
    'dim_location' AS join_target,
    COUNT(*) AS orphan_count,
    COUNT(*) FILTER (
        WHERE h.address_province_new IS NULL OR h.address_ward_new IS NULL
           OR h.address_province_old IS NULL OR h.address_ward_old IS NULL
           OR h.address_district_old IS NULL OR h.address_street_new IS NULL
    ) AS orphan_with_real_null   -- >0 means Silver has a genuine NULL (not '') in an address column
FROM silver.listing_history h
LEFT JOIN gold.dim_location loc
    ON loc.province_new = COALESCE(h.address_province_new, '')
   AND loc.ward_new = COALESCE(h.address_ward_new, '')
   AND loc.province_old = COALESCE(h.address_province_old, '')
   AND loc.ward_old = COALESCE(h.address_ward_old, '')
   AND loc.district_old = COALESCE(h.address_district_old, '')
   AND loc.street = COALESCE(h.address_street_new, '')
WHERE loc.location_key IS NULL

UNION ALL

-- 2. How many Silver rows fail to match gold.dim_property_type?
SELECT
    'dim_property_type' AS join_target,
    COUNT(*) AS orphan_count,
    COUNT(*) FILTER (WHERE h.property_type IS NULL OR h.listing_type IS NULL) AS orphan_with_real_null
FROM silver.listing_history h
LEFT JOIN gold.dim_property_type pt
    ON pt.property_type_name = h.property_type
   AND pt.listing_type = h.listing_type
WHERE pt.property_type_key IS NULL

UNION ALL

-- 3. How many Silver rows fail to match gold.dim_source?
SELECT
    'dim_source' AS join_target,
    COUNT(*) AS orphan_count,
    0 AS orphan_with_real_null
FROM silver.listing_history h
LEFT JOIN gold.dim_source src
    ON src.source_name = gold.infer_source_from_bronze_key(h.source_bronze_key)
   AND src.source_part = h.source_part
WHERE src.source_key IS NULL

UNION ALL

-- 4. Total rows that match all 3 dimensions — the actual number of rows
--    that will be INSERTed into the Fact. If this is 0 even though
--    (1)+(2)+(3) are all 0, the issue is likely on the Python side (wrong
--    SQL file executed, or a wrong DSN/schema search_path).
SELECT
    'joined_all_3_dims' AS join_target,
    COUNT(*) AS orphan_count,
    0 AS orphan_with_real_null
FROM silver.listing_history h
JOIN gold.dim_location loc
    ON loc.province_new = COALESCE(h.address_province_new, '')
   AND loc.ward_new = COALESCE(h.address_ward_new, '')
   AND loc.province_old = COALESCE(h.address_province_old, '')
   AND loc.ward_old = COALESCE(h.address_ward_old, '')
   AND loc.district_old = COALESCE(h.address_district_old, '')
   AND loc.street = COALESCE(h.address_street_new, '')
JOIN gold.dim_property_type pt
    ON pt.property_type_name = h.property_type AND pt.listing_type = h.listing_type
JOIN gold.dim_source src
    ON src.source_name = gold.infer_source_from_bronze_key(h.source_bronze_key)
   AND src.source_part = h.source_part;
