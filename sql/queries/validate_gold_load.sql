-- ============================================================================
-- sql/queries/validate_gold_load.sql
-- Validation run after etl_silver_to_gold.sql.
--
-- Returns 1 row per check (check_name, expected, actual, passed). The
-- caller (parser/silver_to_gold_io.py::validate_gold_load()) reads every
-- row and raises RuntimeError listing which checks have passed=FALSE —
-- Airflow marks the task failed instead of silently passing on bad Gold data.
--
-- If check_row_count fails (especially actual=0): run
-- sql/queries/diagnose_gold_join_loss.sql to find out exactly which JOIN
-- (or schema drift) is dropping rows, instead of guessing.
-- ============================================================================

WITH check_row_count AS (
    -- COUNT(fact) must match COUNT(silver) exactly — immediately surfaces
    -- a dropped-row JOIN in the Fact (e.g. dim_source returning NULL) or
    -- the whole transaction being aborted.
    SELECT
        'row_count_match' AS check_name,
        (SELECT COUNT(*) FROM silver.listing_history)::TEXT AS expected,
        (SELECT COUNT(*) FROM gold.fact_listing_price)::TEXT AS actual,
        (SELECT COUNT(*) FROM silver.listing_history)
            = (SELECT COUNT(*) FROM gold.fact_listing_price) AS passed
),
check_current_uniqueness AS (
    -- Exactly 1 is_current=TRUE row per listing_id — the Fact uses UPSERT,
    -- easy to get wrong if the "close out the old version" step is missing
    -- across repeated runs.
    SELECT
        'is_current_unique_per_listing' AS check_name,
        '0' AS expected,
        COUNT(*)::TEXT AS actual,
        COUNT(*) = 0 AS passed
    FROM (
        SELECT listing_id
        FROM gold.fact_listing_price
        WHERE is_current
        GROUP BY listing_id
        HAVING COUNT(*) > 1
    ) dup
),
check_fk_not_null AS (
    SELECT
        'fact_fk_not_null' AS check_name,
        '0' AS expected,
        COUNT(*)::TEXT AS actual,
        COUNT(*) = 0 AS passed
    FROM gold.fact_listing_price
    WHERE location_key IS NULL
       OR property_type_key IS NULL
       OR feature_key IS NULL
       OR source_key IS NULL
       OR posted_date_key IS NULL
),
check_price_per_m2_flagged AS (
    -- Outliers are allowed to exist, as long as they're already flagged
    -- price_is_outlier=TRUE in Silver. Only flags an error when a row is
    -- over the threshold but NOT flagged — meaning the parser missed it,
    -- not that the data itself is bad.
    SELECT
        'price_per_m2_extreme_all_flagged' AS check_name,
        '0' AS expected,
        COUNT(*)::TEXT AS actual,
        COUNT(*) = 0 AS passed
    FROM gold.fact_listing_price
    WHERE price_per_m2_vnd IS NOT NULL
      AND price_per_m2_vnd > 5000000000
      AND NOT price_is_outlier
),
check_area_within_sanitized_bounds AS (
    -- Regression guard for _sanitize_area(): every non-NULL area_m2 with
    -- area_is_outlier=FALSE must fall within [3, 10000] — anything outside
    -- that range means the sanitize function has been changed/broken and
    -- needs immediate attention.
    SELECT
        'area_within_sanitized_bounds' AS check_name,
        '0' AS expected,
        COUNT(*)::TEXT AS actual,
        COUNT(*) = 0 AS passed
    FROM gold.fact_listing_price
    WHERE area_m2 IS NOT NULL
      AND NOT area_is_outlier
      AND (area_m2 < 3 OR area_m2 > 10000)
)
SELECT * FROM check_row_count
UNION ALL
SELECT * FROM check_current_uniqueness
UNION ALL
SELECT * FROM check_fk_not_null
UNION ALL
SELECT * FROM check_price_per_m2_flagged
UNION ALL
SELECT * FROM check_area_within_sanitized_bounds;
