# analytics/docs/data_dictionary.md — Data dictionary of the 3 views

> Schema `analytics`, defined in `sql/01_create_base_views.sql`. "Gold" columns are taken as-is from `gold.vw_fact_report`. Metric definitions and rules: `metrics.md`.

## 1. `analytics.vw_listing_latest`

- **Grain / key:** one row per `listing_id` (the version with `MAX(valid_from)`); `listing_id` is unique.
- **Source:** `gold.vw_fact_report`. **Used by:** Q1, Q2, Q3, Q4, Q7 (filter on `is_analysis_ready`); Q5 uses it to get the listing type; Q6 CARD 7.

| Column | Derivation / note |
|---|---|
| Gold columns: `listing_key`, `listing_id`, `price_vnd`, `price_per_m2_vnd`, `area_m2`, `bedrooms`, `floors`, `length_m`, `width_m`, `street_width_m`, `has_warning`, `is_expired`, `posted_date`, `valid_from` | Taken as-is |
| Gold columns: `price_is_negotiable`, `price_is_outlier`, `area_is_undetermined`, `area_is_outlier` | Gold quality flags |
| Gold columns: `province_new`, `ward_new`, `province_old`, `ward_old`, `district_old`, `street`, `ward_new_map_key`, `district_old_map_key` | District = `district_old`; ward = `ward_new` |
| Gold columns: `property_type_name`, `listing_type`, `source_name`, `legal_status`, `orientation`, `owner_direct`, `has_dining_room`, `has_kitchen`, `has_rooftop`, `has_car_parking` | The `has_*` flags and `owner_direct` are tri-state: `TRUE` or `NULL`, never a real `FALSE` |
| Gold columns: `is_current`, `valid_to` | **Unreliable** (SCD2 merge, see `metrics.md` section 6.1); do not use |
| `deal_type` | `'Cần bán'` → `sale`; `'Cho thuê'` → `rent`; the key that splits sale from rent |
| `region_group` | `province_old` containing "Bà Rịa" → `brvt_old`; containing "Bình Dương" → `binh_duong_old`; otherwise `hcm_old` |
| `area_ratio` | `area_m2 / (length_m × width_m)` when `length_m > 0`, `width_m > 0` and length × width ≥ 20 m²; otherwise `NULL` |
| `area_band` | `1_lt_40`, `2_40_60`, `3_60_100`, `4_ge_100`; `NULL` if area is missing |
| `is_usable` | `price_per_m2_vnd IS NOT NULL AND NOT price_is_negotiable AND NOT price_is_outlier`; a `NULL` flag counts as `FALSE` |
| `area_suspect` | `area_ratio` in [8, 12) or [80, 120) (R7) |
| `is_commercial_building` | `deal_type = 'rent' AND price_vnd >= 1 billion` (R5) |
| `is_ultra_prime` | `deal_type = 'sale' AND price_per_m2_vnd >= 1 billion` (R3); flag only, not excluded |
| `exclude_reason` | `sale_ppm2_lt_1m`, `rent_ppm2_ge_10m`, `rent_ppm2_lt_10k`, `area_suspect` in priority order; `NULL` if not excluded |
| `is_analysis_ready` | `is_usable AND NOT is_commercial_building AND exclude_reason IS NULL`; the **single filter column** for price-per-m² metrics |

## 2. `analytics.vw_listing_versions`

- **Grain / key:** one row per real version (rows whose `row_hash` equals the previous version's by `valid_from` are removed); key (`listing_id`, `version_no`); `listing_key` is also unique.
- **Source:** `gold.vw_fact_report`. **Used by:** Q5; Q6 (duplicate-hash versions = Gold rows minus the view's rows).

| Column | Derivation / note |
|---|---|
| `listing_key`, `listing_id`, `valid_from`, `source_name`, `posted_date`, `is_expired`, `price_vnd` | Taken from the kept row (`listing_key` for traceability) |
| `version_no` | `ROW_NUMBER()` by `valid_from` after removing duplicates; starts at 1 |
| `row_hash` | `MD5` of `price_vnd`, `price_is_negotiable`, `is_expired`, `has_warning`, `area_m2`, same formula as `silver.compute_row_hash` (Gold has no such column, so it is recomputed). A new version can come from `is_expired`, `has_warning` or area changing, not necessarily price |
| `prev_price_vnd`, `prev_source_name` | Price and source of the previous version; `NULL` at version 1 |
| `days_since_prev_version` | VN date (`Asia/Ho_Chi_Minh`) of `valid_from` minus the VN date of the previous version |
| `is_price_change` | Both prices non-`NULL` and different |
| `is_cross_source` | `source_name` differs from `prev_source_name` |
| `price_change_pct` | `(price_vnd − prev_price_vnd) / prev_price_vnd` (ratio, negative = drop); `NULL` if a price is missing |

There is no `valid_to` or `is_current`: both are wrong for 4,850 listings.

## 3. `analytics.vw_crawl_coverage`

- **Grain / key:** one row per day (`Asia/Ho_Chi_Minh` time), continuous from the first run day to the last run day, including days with no run; key `crawl_date`.
- **Source:** `pipeline.run_state` (contains only DAG 2 runs). **Used by:** Q6 and the Q7 missing-day mask. It does not describe the dataset, which was loaded once.

| Column | Derivation / note |
|---|---|
| `crawl_date` | VN date of `started_at` |
| `run_count` | Number of `run_state` rows in the day; 0 for days with no run |
| `detail_pages_done` | Sum of `detail_pages_done` in the day |
| `coverage_level` | `none` (0 runs), `partial` (1 to under 20), `full` (20 or more) |
| `is_missing_day` | `run_count = 0` |
| `is_partial_day` | `run_count` from 1 to 19 |
