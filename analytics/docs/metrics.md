# analytics/docs/metrics.md — Metrics, filter rules and results

> Snapshot: crawl up to 2026-10-04; figures measured on 2026-10-12 (251,418 Gold rows, 245,000 listings).
> Sections 1–4 and 6–7: definitions, rules, limitations. Section 5: numbers produced from this project's data. View columns: `data_dictionary.md`; how it was checked: `qa.md`.

## 1. Principles and units

- Reads only `gold.vw_fact_report` (and `pipeline.run_state` for `vw_crawl_coverage`). Gold, Silver, the pipeline and the 13 existing cards are not modified.
- All filter rules and flags live in one place (`analytics.vw_listing_latest`); cards only filter on the flag columns.
- The latest version is chosen by `valid_from`, not by Gold's `is_current` / `valid_to` (section 6.1).
- Sale and rent listings are never mixed in the same calculation.

| Quantity | Stored in the view | Displayed |
|---|---|---|
| Sale price per m² | VND/m² | million VND/m² |
| Rent price per m² | VND/m²/month | thousand VND/m²/month |
| Listing price | VND (rent: VND/month) | billion or million VND |
| Ratios | decimal | % |

## 2. Metrics

**Shared**

| Metric | Definition |
|---|---|
| Latest version | The row with `MAX(valid_from)` for each `listing_id` |
| `is_usable` | `price_per_m2_vnd IS NOT NULL AND NOT price_is_negotiable AND NOT price_is_outlier` |
| `is_analysis_ready` | `is_usable AND exclude_reason IS NULL AND NOT is_commercial_building`; the single filter column for price per m² |
| Median, p25, p75 | `percentile_cont(0.5 / 0.25 / 0.75)`; same result as `pandas.Series.median()` and `quantile()` |
| n and sample threshold | `COUNT(*)` of `is_analysis_ready` listings in the group; groups with `n < 30` hide median, p25, p75 (only n is shown) |
| No mean | Mean appears only in QA; the tails are heavy, especially for rent |
| `area_band` | `1_lt_40`, `2_40_60`, `3_60_100`, `4_ge_100`; `NULL` if area is missing |
| District, ward | District = `district_old` (Q1, Q3, Q4); ward = `ward_new` (Q2; the Q1 map uses `ward_new_map_key`). `ward_new` is not nested inside `district_old`; each `district_old` belongs to exactly one `region_group` |
| Real version | A version whose `row_hash` differs from the previous version by `valid_from`; `row_hash` is computed from `price_vnd`, `price_is_negotiable`, `is_expired`, `has_warning`, `area_m2` (listing type is not included) |

**Per tab**

| Tab | Metric | Who uses the result to decide what |
|---|---|---|
| Q1 | median, p25, p75, n by area, property type, sale/rent. Sale: ward level for frontage house, alley house, villa; apartment and boarding house at district level. Rent: district or `region_group` | Buyers, brokers: fair pricing, negotiation by area |
| Q2 | Premium = median(frontage house) ÷ median(alley house) − 1, sale listings only. Cell = `ward_new` × `area_band`, both groups n ≥ 30. Headline = median premium of the qualifying cells (unweighted) | Townhouse buyers: whether to pay extra for frontage |
| Q3 | Gross yield = 12 × median monthly rent price per m² ÷ median sale price per m². Cell = `region_group` × `district_old` × property type, both sale and rent n ≥ 30; apartment, frontage house, alley house only; an area-level estimate, not the same asset. Headline = median yield of the qualifying cells | Investors: choosing district and property type to rent out |
| Q4 | Discount = median(group) ÷ median(base group `2_40_60`) − 1, sale only (hard-coded `deal_type = 'sale'`), within the same property type × district; both the group and the base group n ≥ 30. Headline = median discount of the qualifying cells | Buyers, brokers: choosing an area band, price-per-m² gap |
| Q5 | Pair of previous version (dataset) → next version (web) of the same `listing_id` in `vw_listing_versions`, both prices > 0. Price change when the two prices differ; pct = (new price − old price) ÷ old price, computed in SQL; statistics hidden when n < 30; suspect pairs are not removed because percentiles are robust to tails. Listing type comes from the latest version. Gold only, so there is **no** "% of listings that change price" (it needs a denominator from Silver) | Brokers, sellers: the expected drop when a listing is refreshed; no market inference |
| Q6 | Number of days with no run and with partial runs (`vw_crawl_coverage`), duplicate-hash versions removed, DAG 2 stop reasons, `is_expired` by source | Dashboard owner: which days and sources not to interpret; where to fix the pipeline first |
| Q7 | Median sale price per m² by ISO week, two sources as separate series, not joined; weeks containing a day with a missing crawl are flagged | Reader: whether the price moves abnormally (not a market trend) |

## 3. Filter rules (R1–R9)

`exclude_reason` follows the priority order R1, R4, R6, R7: the first matching rule assigns it and stops. Basis for R1–R5: a sample of listings classified by URL title, original pages not opened.

| # | Condition | Action | Listings | Basis |
|---|---|---|---|---|
| R1 | Sale, price per m² < 1,000,000 | Exclude (`sale_ppm2_lt_1m`) | 31 | 18/20 wrong |
| R2 | Sale, price per m² from 1 to 10 million | Keep (valid houses, suburban land) | 2,234 | 18/20 valid |
| R3 | Sale, price per m² ≥ 1 billion | Keep, flag `is_ultra_prime` | 1,575 | 17/20 valid |
| R4 | Rent, price per m² ≥ 10,000,000 | Exclude (`rent_ppm2_ge_10m`) | 12 | 10/11 wrong |
| R5 | Rent, price ≥ 1 billion/month | Separate segment `is_commercial_building`, not in `is_analysis_ready` | 104 | 18/20 are real commercial buildings |
| R6 | Rent, price per m² < 10,000 | Exclude (`rent_ppm2_lt_10k`) | 4 | Checked by hand, 4/4 listings |
| R7 | `area_ratio = area_m2 / (length_m × width_m)` in [8, 12) or [80, 120), only when length × width ≥ 20 m² (below 20 m² the dimensions are fake) | Flag `area_suspect`, exclude from price per m² | 282 | Dropped decimal point, or `area_m2` is the total floor area of a multi-storey house |
| R8 | `row_hash` equals the previous version's | Excluded from `vw_listing_versions` | 4,350 | SCD2 merge inverted the order (section 6.1) |
| R9 | Day with no run at all | `is_missing_day` mask | 17/40 days | Section 6.4 |

R1–R7 counts: latest version, `is_usable`. R8 counts over all Gold rows; R9 over `vw_crawl_coverage`. A rent `price_vnd` is VND/month; the rent price per m² of commercial buildings may be computed on land area, so it is not comparable with residential. Gold's `price_is_outlier` flag (> 5 billion/m²) protects sale listings but not rent listings, hence R4–R6.

## 4. Time windows and filters

**Time windows**

| Tab | Window |
|---|---|
| Q1–Q4 | Latest version regardless of `posted_date`; has a Source filter |
| Q5 | Dataset → web pairs 14–98 days apart (median 38) |
| Q6 | By crawl day of `vw_crawl_coverage` |
| Q7 | Full ISO weeks: dataset `posted_date` 2026-06-29 → 08-02 (5 weeks), web 2026-08-24 → 09-13 (3 weeks). The 08-03 week is kept only for the sensitivity check (its listing count is clearly higher, so it is excluded). The two series are not joined |

**"Missing data day" mask** (`vw_crawl_coverage`, `Asia/Ho_Chi_Minh` time): `coverage_level` = `none` (0 runs), `partial` (1 to under 20 runs), `full` (20 or more, because the DAG runs hourly); `is_missing_day` = 0 runs. Q7 flags a web week with at least one `is_missing_day`; `partial` days are only noted. The view describes only the web crawler (DAG 2); the 2026-08-14 → 08-25 gap between the two sources is outside the view.

**Parameters:** sample threshold 30 listings/group; `area_band` 40 / 60 / 100 m²; Q4 base group `2_40_60`; `full_day_min_runs` 20.

**Default filters (dashboard level)**

| Tab | Filters |
|---|---|
| Q1 | Listing type = Cần bán (for sale), property type = Nhà trong hẻm (alley house), region = hcm_old, Source left empty |
| Q2 | Region = hcm_old; Area band left empty |
| Q3 | Property type = Nhà trong hẻm (linked to CARD 2, 3); region = hcm_old; Source left empty |
| Q4 | Property type = Nhà trong hẻm (linked to CARD 1, 2); District linked to all 3 cards |
| Q5 | Listing type = Cần bán (both types have enough sample: 907 and 89 price-change pairs); cleared or both selected = combined (996) |
| Q6 | No filters |
| Q7 | Property type = Nhà trong hẻm, region = hcm_old |

Filter rules:
- A condition that always holds is hard-coded in SQL; a condition the viewer changes uses a Field Filter + dashboard filter; default values are set on the dashboard.
- Variables in the SQL must match exactly the variables connected on the dashboard. Default and Required are set at card level only for Q3 CARD 2, Q1 CARD 3, Q7 CARD 1 and CARD 2, with the same values as the dashboard.
- Q1 CARD 3 does not follow Listing type; Q3 CARD 1 and Q4 CARD 3 do not follow Property type.
- Cards with a Field Filter do not alias `analytics.vw_listing_latest`.

## 5. Results (produced from this project's data)

### 5.1 Gold and the three views (snapshot 2026-10-04, after the second pipeline run)

| Item | Value |
|---|---|
| Gold rows | 251,418 |
| Listings (= `vw_listing_latest` rows) | 245,000 (dataset 226,929, web 18,071) |
| `vw_listing_versions` rows | 247,068 (= Gold − 4,350) |
| Duplicate-hash versions (R8) | 4,350, all from the web source |
| Latest version that is not `is_current` | 4,850 |
| Rows with `valid_to < valid_from` | 4,850 |
| `row_hash` recomputed vs Silver | 0 mismatches over 247,068 rows |

### 5.2 Flags and filter rules (measured 2026-10-12)

| Item | Value |
|---|---|
| Listings `usable` / `is_analysis_ready` | 244,760 / 244,353 (gap 407 = 323 by `exclude_reason` + 84 commercial buildings) |
| `exclude_reason` (on `usable`) | `sale_ppm2_lt_1m` 31; `rent_ppm2_ge_10m` 12; `rent_ppm2_lt_10k` 4; `area_suspect` 276 |
| R1–R7 (`usable` listings, latest version) | 31 / 2,234 / 1,575 / 12 / 104 / 4 / 282 |
| `region_group` (listings; snapshot 2026-10-03, not re-measured) | HCM 92.7%, BRVT 4.4%, BD 2.9% |

### 5.3 Crawl (2026-08-26 → 2026-10-04, 40 days)

| Item | Value |
|---|---|
| Days with no run / partial / full | 17 / 12 / 11 |
| Stop reasons of 318 runs | `time_box` 230 (72.33%), `proxy_exhausted` 70 (22.01%), `incomplete` 8, `recovered` 5, `fetch_error` 5 |
| Total new web versions in Gold (Q6 CARD 5) | 13,743 |
| Total detail pages fetched | 31,551 |
| Expired by source (latest version) | dataset 226,929 listings, 64,294 expired (28.33%); web 18,071 listings, 3 expired (0.02%) |

### 5.4 Per tab

- **Q1.** Unfiltered: sale 229,225 listings, median 132.61 million/m² (p25 89.98; p75 204.67); rent 15,128, median 213.11 thousand/m²/month (p25 136; p75 369.57). Cần bán + Nhà trong hẻm + hcm_old: n 120,046, median 119.63 (p25 88.08; p75 163.27); 21 districts, Quận 1 highest at 259.32 (n 2,821), Huyện Củ Chi lowest at 6.77. The 21 districts total n 120,035, exactly 11 below 120,046: all are Huyện Cần Giờ (n 11 < 30, hidden); no listing lacks a district.
- **Q2** (median premium of qualifying cells). hcm_old: 258 cells, 89 wards, 50.81%; by area band: <40 m² 72.19 (22 cells); 40–60 49.07 (67); 60–100 47.89 (83); ≥100 54.83 (86). Without the region filter: 280 cells, 101 wards, 50.1%. Top 5 cells: Khánh Hội ≥100 m² 347.33% (n 59 / 35; median 469.23 vs 104.90 million/m²; median area 160 m² vs 133 m²; no `ultra_prime`); Sài Gòn 40–60 285.69%; Sài Gòn ≥100 279.11%; Vĩnh Hội ≥100 236.06%; Xóm Chiếu 60–100 225.97%.
- **Q3** (median gross yield by district). hcm_old: apartment 17 districts 3.43%; frontage house 17 districts 1.69%; alley house 14 districts 1.99% (1.67–2.86). Without the region filter: apartment 19 districts 3.23% (2.34–4.87); frontage house 18 districts 1.68% (1.07–2.31); alley house 15 districts 1.95% (1.61–2.86).
- **Q4** (median discount vs the 40–60 m² band; <40 / 60–100 / ≥100). All property types: +9.7 / −5.8 / −13.0 (37 / 59 / 51 cells). Alley house: +11.33 / −10.34 / −23.25 (20 / 23 / 23 cells). Frontage house: +1.29 / −3.27 / −9.4 (17 / 20 / 20). Villa −13.77 / −13.24 (4 / 4 cells); apartment −1.27 / +33.39 (12 / 4 cells): apartments ≥100 m² and villas have only 4 cells, so no conclusion is drawn.
- **Q6.** 17 days with no run, 12 days with partial runs, 4,350 duplicate-hash versions (run counts and stop reasons in 5.3).
- **Q7** (alley house, hcm_old, median million/m² by week). Dataset 06-29 → 07-27: 118.75; 123.41; 121.88; 120.31; 120.19; ±8% band around the dataset series: 109.25–133.28. Web 08-24 → 09-07: 119.23; 117.00; 131.91 (n 1,335 / 1,275 / 573), within the band; the +12.7% jump in the 09-07 week belongs to a week flagged for missing crawl days. Web week 08-31 (all property types): median down 11.8% (126.32 → 111.40) because the old-HCM share fell from 98.1% to 67.1%, not because of price; filtering alley house + hcm_old leaves only −1.9%.

### 5.5 Q5: price changes of listings the web revisited

| Item | Value |
|---|---|
| Dataset → web pairs | 2,068, all with both prices > 0 (no pair has a zero price); 242,932 listings have 1 version, 2,068 listings have 2 versions |
| Price-change / price-drop pairs | 996 / 842 (84.54% drops) |
| Percentiles of % price change (996 pairs) | min −100; p01 −97.8; p25 −9.09; median −5.06; p75 −2.11; p99 +119.5; max +19,323.1 |
| Gap between the two observations | 14 / 38 / 98 days (min / median / max) |
| By listing type (pairs / price changes / drops) | Cần bán (sale) 1,717 / 907 / 778 (85.78%; p25 / median / p75 = −8.40 / −4.88 / −2.17); Cho thuê (rent) 351 / 89 / 64 (71.91%; −20.00 / −10.00 / +6.67) |
| Tails (996 price-change pairs) | 24 pairs drop by 50% or more (17 by 90% or more); 24 pairs rise by 50% or more (3 by 10x or more); 48 pairs (4.8%) outside ±50% in total |
| Manual check of the 15 largest drops | 4 pairs changed listing type sale ↔ rent; 10 pairs fixed a unit label ("X billion / m²" entered as a total price); 1 pair typo |
| Suspect pairs caught automatically | 13 pairs (1.3%): unit fix 10, typo (extra 0) 3 |
| Sensitivity (drop suspect pairs and pairs with no derivable area, 982 left) | median −5.1 → −5.0; drop share 84.5 → 84.6%; conclusion unchanged, so no outlier trimming |
| Body of the distribution | 773 pairs (77.6%) drop 2–20% |
| By gap (pairs / price changes / median %) | <30 days 681 / 276 / −4.83; 30–59 days 1,047 / 534 / −5.10; 60–89 days 337 / 183 / −5.26; 90 days or more 3 / 3 (hidden) |

## 6. Known data issues

### 6.1 SCD2 merge inverted the version order (Silver)

A staging row older than every existing row always has a NULL `prev_hash`, so it always becomes a new version; `is_latest` is computed only within the set of change points, so the older row becomes `is_current` while the newer row is closed with `valid_to` < `valid_from`. Trigger: DAG 3 processed the first web file (2026-08-29 17:58) before the first dataset file (20:47). Result: 4,850 listings whose `is_current` is not the latest version (4,828 old dataset versions, 22 old web versions) and 4,350 duplicate-hash versions. Handled in the analytics layer: latest version by `MAX(valid_from)`, duplicate-hash versions dropped. The pipeline is not modified. Possible later fix: recompute `valid_to` / `is_current` over the full history for the affected listings, compare the hash with the following row as well, process Bronze files by `crawl_date`, run a one-off fix script, then rerun Silver → Gold.

### 6.2 Price

Posters choose the wrong unit (the parser copies the value as-is) and some listings are labeled sale/rent wrongly. Handled by R1, R4–R6.

### 6.3 Area

The site drops the decimal point when a poster enters a decimal area (the original HTML already reads "6.065" for 60.65 m²); the parser copies it correctly, so the parser is not changed. Handled by R7.

### 6.4 Crawler

The system is deliberately switched off when data is not needed (learning project); this is not a bug. The crawler does not backfill, so listings posted while it was off are mostly lost. Handled by the `vw_crawl_coverage` mask; no trend conclusions.

## 7. Limitations

1. Asking prices, not transaction prices.
2. The two sources differ in coverage, property-type mix and time span; the web starts on 2026-08-26; the 08-14 → 08-25 gap and the days the system was off have no data. `valid_from` is the crawl date, not the Gold load date.
3. The web does not recrawl old URLs: web `is_expired` is almost always `FALSE` (3 of 18,071 listings) and web prices are not updated after the first crawl; expiry rates are not compared between sources. Q1–Q4 include expired listings (28.3% of dataset listings), not separated.
4. `posted_date` is most likely the latest posting or refresh date: the web's "new listing" count is inflated and weekly counts reflect the crawl schedule. Q7: dataset listings revisited by the web move to the web source with a new `posted_date`, so read Q7 as a stability indicator, not a market trend.
5. Price-unit errors in the middle of the distribution are not caught by R1–R7. Dedup is by URL only: the same house posted twice under two URLs still counts as two listings (frequency not measured).
6. `area_m2` is rounded down to an integer; for multi-storey houses it is often total floor area. R7 only catches errors when both length and width exist, so errors in listings without dimensions pass through; multi-storey buildings may be flagged wrongly.
7. A hung run followed by `recovered` / `incomplete` makes `run_count` higher than reality (2026-08-31: 21 runs but only 17 with data); the view is not changed.
8. Q2: the cell is `ward_new`, so the premium mixes frontage location with house type; `ward_new` includes wards originally from Bình Dương and Bà Rịa - Vũng Tàu, so use the Region filter (default hcm_old). The Khánh Hội ≥100 m² cell has only n = 35 in the alley group.
9. Q3: the rent sample for townhouses is thin, so no yield conclusion for townhouses. Q4: the all-types view is dominated by alley houses; use CARD 3.
10. Q5: the sample contains only dataset → web pairs with different hashes (listings the web revisited, mostly refreshed or renewed); it does not represent the market and cannot measure the share of listings that change price. "Price change" is the difference between the price when the dataset saw it and when the web revisited it (14–98 days), with no knowledge of when it changed. Pairs that changed listing type cannot be separated yet (the hash excludes listing type). The extreme tail (48 of 996 pairs) comes from unit-label fixes, listing-type changes and typos. The body of 773 pairs dropping 2–20% has no established cause (real price adjustment or listing refresh), so "price changes come from fixing errors" is not concluded. The rent tab (89 pairs, 20.2% in the tail) should be read with caution. The "/ m²" label error in dataset-only listings has not been measured.
11. SCD2 merge not fixed (section 6.1).
