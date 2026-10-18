# analytics/docs/qa.md — Impact scope and check results

> Numeric results: `metrics.md` section 5. Check SQL: folder `qa/`.

## 1. Impact scope

| Item | Impact |
|---|---|
| Gold (`fact_listing_price`, `dim_*`, `vw_fact_report`), Silver, Bronze | Unchanged; the new views only `SELECT` |
| `pipeline.run_state` | Read-only (one `SELECT` in `vw_crawl_coverage`) |
| 4 Airflow DAGs | Unchanged; `analytics/` is outside the folders mounted into the Airflow containers |
| 13 existing Metabase cards | Unchanged; they read `gold.*`, not `analytics.*` |
| New objects | Schema `analytics` + 3 views (`CREATE OR REPLACE`, safe to re-run) |
| Rollback | `DROP SCHEMA analytics CASCADE;` removes only objects in `analytics`; Gold does not depend on it |

**Reverse-dependency risk:** the `analytics` views depend on `gold.vw_fact_report`. `DROP VIEW gold.vw_fact_report` without `CASCADE` fails because dependent views exist; with `CASCADE` the analytics views are dropped silently. After every Gold rebuild, rerun `sql/01_create_base_views.sql` and check that all 3 views exist:

```sql
SELECT table_name FROM information_schema.views WHERE table_schema = 'analytics' ORDER BY 1;
```

Scan the source for commands that could touch the views (PowerShell, from the project root):

```powershell
Get-ChildItem dags, parser, crawler, sql, scripts -Recurse -Include *.py,*.sql,*.sh |
  Select-String -Pattern 'DROP\s+(VIEW|TABLE|SCHEMA)|CASCADE|vw_fact_report'
```

Result: no command touches Gold objects; the 3 views remained intact after running DAG 2 → 3 → 4.

## 2. What was checked

| Item | Method | Result |
|---|---|---|
| View creation | Ran `01_create_base_views.sql` twice in a row | No error; exactly 3 views |
| Invariants | `02_invariants.sql` (4 queries) | 0 rows for each query |
| Reconciliation | `03_reconcile.sql` | Row and listing counts match Gold; 4,350 duplicate hashes and 4,850 / 4,850 match; R1, R4, R6, R7 match; `row_hash` matches Silver 100% |
| Manual spot-check | `04_spotcheck.sql`; opened the original URLs of 9 listings still online | Price and area match Gold; version order, hash, `price_change_pct`, `days_since_prev_version` correct; VN-time dates correct; the 4 `incomplete` runs of 2026-08-31 have `detail_pages_done` = 0 |
| Finding from the spot-check | Listings with length × width < 20 m² have fake dimensions and were wrongly flagged `area_suspect` | Added the condition length × width ≥ 20 m² to R7 (R7 = 282) |
| Cross-check card = SQL = pandas | `recompute_median.py`, `recompute_q2_cell.py` | 124 groups match, largest difference about 6e-8; Q1 Quận 1: 2,821 / 259.32; Q2 Khánh Hội ≥100 m²: 59 / 469.23 and 35 / 104.90 (premium 347.33%) |
| Re-measure R1–R7 after the second pipeline run | `recheck_flags_q1.sql` | Each number differs by at most 235 (extra Gold rows added); the identity 407 = 323 + 84 holds |
| 11-listing gap in Q1 | `recheck_flags_q1.sql` | All are Huyện Cần Giờ (n 11 < 30, hidden); 0 listings lack a district |
| Metabase: n < 30 and units | Inspected each card | No card shows a group with n < 30 (smallest n is 30); units correct; sale and rent not mixed |
| Metabase: speed | 24 cards of Q1–Q4, Q6, Q7 and 5 cards of Q5 with default filters | All under 3 seconds, slowest 1.9 seconds |
| 13 existing cards | Compared with the baseline (8 of 13 cards remain) | Listing count 243,788 → 245,000 (+1,212), no number decreased; the difference is Gold growth after the pipeline runs |
| Q5 | `q5_sizing.sql`, `q5_qa.sql`, manual check of the 15 largest drops | 2,068 / 996 / 842 match; 242,932 + 2,068 = 245,000; tail caused by unit-label fixes, listing-type changes, typos (`metrics.md` section 5.5); sensitivity −5.1 → −5.0 |
