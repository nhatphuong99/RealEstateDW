-- analytics/sql/01_create_base_views.sql
-- Chạy (PowerShell, từ thư mục gốc project):
--   $env:PGCLIENTENCODING = 'UTF8'
--   psql "$env:POSTGRES_DW_DSN_LOCAL" -v ON_ERROR_STOP=1 -f analytics/sql/01_create_base_views.sql
-- Chạy lại nhiều lần được (CREATE OR REPLACE). Chỉ đọc gold.vw_fact_report và
-- pipeline.run_state; chỉ tạo object trong schema analytics.
--
-- Nếu đổi tên/thứ tự/kiểu cột của một view đã tạo: DROP VIEW analytics.<tên>; rồi chạy lại.
--
-- NGUỒN (đã đối chiếu với sql/schema_full.sql):
--   gold.vw_fact_report có đủ cột dùng ở đây, TRỪ row_hash (Gold không có).
--   row_hash được tính lại trong analytics với đúng công thức silver.compute_row_hash
--   (5 trường: price_vnd, price_is_negotiable, is_expired, has_warning, area_m2)
--   để không đọc Silver; QA đối chiếu mẫu với silver.listing_history.row_hash.
--   pipeline.run_state: run_id, started_at (timestamptz), ended_at, stopped_reason,
--   detail_pages_done (chỉ chứa lần chạy DAG 2).

CREATE SCHEMA IF NOT EXISTS analytics;

-- ---------------------------------------------------------------------------
-- 1. vw_listing_latest: mỗi listing một dòng (bản có MAX(valid_from)) + cờ/quy tắc
--    Không dùng is_current/valid_to vì merge SCD2 đảo thứ tự (00_discovery mục 3.1)
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
        -- Phân nhóm theo tỉnh cũ (province_new luôn là TP.HCM)
        CASE
            WHEN l.province_old ILIKE '%Bà Rịa%' THEN 'brvt_old'
            WHEN l.province_old ILIKE '%Bình Dương%' THEN 'binh_duong_old'
            ELSE 'hcm_old'
        END AS region_group,
        -- Tỷ lệ diện tích / (dài x ngang); chỉ tính khi kích thước hợp lệ
        CASE
            WHEN l.length_m > 0 AND l.width_m > 0
                THEN l.area_m2 / (l.length_m * l.width_m)
        END AS area_ratio,
        CASE
            WHEN l.area_m2 IS NULL THEN NULL
            WHEN l.area_m2 < 40 THEN '1_lt_40'
            WHEN l.area_m2 < 60 THEN '2_40_60'
            WHEN l.area_m2 < 100 THEN '3_60_100'
            ELSE '4_ge_100'
        END AS area_band,
        -- Định nghĩa usable của Day 1 (cờ Gold); NULL của cờ coi như FALSE
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
    -- Thứ tự ưu tiên: dòng đầu khớp thì dừng (metrics.md mục 4)
    CASE
        WHEN t.deal_type = 'sale' AND t.price_per_m2_vnd < 1000000 THEN 'sale_ppm2_lt_1m'
        WHEN t.deal_type = 'rent' AND t.price_per_m2_vnd >= 10000000 THEN 'rent_ppm2_ge_10m'
        WHEN t.deal_type = 'rent' AND t.price_per_m2_vnd < 10000 THEN 'rent_ppm2_lt_10k'
        WHEN t.area_suspect THEN 'area_suspect'
    END AS exclude_reason,
    -- Cột lọc duy nhất cho mọi chỉ số giá/m²
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
-- 2. vw_listing_versions: lịch sử version, loại bản trùng hash liên tiếp
--    Sắp xếp theo valid_from (không tin valid_to/is_current)
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
        -- Cùng công thức silver.compute_row_hash (đổi một bên phải đổi cả hai)
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
    -- Giữ version đầu tiên của mỗi chuỗi hash giống nhau
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
    (n.valid_from::date - n.prev_valid_from::date) AS days_since_prev_version,
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
-- 3. vw_crawl_coverage: số lần chạy theo ngày (kể cả ngày 0 lần) từ pipeline.run_state
--    Múi giờ Asia/Ho_Chi_Minh; dùng làm mặt nạ "ngày thiếu dữ liệu"
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.vw_crawl_coverage AS
WITH params AS (
    SELECT 20 AS full_day_min_runs   -- ngưỡng tạm (DAG chạy hằng giờ)
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
