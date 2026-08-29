-- ============================================================================
-- sql/queries/validate_gold_load.sql
-- Validate sau khi chạy etl_silver_to_gold.sql.
--
-- Trả về 1 dòng/check (check_name, expected, actual, passed). Caller
-- (parser/silver_to_gold_io.py::validate_gold_load()) đọc toàn bộ dòng,
-- raise RuntimeError liệt kê check nào passed=FALSE -- Airflow đánh dấu
-- task fail thay vì âm thầm pass khi dữ liệu Gold sai lệch.
--
-- Nếu check_row_count_match FAIL (đặc biệt actual=0): chạy
-- sql/queries/diagnose_gold_join_loss.sql để biết chính xác JOIN nào (hay
-- schema-drift nào) đang làm rớt dòng, thay vì đoán.
-- ============================================================================

WITH check_row_count AS (
    -- COUNT(fact) phải khớp COUNT(silver) tuyệt đối -- nếu Fact JOIN bị
    -- rớt dòng nào (VD do dim_source trả NULL vì source_bronze_key không
    -- khớp prefix nào, hoặc cả transaction bị abort giữa chừng khiến 0
    -- dòng được insert) sẽ lộ ra ngay ở đây.
    SELECT
        'row_count_match' AS check_name,
        (SELECT COUNT(*) FROM silver.listing_history)::TEXT AS expected,
        (SELECT COUNT(*) FROM gold.fact_listing_price)::TEXT AS actual,
        (SELECT COUNT(*) FROM silver.listing_history)
            = (SELECT COUNT(*) FROM gold.fact_listing_price) AS passed
),
check_current_uniqueness AS (
    -- Đúng 1 dòng is_current=TRUE cho mỗi listing_id, đúng ý nghĩa "chưa
    -- từng phát hiện version mới hơn thay thế" -- quan trọng vì Fact dùng
    -- UPSERT, dễ sót logic nếu ETL chạy lại nhiều lần mà thiếu bước đóng
    -- version cũ.
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
check_reconfirmation_visibility AS (
    -- Thông tin (KHÔNG phải lỗi) — tỉ lệ dòng có is_reconfirmed=TRUE
    -- (từng được crawl lại ≥2 lần), phản ánh trực tiếp giới hạn độ phủ
    -- crawler (mỗi URL chỉ enqueue 1 lần qua UNIQUE(url)). Luôn passed=TRUE,
    -- chỉ để log số liệu ra output cho báo cáo/dashboard, KHÔNG làm fail task.
    SELECT
        'reconfirmed_ratio_info' AS check_name,
        'n/a (informational)' AS expected,
        ROUND(100.0 * COUNT(*) FILTER (WHERE is_reconfirmed) / NULLIF(COUNT(*), 0), 2)::TEXT || '%' AS actual,
        TRUE AS passed
    FROM gold.fact_listing_price
),
check_fk_not_null AS (
    -- Cột FK đã NOT NULL ở DDL nên về lý thuyết không thể NULL (insert
    -- sẽ tự fail sớm hơn) -- check này chỉ để có thông điệp rõ ràng thay
    -- vì lỗi constraint chung chung, hữu ích khi debug qua log.
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
       OR observed_date_key IS NULL
       OR posted_date_key IS NULL
),
check_price_per_m2_flagged AS (
    -- CHO PHÉP outlier tồn tại, MIỄN LÀ đã được đánh dấu đúng
    -- price_is_outlier=TRUE ở Silver (không null hóa price_vnd -- xem
    -- comment cột). Check này chỉ báo lỗi khi có dòng vượt ngưỡng NHƯNG
    -- CHƯA được flag -- nghĩa là parser bỏ sót, không phải dữ liệu xấu.
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
    -- Regression guard cho bug _sanitize_area() thiếu ngưỡng dưới: mọi
    -- area_m2 KHÔNG NULL và area_is_outlier=FALSE phải nằm trong [3, 10000]
    -- -- nếu có dòng lọt ra ngoài, nghĩa là _sanitize_area() đã bị sửa/
    -- hỏng lại, cần kiểm tra ngay.
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
SELECT * FROM check_reconfirmation_visibility
UNION ALL
SELECT * FROM check_fk_not_null
UNION ALL
SELECT * FROM check_price_per_m2_flagged
UNION ALL
SELECT * FROM check_area_within_sanitized_bounds;
