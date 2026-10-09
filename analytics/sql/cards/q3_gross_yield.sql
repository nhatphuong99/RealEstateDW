-- analytics/sql/cards/q3_gross_yield.sql
-- Tab Q3 "Gross yield theo quận". Gross yield = 12 × median giá/m² thuê tháng ÷ median giá/m² bán (docs/metrics.md mục 3).
-- Ô = (region_group, district_old, property_type_name); chỉ giữ ô mà CẢ bán và thuê có n >= 30.
-- Chỉ 3 loại hình: Căn hộ chung cư, Nhà mặt tiền, Nhà trong hẻm. Ước tính khu vực, KHÔNG phải cùng một tài sản.
-- Cả hai vế cùng đơn vị VND/m², nên tỷ số không cần quy đổi; cột hiển thị bán = triệu VND/m², thuê = triệu VND/m²/năm.
-- Field Filter (Dropdown), map vào analytics.vw_listing_latest:
--   {{loai_hinh}} -> property_type_name, {{nguon}} -> source_name, {{vung}} -> region_group
-- KHÔNG đặt alias cho analytics.vw_listing_latest. Card nào cũng lặp lại CTE `cell`.
-- Chạy thử trên DataGrip: xóa các dòng [[AND {{...}}]] hoặc thay bằng điều kiện thật.

-- CARD 1: "Tổng quan theo loại hình" (Table 3 dòng; KHÔNG có biến {{loai_hinh}}, không nối bộ lọc Loại BĐS): median yield của các quận đạt n >= 30 ở cả hai vế
WITH cell AS (
    SELECT
        region_group,
        district_old,
        property_type_name,
        COUNT(*) FILTER (WHERE deal_type = 'sale') AS n_ban,
        COUNT(*) FILTER (WHERE deal_type = 'rent') AS n_thue,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE deal_type = 'sale') AS median_ban_vnd,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE deal_type = 'rent') AS median_thue_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND property_type_name IN ('Căn hộ chung cư', 'Nhà mặt tiền', 'Nhà trong hẻm')
      AND district_old IS NOT NULL
      [[AND {{nguon}}]]
      [[AND {{vung}}]]
    GROUP BY region_group, district_old, property_type_name
),
qualified AS (
    SELECT *, 12 * median_thue_vnd / median_ban_vnd AS gross_yield
    FROM cell
    WHERE n_ban >= 30 AND n_thue >= 30
)
SELECT
    property_type_name AS loai_hinh,
    COUNT(*) AS so_quan_dat,
    ROUND((100 * percentile_cont(0.5) WITHIN GROUP (ORDER BY gross_yield))::numeric, 2) AS yield_median_pct,
    ROUND((100 * MIN(gross_yield))::numeric, 2) AS yield_min_pct,
    ROUND((100 * MAX(gross_yield))::numeric, 2) AS yield_max_pct
FROM qualified
GROUP BY property_type_name
ORDER BY property_type_name;

-- CARD 2: "Gross yield theo quận" (Bar ngang, xếp giảm dần; nên đặt {{loai_hinh}} Required, mặc định "Căn hộ chung cư")
WITH cell AS (
    SELECT
        region_group,
        district_old,
        property_type_name,
        COUNT(*) FILTER (WHERE deal_type = 'sale') AS n_ban,
        COUNT(*) FILTER (WHERE deal_type = 'rent') AS n_thue,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE deal_type = 'sale') AS median_ban_vnd,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE deal_type = 'rent') AS median_thue_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND property_type_name IN ('Căn hộ chung cư', 'Nhà mặt tiền', 'Nhà trong hẻm')
      AND district_old IS NOT NULL
      [[AND {{loai_hinh}}]]
      [[AND {{nguon}}]]
      [[AND {{vung}}]]
    GROUP BY region_group, district_old, property_type_name
)
SELECT
    district_old AS quan_huyen,
    ROUND((100 * 12 * median_thue_vnd / median_ban_vnd)::numeric, 2) AS gross_yield_pct
FROM cell
WHERE n_ban >= 30 AND n_thue >= 30
ORDER BY 12 * median_thue_vnd / median_ban_vnd DESC;

-- CARD 3: "Bảng quận" (Table: median bán, median thuê × 12, yield, n bán, n thuê)
WITH cell AS (
    SELECT
        region_group,
        district_old,
        property_type_name,
        COUNT(*) FILTER (WHERE deal_type = 'sale') AS n_ban,
        COUNT(*) FILTER (WHERE deal_type = 'rent') AS n_thue,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE deal_type = 'sale') AS median_ban_vnd,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE deal_type = 'rent') AS median_thue_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND property_type_name IN ('Căn hộ chung cư', 'Nhà mặt tiền', 'Nhà trong hẻm')
      AND district_old IS NOT NULL
      [[AND {{loai_hinh}}]]
      [[AND {{nguon}}]]
      [[AND {{vung}}]]
    GROUP BY region_group, district_old, property_type_name
)
SELECT
    property_type_name AS loai_hinh,
    district_old AS quan_huyen,
    ROUND((median_ban_vnd / 1e6)::numeric, 2) AS median_ban_trieu_m2,
    ROUND((12 * median_thue_vnd / 1e6)::numeric, 2) AS median_thue_x12_trieu_m2_nam,
    ROUND((100 * 12 * median_thue_vnd / median_ban_vnd)::numeric, 2) AS gross_yield_pct,
    n_ban,
    n_thue
FROM cell
WHERE n_ban >= 30 AND n_thue >= 30
ORDER BY property_type_name, 12 * median_thue_vnd / median_ban_vnd DESC;
