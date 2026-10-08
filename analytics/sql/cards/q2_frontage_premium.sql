-- analytics/sql/cards/q2_frontage_premium.sql
-- Tab Q2 "Mặt tiền đắt hơn hẻm bao nhiêu % cùng phường". Chỉ tin bán (docs/metrics.md mục 3).
-- Đơn vị phân tích = ô (ward_new, area_band): premium = median(Nhà mặt tiền) / median(Nhà trong hẻm) - 1.
-- Chỉ giữ ô mà CẢ HAI nhóm có n >= 30. Chỉ số tổng hợp = median của premium các ô đạt (không trọng số).
-- Field Filter (Dropdown), map vào analytics.vw_listing_latest:
--   {{vung}} -> region_group, {{nhom_dien_tich}} -> area_band
-- KHÔNG đặt alias cho analytics.vw_listing_latest. Card nào cũng lặp lại CTE `cell` (Metabase không chia sẻ CTE).
-- Chạy thử trên DataGrip: xóa các dòng [[AND {{...}}]] hoặc thay bằng điều kiện thật.

-- CARD 1: "Premium median" (Number, định dạng %)
WITH cell AS (
    SELECT
        ward_new,
        area_band,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS n_mt,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS n_hem,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS median_mt_vnd,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS median_hem_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND deal_type = 'sale'
      AND property_type_name IN ('Nhà mặt tiền', 'Nhà trong hẻm')
      AND ward_new IS NOT NULL
      AND area_band IS NOT NULL
      [[AND {{vung}}]]
      [[AND {{nhom_dien_tich}}]]
    GROUP BY ward_new, area_band
),
qualified AS (
    SELECT *, median_mt_vnd / median_hem_vnd - 1 AS premium
    FROM cell
    WHERE n_mt >= 30 AND n_hem >= 30
)
SELECT ROUND((100 * percentile_cont(0.5) WITHIN GROUP (ORDER BY premium))::numeric, 1) AS premium_median_pct
FROM qualified;

-- CARD 2: "Số ô (phường × nhóm diện tích) đạt n >= 30 ở cả hai nhóm" (Table một dòng)
WITH cell AS (
    SELECT
        ward_new,
        area_band,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS n_mt,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS n_hem
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND deal_type = 'sale'
      AND property_type_name IN ('Nhà mặt tiền', 'Nhà trong hẻm')
      AND ward_new IS NOT NULL
      AND area_band IS NOT NULL
      [[AND {{vung}}]]
      [[AND {{nhom_dien_tich}}]]
    GROUP BY ward_new, area_band
)
SELECT
    COUNT(*) AS so_o_dat,
    COUNT(DISTINCT ward_new) AS so_phuong_dat
FROM cell
WHERE n_mt >= 30 AND n_hem >= 30;

-- CARD 3: "Premium cao nhất" (Table một dòng: phường, nhóm diện tích, premium %)
WITH cell AS (
    SELECT
        ward_new,
        area_band,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS n_mt,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS n_hem,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS median_mt_vnd,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS median_hem_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND deal_type = 'sale'
      AND property_type_name IN ('Nhà mặt tiền', 'Nhà trong hẻm')
      AND ward_new IS NOT NULL
      AND area_band IS NOT NULL
      [[AND {{vung}}]]
      [[AND {{nhom_dien_tich}}]]
    GROUP BY ward_new, area_band
)
SELECT
    ward_new AS phuong,
    CASE area_band
        WHEN '1_lt_40' THEN 'Dưới 40 m²' WHEN '2_40_60' THEN '40 – 60 m²'
        WHEN '3_60_100' THEN '60 – 100 m²' WHEN '4_ge_100' THEN 'Từ 100 m² trở lên'
    END AS nhom_dien_tich,
    n_mt,
    n_hem,
    ROUND((100 * (median_mt_vnd / median_hem_vnd - 1))::numeric, 1) AS premium_pct
FROM cell
WHERE n_mt >= 30 AND n_hem >= 30
ORDER BY median_mt_vnd / median_hem_vnd DESC
LIMIT 1;

-- CARD 4: "Premium theo nhóm diện tích" (Bar; trục X = nhom_dien_tich)
-- Thay cho "cột theo quận" của wireframe: ward_new (phường mới) không lồng trong district_old (quận cũ),
-- nên gom theo quận sẽ phải chọn quận cho từng phường. Gom theo nhóm diện tích thì luôn rõ nghĩa.
WITH cell AS (
    SELECT
        ward_new,
        area_band,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS n_mt,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS n_hem,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS median_mt_vnd,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS median_hem_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND deal_type = 'sale'
      AND property_type_name IN ('Nhà mặt tiền', 'Nhà trong hẻm')
      AND ward_new IS NOT NULL
      AND area_band IS NOT NULL
      [[AND {{vung}}]]
      [[AND {{nhom_dien_tich}}]]
    GROUP BY ward_new, area_band
),
qualified AS (
    SELECT *, median_mt_vnd / median_hem_vnd - 1 AS premium
    FROM cell
    WHERE n_mt >= 30 AND n_hem >= 30
)
SELECT
    CASE area_band
        WHEN '1_lt_40' THEN 'Dưới 40 m²' WHEN '2_40_60' THEN '40 – 60 m²'
        WHEN '3_60_100' THEN '60 – 100 m²' WHEN '4_ge_100' THEN 'Từ 100 m² trở lên'
    END AS nhom_dien_tich,
    COUNT(*) AS so_phuong,
    ROUND((100 * percentile_cont(0.5) WITHIN GROUP (ORDER BY premium))::numeric, 1) AS premium_median_pct
FROM qualified
GROUP BY area_band
ORDER BY area_band;

-- CARD 5: "Bảng phường × nhóm diện tích" (Table, xếp giảm dần theo premium)
WITH cell AS (
    SELECT
        ward_new,
        area_band,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS n_mt,
        COUNT(*) FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS n_hem,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE property_type_name = 'Nhà mặt tiền') AS median_mt_vnd,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)
            FILTER (WHERE property_type_name = 'Nhà trong hẻm') AS median_hem_vnd
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND deal_type = 'sale'
      AND property_type_name IN ('Nhà mặt tiền', 'Nhà trong hẻm')
      AND ward_new IS NOT NULL
      AND area_band IS NOT NULL
      [[AND {{vung}}]]
      [[AND {{nhom_dien_tich}}]]
    GROUP BY ward_new, area_band
)
SELECT
    ward_new AS phuong,
    CASE area_band
        WHEN '1_lt_40' THEN 'Dưới 40 m²' WHEN '2_40_60' THEN '40 – 60 m²'
        WHEN '3_60_100' THEN '60 – 100 m²' WHEN '4_ge_100' THEN 'Từ 100 m² trở lên'
    END AS nhom_dien_tich,
    ROUND((median_mt_vnd / 1e6)::numeric, 2) AS median_mat_tien_trieu_m2,
    ROUND((median_hem_vnd / 1e6)::numeric, 2) AS median_hem_trieu_m2,
    ROUND((100 * (median_mt_vnd / median_hem_vnd - 1))::numeric, 1) AS premium_pct,
    n_mt,
    n_hem
FROM cell
WHERE n_mt >= 30 AND n_hem >= 30
ORDER BY median_mt_vnd / median_hem_vnd DESC;
