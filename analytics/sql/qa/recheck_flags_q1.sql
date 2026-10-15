-- analytics/sql/qa/recheck_flags_q1.sql
-- Kiểm tra lại hai điểm thắc mắc sau QA card: (1) số R1-R7 và is_analysis_ready, (2) tổng n của các quận ở Q1 CARD 2.
-- Chỉ đọc analytics.vw_listing_latest. Chạy từng khối trong DataGrip (bôi đen khối, Ctrl+Enter).

-- KHỐI A: đếm lại R1-R7 trên bản mới nhất (cơ sở is_usable, giống QA D4a). So với snapshot 2026-10-03 ở docs/metrics.md mục 9.2.
-- Quy tắc chấp nhận: Gold tăng 251.183 -> 251.418 (+235 dòng) nên mỗi số chỉ được lệch tối đa 235.
SELECT
    COUNT(*) AS so_listing,
    COUNT(*) FILTER (WHERE is_usable) AS usable,
    COUNT(*) FILTER (WHERE is_analysis_ready) AS analysis_ready,
    COUNT(*) FILTER (WHERE is_usable AND exclude_reason = 'sale_ppm2_lt_1m') AS r1_loai,
    COUNT(*) FILTER (WHERE is_usable AND deal_type = 'sale' AND price_per_m2_vnd >= 1e6 AND price_per_m2_vnd < 1e7) AS r2_giu_1_den_10_trieu,
    COUNT(*) FILTER (WHERE is_usable AND is_ultra_prime) AS r3_ultra_prime,
    COUNT(*) FILTER (WHERE is_usable AND exclude_reason = 'rent_ppm2_ge_10m') AS r4_loai,
    COUNT(*) FILTER (WHERE is_usable AND is_commercial_building) AS r5_tong_nha_thuong_mai,
    COUNT(*) FILTER (WHERE is_usable AND exclude_reason = 'rent_ppm2_lt_10k') AS r6_loai,
    COUNT(*) FILTER (WHERE area_suspect) AS r7_gan_co,
    COUNT(*) FILTER (WHERE is_usable AND exclude_reason = 'area_suspect') AS r7_bi_loai
FROM analytics.vw_listing_latest;

-- KHỐI B: đẳng thức nội bộ. Kỳ vọng chenh = loai_theo_ly_do + tm_khong_bi_loai, và sai_lech = 0.
SELECT
    COUNT(*) FILTER (WHERE is_usable) - COUNT(*) FILTER (WHERE is_analysis_ready) AS chenh,
    COUNT(*) FILTER (WHERE is_usable AND exclude_reason IS NOT NULL) AS loai_theo_ly_do,
    COUNT(*) FILTER (WHERE is_usable AND exclude_reason IS NULL AND is_commercial_building) AS tm_khong_bi_loai,
    (COUNT(*) FILTER (WHERE is_usable) - COUNT(*) FILTER (WHERE is_analysis_ready))
      - COUNT(*) FILTER (WHERE is_usable AND exclude_reason IS NOT NULL)
      - COUNT(*) FILTER (WHERE is_usable AND exclude_reason IS NULL AND is_commercial_building) AS sai_lech
FROM analytics.vw_listing_latest;

-- KHỐI C: tổng n các quận ở Q1 CARD 2 (Cần bán + Nhà trong hẻm + hcm_old, đúng bộ lọc mặc định).
-- Kỳ vọng: so_quan_hien = 21, tong_n_hien = 120035, tong_n = 120046; phần chênh 11 nằm ở tong_n_an + n_khong_quan.
WITH g AS (
    SELECT district_old, COUNT(*) AS n
    FROM analytics.vw_listing_latest
    WHERE is_analysis_ready
      AND listing_type = 'Cần bán'
      AND property_type_name = 'Nhà trong hẻm'
      AND region_group = 'hcm_old'
    GROUP BY district_old
)
SELECT
    COUNT(*) FILTER (WHERE n >= 30) AS so_quan_hien,
    SUM(n) FILTER (WHERE n >= 30) AS tong_n_hien,
    COUNT(*) FILTER (WHERE n < 30 AND district_old IS NOT NULL) AS so_quan_an,
    COALESCE(SUM(n) FILTER (WHERE n < 30 AND district_old IS NOT NULL), 0) AS tong_n_an,
    COALESCE(SUM(n) FILTER (WHERE district_old IS NULL), 0) AS n_khong_quan,
    SUM(n) AS tong_n
FROM g;

-- KHỐI D: liệt kê 8 nhóm quận nhỏ nhất (cùng bộ lọc) để thấy quận nào bị ẩn và dòng không có quận (district_old rỗng).
SELECT district_old, COUNT(*) AS n, COUNT(*) >= 30 AS hien_thi
FROM analytics.vw_listing_latest
WHERE is_analysis_ready
  AND listing_type = 'Cần bán'
  AND property_type_name = 'Nhà trong hẻm'
  AND region_group = 'hcm_old'
GROUP BY district_old
ORDER BY n
LIMIT 8;
