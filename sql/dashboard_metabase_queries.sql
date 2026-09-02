-- ============================================================================
-- sql/dashboard_metabase_queries.sql
-- Tổng hợp SQL cho dashboard Metabase (BĐS TP.HCM — 3 tab, 13 card).
-- Nguồn: gold.vw_fact_report (xem 006_gold_reporting_view.sql).
-- CHỈ để tham khảo/copy tay vào từng Question trong Metabase SQL editor —
-- KHÔNG chạy bằng psql, KHÔNG thuộc ETL DAG nào.
--
-- Field Filter (giữ nguyên tên xuyên suốt):
--   {{ngay_dang}}       -> posted_date        -> Date range
--   {{loai_tin}}        -> listing_type       -> Dropdown
--   {{loai_hinh_bds}}   -> property_type_name -> Dropdown
--   {{khu_vuc_phuong}}  -> ward_new_map_key   -> Dropdown
--   {{khu_vuc_quan}}    -> district_old_map_key -> Dropdown
--
-- Bộ lọc chuẩn (mọi card tính giá):
--   price_is_negotiable = FALSE, price_is_outlier = FALSE,
--   area_is_outlier = FALSE, area_is_undetermined = FALSE
-- is_current = TRUE bắt buộc ở MỌI card — tránh đếm trùng version SCD2.
-- Làm tròn ROUND(..., 2) cho mọi giá trị tiền tệ/diện tích.
-- ============================================================================


-- ============================================================================
-- TAB 1 — TỔNG QUAN
-- ============================================================================

-- Card 1 — Tổng số tin đang theo dõi (Number)
SELECT COUNT(*) AS tong_so_tin
FROM gold.vw_fact_report
WHERE is_current = TRUE
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
;

-- Card 2 — Giá trung bình/m² toàn TP.HCM (Number, suffix " triệu/m²")
SELECT ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
;

-- Card 3 — Giá trung vị/m² toàn TP.HCM (Number, suffix " triệu/m²")
SELECT ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)::NUMERIC / 1000000.0, 2) AS gia_trung_vi_trieu_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
;

-- Card 4 — Bản đồ giá TB/m² theo Phường/Xã (Map -> Region Map -> HCMC_Ward_New_Map)
-- Region field = ward_new_map_key | Metric field = gia_tb_trieu_m2
SELECT ward_new_map_key,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2,
       COUNT(*) AS so_tin
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
  AND ward_new_map_key <> ''
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
GROUP BY ward_new_map_key
HAVING COUNT(*) >= 5
ORDER BY gia_tb_trieu_m2 DESC
;

-- Card 5 — Bản đồ giá TB/m² theo Quận/Huyện (Map -> Region Map -> HCMC_District_Old_Map)
-- Region field = district_old_map_key | Metric field = gia_tb_trieu_m2
SELECT district_old_map_key,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2,
       COUNT(*) AS so_tin
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
  AND district_old_map_key <> ''
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
GROUP BY district_old_map_key
HAVING COUNT(*) >= 5
ORDER BY gia_tb_trieu_m2 DESC
;


-- ============================================================================
-- TAB 2 — THEO KHU VỰC
-- ============================================================================

-- Card 6 — Top 10 Phường/Xã giá cao nhất (Table)
SELECT ward_new_map_key AS phuong_xa,
       COUNT(*) AS so_tin,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)::NUMERIC / 1000000.0, 2) AS gia_trung_vi_trieu_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
  AND ward_new_map_key <> ''
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_phuong}} ]]
GROUP BY ward_new_map_key
HAVING COUNT(*) >= 5
ORDER BY gia_tb_trieu_m2 DESC
LIMIT 10
;

-- Card 7 — Top 10 Phường/Xã giá thấp nhất (Table)
SELECT ward_new_map_key AS phuong_xa,
       COUNT(*) AS so_tin,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)::NUMERIC / 1000000.0, 2) AS gia_trung_vi_trieu_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
  AND ward_new_map_key <> ''
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_phuong}} ]]
GROUP BY ward_new_map_key
HAVING COUNT(*) >= 5
ORDER BY gia_tb_trieu_m2 ASC
LIMIT 10
;

-- Card 8 — Bảng tổng hợp theo Quận/Huyện cũ (Table)
SELECT district_old_map_key AS quan_huyen,
       COUNT(*) AS so_tin,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2_vnd)::NUMERIC / 1000000.0, 2) AS gia_trung_vi_trieu_m2,
       ROUND(AVG(area_m2), 2) AS dien_tich_tb_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
  AND district_old_map_key <> ''
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_quan}} ]]
GROUP BY district_old_map_key
ORDER BY gia_tb_trieu_m2 DESC
;


-- ============================================================================
-- TAB 3 — XU HƯỚNG & PHÂN BỐ
-- ============================================================================

-- Card 9 — Xu hướng giá TB/m² theo tháng, theo loại hình BĐS (Line chart)
SELECT DATE_TRUNC('month', posted_date)::date AS thang,
       property_type_name,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_quan}} ]]
GROUP BY 1, 2
HAVING COUNT(*) >= 5
ORDER BY 1
;

-- Card 10 — Số lượng tin quan sát theo tháng (Bar chart)
SELECT DATE_TRUNC('month', posted_date)::date AS thang,
       COUNT(*) AS so_tin
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_quan}} ]]
GROUP BY 1
ORDER BY 1
;

-- Card 11 — Phân bố tin theo loại hình BĐS (Row chart)
-- Không lọc price_is_.../area_is_...: chỉ đếm số lượng, không tính giá trị giá/diện tích.
SELECT property_type_name,
       COUNT(*) AS so_tin
FROM gold.vw_fact_report
WHERE is_current = TRUE
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_quan}} ]]
GROUP BY property_type_name
ORDER BY so_tin DESC
;

-- Card 12 — Tỷ lệ Cần bán / Cho thuê (Donut chart)
SELECT listing_type,
       COUNT(*) AS so_tin
FROM gold.vw_fact_report
WHERE is_current = TRUE
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_quan}} ]]
GROUP BY listing_type
;

-- Card 13 — Giá TB/m² theo loại hình BĐS (Bar chart)
SELECT property_type_name,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
[[ AND {{ngay_dang}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_quan}} ]]
GROUP BY property_type_name
ORDER BY gia_tb_trieu_m2 DESC
;
