-- ============================================================================
-- sql/dashboard_metabase_queries.sql
-- Tổng hợp toàn bộ SQL Question dùng để build dashboard Metabase
-- (BĐS TP.HCM - Tổng quan & Phân tích giá — 3 tab, 12 card).
--
-- Nguồn dữ liệu: gold.vw_fact_report (xem sql/006_gold_reporting_view.sql).
-- File này CHỈ để tham khảo/copy thủ công vào từng Question trong Metabase
-- SQL editor — KHÔNG chạy trực tiếp bằng psql, KHÔNG phải 1 phần của ETL DAG.
--
-- Quy ước biến (Field Filter) — GIỮ NGUYÊN tên xuyên suốt mọi card:
--   {{ngay_quan_sat}}  -> Field Filter -> observed_date        -> Date range
--   {{loai_tin}}       -> Field Filter -> listing_type          -> Dropdown list
--   {{loai_hinh_bds}}  -> Field Filter -> property_type_name    -> Dropdown list
--   {{khu_vuc_phuong}} -> Field Filter -> ward_new_map_key      -> Dropdown list (chỉ Tab 2)
--   {{khu_vuc_quan}}   -> Field Filter -> district_old_map_key  -> Dropdown list (chỉ Tab 2)
--
-- Quy ước lọc giá trị hợp lệ (bộ lọc chuẩn) — áp dụng mọi card tính giá:
--   price_is_negotiable = FALSE
--   price_is_outlier    = FALSE
--   area_is_outlier     = FALSE
--   area_is_undetermined = FALSE
--
-- Làm tròn: ROUND(..., 2) — 2 chữ số thập phân cho mọi giá trị tiền tệ/diện tích.
-- ============================================================================


-- ============================================================================
-- TAB 1 — TỔNG QUAN
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Card 1 — Tổng số tin đang theo dõi (Number)
-- Cố ý KHÔNG có {{ngay_quan_sat}}: luôn là tổng cố định, không phụ thuộc filter ngày.
-- ----------------------------------------------------------------------------
SELECT COUNT(*) AS tong_so_tin
FROM gold.vw_fact_report
WHERE is_current = TRUE
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
;

-- ----------------------------------------------------------------------------
-- Card 2 — Giá trung bình/m² toàn TP.HCM (Number, suffix " triệu/m²")
-- ----------------------------------------------------------------------------
SELECT ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
[[ AND {{ngay_quan_sat}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
;

-- ----------------------------------------------------------------------------
-- Card 6 — Bản đồ giá TB/m² theo Phường/Xã (địa giới mới)
-- Viz: Map -> Region Map -> HCMC_Ward_New_Map
--   Region field = ward_new_map_key | Metric field = gia_tb_trieu_m2
-- ----------------------------------------------------------------------------
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
[[ AND {{ngay_quan_sat}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
GROUP BY ward_new_map_key
HAVING COUNT(*) >= 5
ORDER BY gia_tb_trieu_m2 DESC
;

-- ----------------------------------------------------------------------------
-- Card 7 — Bản đồ giá TB/m² theo Quận/Huyện (địa giới cũ)
-- Viz: Map -> Region Map -> HCMC_District_Old_Map
--   Region field = district_old_map_key | Metric field = gia_tb_trieu_m2
-- ----------------------------------------------------------------------------
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
[[ AND {{ngay_quan_sat}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
GROUP BY district_old_map_key
HAVING COUNT(*) >= 5
ORDER BY gia_tb_trieu_m2 DESC
;


-- ============================================================================
-- TAB 2 — THEO KHU VỰC
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Card 8 — Top 10 Phường/Xã giá cao nhất (Table)
-- ----------------------------------------------------------------------------
SELECT ward_new_map_key AS phuong_xa,
       COUNT(*) AS so_tin,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
  AND ward_new_map_key <> ''
[[ AND {{ngay_quan_sat}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_phuong}} ]]
GROUP BY ward_new_map_key
HAVING COUNT(*) >= 5
ORDER BY gia_tb_trieu_m2 DESC
LIMIT 10
;

-- ----------------------------------------------------------------------------
-- Card 9 — Top 10 Phường/Xã giá thấp nhất (Table)
-- Chỉ khác Card 8 ở ORDER BY ASC — vẫn khai báo đủ 4 biến, không rút gọn.
-- ----------------------------------------------------------------------------
SELECT ward_new_map_key AS phuong_xa,
       COUNT(*) AS so_tin,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
  AND ward_new_map_key <> ''
[[ AND {{ngay_quan_sat}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_phuong}} ]]
GROUP BY ward_new_map_key
HAVING COUNT(*) >= 5
ORDER BY gia_tb_trieu_m2 ASC
LIMIT 10
;

-- ----------------------------------------------------------------------------
-- Card 10 — Bảng tổng hợp theo Quận/Huyện cũ (Table)
-- ----------------------------------------------------------------------------
SELECT district_old_map_key AS quan_huyen,
       COUNT(*) AS so_tin,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2,
       ROUND(AVG(area_m2), 2) AS dien_tich_tb_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
  AND district_old_map_key <> ''
[[ AND {{ngay_quan_sat}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
[[ AND {{khu_vuc_quan}} ]]
GROUP BY district_old_map_key
ORDER BY gia_tb_trieu_m2 DESC
;


-- ============================================================================
-- TAB 3 — XU HƯỚNG & PHÂN BỐ
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Card 11 — Xu hướng giá TB/m² theo tháng, theo loại hình BĐS (Line chart)
-- Cố ý KHÔNG có is_current = TRUE: dùng toàn bộ observation để dựng trục thời gian.
-- ----------------------------------------------------------------------------
SELECT DATE_TRUNC('month', observed_date)::date AS thang,
       property_type_name,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2
FROM gold.vw_fact_report
WHERE price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
[[ AND {{ngay_quan_sat}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
GROUP BY 1, 2
ORDER BY 1
;

-- ----------------------------------------------------------------------------
-- Card 12 — Số lượng tin quan sát theo tháng (Bar chart)
-- Giữ nguyên 4 điều kiện lọc chuẩn để trục X khớp đúng tập dữ liệu ở Card 11.
-- ----------------------------------------------------------------------------
SELECT DATE_TRUNC('month', observed_date)::date AS thang,
       COUNT(*) AS so_quan_sat
FROM gold.vw_fact_report
WHERE price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
[[ AND {{ngay_quan_sat}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
GROUP BY 1
ORDER BY 1
;

-- ----------------------------------------------------------------------------
-- Card 13 — Phân bố tin theo loại hình BĐS (Row chart)
-- Không lọc price_is_.../area_is_...: chỉ đếm số lượng, không liên quan giá trị giá/diện tích.
-- ----------------------------------------------------------------------------
SELECT property_type_name,
       COUNT(*) AS so_tin
FROM gold.vw_fact_report
WHERE is_current = TRUE
[[ AND {{ngay_quan_sat}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
GROUP BY property_type_name
ORDER BY so_tin DESC
;

-- ----------------------------------------------------------------------------
-- Card 14 — Tỷ lệ Cần bán / Cho thuê (Donut chart)
-- ----------------------------------------------------------------------------
SELECT listing_type,
       COUNT(*) AS so_tin
FROM gold.vw_fact_report
WHERE is_current = TRUE
[[ AND {{ngay_quan_sat}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
GROUP BY listing_type
;

-- ----------------------------------------------------------------------------
-- Card 15 — Giá TB/m² theo loại hình BĐS (Bar chart)
-- ----------------------------------------------------------------------------
SELECT property_type_name,
       ROUND(AVG(price_per_m2_vnd) / 1000000.0, 2) AS gia_tb_trieu_m2
FROM gold.vw_fact_report
WHERE is_current = TRUE
  AND price_is_negotiable = FALSE
  AND price_is_outlier = FALSE
  AND area_is_outlier = FALSE
  AND area_is_undetermined = FALSE
[[ AND {{ngay_quan_sat}} ]]
[[ AND {{loai_tin}} ]]
[[ AND {{loai_hinh_bds}} ]]
GROUP BY property_type_name
ORDER BY gia_tb_trieu_m2 DESC
;
