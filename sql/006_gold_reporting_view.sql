-- ============================================================================
-- sql/006_gold_reporting_view.sql
-- Tầng BI (Metabase) — KHÔNG thêm logic nghiệp vụ mới, chỉ 2 việc:
--   1. gold.map_district_geo_crosswalk — bảng mapping nhỏ, CHỈ phục vụ khớp
--      tên quận/huyện với GeoJSON (gis.vn) khi build region map trên Metabase.
--      KHÔNG đụng vào gold.dim_location gốc — giữ nguyên district_old đúng
--      loại hình hành chính hiện hành (VD: "Thành phố Bến Cát"), chỉ ánh xạ
--      SANG tên mà GeoJSON đang dùng (snapshot cũ hơn, còn ghi "Thị xã Bến Cát").
--      Xem lý do chi tiết: đối chiếu 38/39 quận/huyện, 5 lệch do khác thời điểm
--      snapshot giữa 2 nguồn (Dĩ An/Thuận An 2020, Tân Uyên 2023, Bến Cát 2024,
--      Phú Mỹ lên TP gần đây).
--   2. gold.vw_fact_report — view denormalized (Fact + 5 Dim), phẳng hóa toàn
--      bộ Star Schema thành 1 bảng duy nhất để Metabase GUI (notebook editor)
--      dùng trực tiếp, không phải tự JOIN lại mỗi Question. Grain GIỮ NGUYÊN
--      = grain của gold.fact_listing_price (1 dòng = 1 version giá đã quan sát).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. CROSSWALK — chỉ 5 dòng, đúng bằng số lệch phát hiện được khi đối chiếu
--    GeoJSON quận/huyện cũ (gis.vn) với gold.dim_location.district_old.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.map_district_geo_crosswalk (
    district_old         VARCHAR(100)  PRIMARY KEY,   -- Giá trị hiện hành trong gold.dim_location.
    geojson_ten_day_du     VARCHAR(100)  NOT NULL,       -- Giá trị khớp field "ten_day_du" trong GeoJSON.
    ghi_chu                 TEXT
);

COMMENT ON TABLE gold.map_district_geo_crosswalk IS
    'Mapping trình bày CHỈ dùng cho region map Metabase — ánh xạ district_old (loại hình '
    'hành chính hiện hành) sang tên mà GeoJSON quận/huyện cũ (nguồn gis.vn) đang dùng '
    '(snapshot cũ hơn, một số thị xã/huyện chưa lên thành phố tại thời điểm biên soạn). '
    'KHÔNG sửa gold.dim_location — bảng này chỉ phục vụ tầng trình bày.';

INSERT INTO gold.map_district_geo_crosswalk (district_old, geojson_ten_day_du, ghi_chu) VALUES
    ('Thành phố Bến Cát',  'Thị xã Bến Cát',  'GeoJSON snapshot trước 01/05/2024 (Bến Cát lên TP)'),
    ('Thành phố Dĩ An',    'Thị xã Dĩ An',    'GeoJSON snapshot trước 2020 (Dĩ An lên TP)'),
    ('Thành phố Thuận An', 'Thị xã Thuận An', 'GeoJSON snapshot trước 2020 (Thuận An lên TP)'),
    ('Thành phố Tân Uyên', 'Thị xã Tân Uyên', 'GeoJSON snapshot trước 2023 (Tân Uyên lên TP)'),
    ('Thành phố Phú Mỹ',   'Huyện Phú Mỹ',    'GeoJSON snapshot trước khi Phú Mỹ lên TP')
ON CONFLICT (district_old) DO UPDATE SET
    geojson_ten_day_du = EXCLUDED.geojson_ten_day_du,
    ghi_chu = EXCLUDED.ghi_chu;

-- ----------------------------------------------------------------------------
-- 2. VIEW REPORTING — Fact + 5 Dim, phẳng hóa cho Metabase.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_fact_report AS
SELECT
    -- Degenerate dimension / định danh
    f.listing_key,
    f.listing_id,
    f.listing_url,

    -- Measure chính
    f.price_vnd,
    f.price_per_m2_vnd,
    f.area_m2,
    f.bedrooms,
    f.floors,
    f.length_m,
    f.width_m,
    f.street_width_m,

    -- Cờ lọc — LUÔN filter các cờ *_outlier/*_negotiable/*_undetermined khi tính AVG/SUM
    f.price_is_negotiable,
    f.price_is_outlier,
    f.area_is_undetermined,
    f.area_is_outlier,
    f.has_warning,
    f.is_expired,

    -- Temporal / độ tin cậy
    f.valid_from,
    f.valid_to,
    f.last_seen_at,
    f.is_current,
    f.is_reconfirmed,

    -- Dim_date (role-playing, join 2 lần)
    dd_obs.full_date   AS observed_date,
    dd_obs.day         AS observed_day,
    dd_obs.month       AS observed_month,
    dd_obs.quarter     AS observed_quarter,
    dd_obs.year        AS observed_year,
    dd_post.full_date  AS posted_date,
    dd_post.month      AS posted_month,
    dd_post.year       AS posted_year,

    -- Dim_location
    dl.province_new,
    dl.ward_new,
    dl.province_old,
    dl.ward_old,
    dl.district_old,
    -- Cột phái sinh CHỈ dùng làm region identifier khi build map theo quận/huyện cũ.
    -- Query để tính AVG/COUNT theo địa lý cũ PHẢI GROUP BY cột này (không phải district_old thô).
    COALESCE(mc.geojson_ten_day_du, dl.district_old) AS district_old_map_key,
    dl.street,

    -- Dim_property_type
    dpt.property_type_name,
    dpt.listing_type,

    -- Dim_source
    ds.source_name,
    ds.source_part,

    -- Dim_property_features (junk dimension)
    dpf.orientation,
    dpf.legal_status,
    dpf.has_dining_room,
    dpf.has_kitchen,
    dpf.has_rooftop,
    dpf.has_car_parking,
    dpf.owner_direct

FROM gold.fact_listing_price f
JOIN gold.dim_date dd_obs               ON dd_obs.date_key = f.observed_date_key
JOIN gold.dim_date dd_post              ON dd_post.date_key = f.posted_date_key
JOIN gold.dim_location dl               ON dl.location_key = f.location_key
JOIN gold.dim_property_type dpt         ON dpt.property_type_key = f.property_type_key
JOIN gold.dim_source ds                 ON ds.source_key = f.source_key
JOIN gold.dim_property_features dpf     ON dpf.feature_key = f.feature_key
LEFT JOIN gold.map_district_geo_crosswalk mc ON mc.district_old = dl.district_old;

COMMENT ON VIEW gold.vw_fact_report IS
    'View phẳng phục vụ Metabase (tầng BI) — Fact + 5 Dim đã JOIN sẵn. Grain GIỮ NGUYÊN như '
    'gold.fact_listing_price (1 dòng = 1 version giá đã quan sát). KHÔNG tổng hợp sẵn (không '
    'GROUP BY) — để Metabase tự aggregate theo nhu cầu từng Question/Dashboard filter. '
    'Khi tính giá TB/m² LUÔN lọc price_is_negotiable=FALSE, price_is_outlier=FALSE, '
    'area_is_outlier=FALSE, area_is_undetermined=FALSE. Khi cần "hiện trạng thị trường" '
    '(map, KPI, phân bố) thêm is_current=TRUE; khi cần "xu hướng theo thời gian" bỏ điều kiện '
    'is_current (dùng toàn bộ observation), bucket theo tuần/tháng — xem comment '
    'observed_date_key ở gold.fact_listing_price. Dùng district_old_map_key (không phải '
    'district_old thô) khi GROUP BY để build region map theo quận/huyện cũ.';
