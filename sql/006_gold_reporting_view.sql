-- ============================================================================
-- sql/006_gold_reporting_view.sql
-- Tầng BI (Metabase) — KHÔNG đụng đến dữ liệu Silver/Gold gốc.
--
-- Gồm 3 phần:
--   1. gold.map_district_geo_crosswalk — ánh xạ district_old -> tên khớp
--      GeoJSON quận/huyện cũ (nguồn: gis.vn, snapshot cũ hơn thời điểm hiện
--      hành nên 1 số quận/huyện đã lên Thành phố nhưng GeoJSON còn ghi
--      Thị xã/Huyện).
--   2. gold.map_ward_geo_crosswalk — ánh xạ ward_new -> tên khớp GeoJSON
--      phường/xã mới (nguồn: gis.vn, đã cập nhật sau 01/07/2025). Hiện chỉ
--      có 1 trường hợp lệch thật (data quality issue của alonhadat, không
--      phải lỗi ETL — xem ghi_chu).
--   3. gold.vw_fact_report — view phẳng join Fact + 5 Dim, dùng làm nguồn
--      chính cho mọi Question trong Metabase. Grain giữ nguyên = grain của
--      fact_listing_price (1 dòng = 1 version giá đã quan sát).
--
-- Chuẩn hóa Unicode: mọi so khớp text với GeoJSON đều bọc NORMALIZE(..., NFC)
-- 2 chiều — tránh lỗi lệch do 1 bên NFC/1 bên NFD (xem case "Xã Đất Đỏ" đã
-- phát hiện qua đối chiếu thủ công, dù hiển thị giống hệt nhau nhưng so sánh
-- string trực tiếp cho kết quả False). Hàm NORMALIZE() có sẵn từ PostgreSQL
-- 13+ (image postgres:16 đang dùng đáp ứng đủ), không cần cài extension.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. CROSSWALK — Quận/Huyện cũ (district_old) -> tên khớp GeoJSON
--
-- Nguyên nhân lệch: GeoJSON (gis.vn) là snapshot tại thời điểm các đơn vị
-- này còn là Thị xã/Huyện, trong khi district_old ở Gold ghi đúng loại hình
-- hành chính hiện hành (đã lên Thành phố). Xác nhận qua tra cứu:
--   - Dĩ An, Thuận An: lên Thành phố năm 2020
--   - Tân Uyên: lên Thành phố năm 2023
--   - Bến Cát: lên Thành phố từ 01/05/2024
--   - Phú Mỹ: lên Thành phố (Huyện -> Thành phố)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.map_district_geo_crosswalk (
    district_old         VARCHAR(100)  PRIMARY KEY,
    geojson_ten_day_du     VARCHAR(100)  NOT NULL,   -- Khớp field "loai" + " " + "ten_huyen" trong GeoJSON đã build.
    ghi_chu                TEXT
);

COMMENT ON TABLE gold.map_district_geo_crosswalk IS
    'Crosswalk CHỈ phục vụ trình bày bản đồ Metabase (region map cấp Quận/Huyện cũ). '
    'KHÔNG sửa gold.dim_location.district_old gốc — giữ nguyên nguyên tắc Gold data immutable.';

INSERT INTO gold.map_district_geo_crosswalk (district_old, geojson_ten_day_du, ghi_chu) VALUES
    ('Thành phố Bến Cát',  'Thị xã Bến Cát',  'GeoJSON snapshot trước 01/05/2024 (Bến Cát lên TP)'),
    ('Thành phố Dĩ An',    'Thị xã Dĩ An',    'GeoJSON snapshot trước 2020 (Dĩ An lên TP)'),
    ('Thành phố Thuận An', 'Thị xã Thuận An', 'GeoJSON snapshot trước 2020 (Thuận An lên TP)'),
    ('Thành phố Tân Uyên', 'Thị xã Tân Uyên', 'GeoJSON snapshot trước 2023 (Tân Uyên lên TP)'),
    ('Thành phố Phú Mỹ',   'Huyện Phú Mỹ',    'GeoJSON snapshot trước khi Phú Mỹ lên TP')
ON CONFLICT (district_old) DO NOTHING;

-- ----------------------------------------------------------------------------
-- 2. CROSSWALK — Phường/Xã mới (ward_new) -> tên khớp GeoJSON
--
-- Nguyên nhân lệch: data quality issue của NGUỒN alonhadat (field "phường xã
-- mới" ghi sai tiền tố), KHÔNG phải lỗi ETL của dự án — ward_new lấy trực
-- tiếp từ field gốc, không qua transform/crosswalk nào. Đã xác nhận qua
-- Nghị quyết 1685/NQ-UBTVQH15 (hiệu lực 01/07/2025): tên chính thức là
-- "Xã Hóc Môn" (hợp nhất xã Tân Hiệp, xã Tân Xuân và thị trấn Hóc Môn).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.map_ward_geo_crosswalk (
    ward_new              VARCHAR(100)  PRIMARY KEY,
    geojson_ten_day_du     VARCHAR(100)  NOT NULL,   -- Khớp field "loai" + " " + "ten_xa" trong GeoJSON đã build.
    ghi_chu                TEXT
);

COMMENT ON TABLE gold.map_ward_geo_crosswalk IS
    'Crosswalk CHỈ phục vụ trình bày bản đồ Metabase (region map cấp Phường/Xã mới). '
    'KHÔNG sửa gold.dim_location.ward_new gốc — giữ nguyên nguyên tắc Gold data immutable.';

INSERT INTO gold.map_ward_geo_crosswalk (ward_new, geojson_ten_day_du, ghi_chu) VALUES
    ('Phường Hóc Môn', 'Xã Hóc Môn',
     'Data quality issue nguồn alonhadat: sai tiền tố. Tên chính thức theo NQ 1685/NQ-UBTVQH15 '
     '(01/07/2025) là "Xã Hóc Môn", hợp nhất xã Tân Hiệp, xã Tân Xuân, thị trấn Hóc Môn')
ON CONFLICT (ward_new) DO NOTHING;

-- ----------------------------------------------------------------------------
-- 3. VIEW BÁO CÁO PHẲNG — nguồn chính cho mọi Question trong Metabase.
--
-- Cột *_map_key: dùng làm region identifier khi build câu hỏi region map
-- (GROUP BY *_map_key, KHÔNG GROUP BY ward_new/district_old thô). Cả 2 vế so
-- khớp trong LEFT JOIN đều bọc NORMALIZE(..., NFC) để tránh lệch do encode
-- Unicode khác dạng (NFC/NFD) dù hiển thị giống hệt nhau.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_fact_report AS
SELECT
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

    -- Cờ lọc — LUÔN áp dụng khi tính AVG/SUM giá (xem COMMENT gốc ở fact_listing_price)
    f.price_is_negotiable,
    f.price_is_outlier,
    f.area_is_undetermined,
    f.area_is_outlier,
    f.has_warning,
    f.is_expired,
    f.is_current,
    f.is_reconfirmed,

    -- Temporal
    f.valid_from,
    f.valid_to,
    f.last_seen_at,

    -- Dim_date (role-playing, join 2 lần)
    dd_obs.full_date   AS observed_date,
    dd_obs.year        AS observed_year,
    dd_obs.month       AS observed_month,
    dd_obs.quarter     AS observed_quarter,
    dd_post.full_date  AS posted_date,

    -- Dim_location — giữ nguyên bản gốc để audit/hiển thị
    dl.province_new,
    dl.ward_new,
    dl.province_old,
    dl.ward_old,
    dl.district_old,
    dl.street,

    -- Map key — dùng cho region map Metabase (đã chuẩn hóa NFC + qua crosswalk nếu cần)
    COALESCE(
        mw.geojson_ten_day_du,
        NORMALIZE(dl.ward_new, NFC)
    ) AS ward_new_map_key,
    COALESCE(
        md.geojson_ten_day_du,
        NORMALIZE(dl.district_old, NFC)
    ) AS district_old_map_key,

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
JOIN gold.dim_date dd_obs             ON dd_obs.date_key = f.observed_date_key
JOIN gold.dim_date dd_post            ON dd_post.date_key = f.posted_date_key
JOIN gold.dim_location dl             ON dl.location_key = f.location_key
JOIN gold.dim_property_type dpt       ON dpt.property_type_key = f.property_type_key
JOIN gold.dim_source ds               ON ds.source_key = f.source_key
JOIN gold.dim_property_features dpf   ON dpf.feature_key = f.feature_key
LEFT JOIN gold.map_ward_geo_crosswalk mw
       ON NORMALIZE(mw.ward_new, NFC) = NORMALIZE(dl.ward_new, NFC)
LEFT JOIN gold.map_district_geo_crosswalk md
       ON NORMALIZE(md.district_old, NFC) = NORMALIZE(dl.district_old, NFC);

COMMENT ON VIEW gold.vw_fact_report IS
    'View phẳng phục vụ Metabase — join sẵn Fact + 5 Dim, KHÔNG tổng hợp sẵn (để Metabase '
    'tự GROUP BY theo nhu cầu từng Question). Grain = grain của gold.fact_listing_price. '
    'Dùng ward_new_map_key/district_old_map_key (không phải ward_new/district_old thô) '
    'khi build region map — đã xử lý sẵn lệch tên do khác thời điểm snapshot GeoJSON và '
    'lệch encode Unicode NFC/NFD.';
