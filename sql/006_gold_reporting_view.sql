-- sql/006_gold_reporting_view.sql (đề xuất — CHƯA tạo file, chờ xác nhận)
-- View phục vụ tầng BI (Metabase). Grain giữ nguyên = grain của fact_listing_price
-- (1 dòng = 1 version giá đã quan sát). KHÔNG tổng hợp sẵn ở đây — để Metabase tự GROUP BY.
CREATE OR REPLACE VIEW gold.vw_fact_report AS
SELECT
    f.listing_key, f.listing_id, f.listing_url,
    f.price_vnd, f.price_per_m2_vnd, f.area_m2, f.bedrooms, f.floors,
    f.length_m, f.width_m, f.street_width_m,
    f.price_is_negotiable, f.price_is_outlier,
    f.area_is_undetermined, f.area_is_outlier,
    f.is_current, f.is_reconfirmed, f.is_expired, f.has_warning,
    f.valid_from, f.last_seen_at,
    dd_obs.full_date  AS observed_date,
    dd_obs.year       AS observed_year,
    dd_obs.month      AS observed_month,
    dd_obs.quarter    AS observed_quarter,
    dd_post.full_date AS posted_date,
    dl.ward_new, dl.province_new,
    dl.ward_old, dl.district_old, dl.province_old, dl.street,
    dpt.property_type_name, dpt.listing_type,
    ds.source_name, ds.source_part,
    dpf.orientation, dpf.legal_status,
    dpf.has_dining_room, dpf.has_kitchen, dpf.has_rooftop,
    dpf.has_car_parking, dpf.owner_direct
FROM gold.fact_listing_price f
JOIN gold.dim_date dd_obs         ON dd_obs.date_key = f.observed_date_key
JOIN gold.dim_date dd_post        ON dd_post.date_key = f.posted_date_key
JOIN gold.dim_location dl         ON dl.location_key = f.location_key
JOIN gold.dim_property_type dpt   ON dpt.property_type_key = f.property_type_key
JOIN gold.dim_source ds           ON ds.source_key = f.source_key
JOIN gold.dim_property_features dpf ON dpf.feature_key = f.feature_key;