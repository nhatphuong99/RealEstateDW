-- ============================================================================
-- sql/queries/seed_test_scd2_merge.sql
-- Seed dữ liệu test cho merge_scd2_listing_history.sql (Phase 3, Task 16 & 17).
-- Dùng listing_id sentinel 900000001 (ngoài khoảng ID thật của
-- alonhadat.com.vn) để không đụng dữ liệu crawl thật. Script IDEMPOTENT —
-- chạy lại bao nhiêu lần cũng dọn sạch dữ liệu test cũ trước khi insert mới,
-- an toàn để test lại nhiều lần.
--
-- Kịch bản (đúng ví dụ minh họa trong bronze_to_silver_plan.md):
--   2026-03-01 -> giá 4,5 tỷ  (H1)
--   2026-05-11 -> giá 4,5 tỷ  (H1, không đổi)
--   2026-08-20 -> giá 4,2 tỷ  (H2, đổi giá)
--
-- Kết quả ĐÚNG sau khi chạy merge_scd2_listing_history.sql lần đầu:
--   listing_history có đúng 2 dòng cho listing_id=900000001:
--     v1: valid_from=2026-03-01, valid_to=2026-08-20, is_current=false,
--         last_seen_at=2026-05-11
--     v2: valid_from=2026-08-20, valid_to=NULL, is_current=true,
--         last_seen_at=2026-08-20
--
-- Cách dùng — Task 16 (nhiều version cùng 1 batch):
--   1. psql ... -f sql/queries/seed_test_scd2_merge.sql
--   2. psql ... -f sql/queries/merge_scd2_listing_history.sql
--   3. SELECT listing_id, valid_from, valid_to, is_current, last_seen_at,
--             price_vnd, row_hash
--      FROM silver.listing_history WHERE listing_id = 900000001
--      ORDER BY valid_from;
--      -> phải thấy đúng 2 dòng như mô tả ở trên.
--
-- Cách dùng — Task 17 (rerun idempotent, KHÔNG chạy lại bước 1):
--   4. Chạy thẳng lại bước 2 (merge_scd2_listing_history.sql) LẦN NỮA,
--      KHÔNG seed lại (staging_batch vẫn còn nguyên 3 dòng cũ vì merge
--      script không tự xoá staging).
--   5. Lặp lại câu SELECT ở bước 3 -> vẫn phải ra ĐÚNG 2 dòng y hệt (không
--      sinh thêm version nào, last_seen_at không đổi vì crawl_date trong
--      staging không có gì mới hơn last_seen_at hiện tại).
-- ============================================================================

BEGIN;

-- Dọn dữ liệu test cũ (idempotent) — chỉ động tới listing_id sentinel,
-- không ảnh hưởng dữ liệu crawl thật.
DELETE FROM silver.listing_staging_batch WHERE listing_id = 900000001;
DELETE FROM silver.listing_history WHERE listing_id = 900000001;
DELETE FROM silver.parse_quarantine WHERE source_bronze_key = 'test/seed_scd2.parquet';

INSERT INTO silver.listing_staging_batch (
    listing_id, listing_url, source_part, source_bronze_key, crawl_date,
    title, listing_type, property_type, posted_date,
    price_vnd, price_raw, price_is_negotiable,
    area_m2, area_raw, area_is_undetermined,
    length_m, width_m, street_width_m, floors, bedrooms,
    orientation, legal_status,
    has_dining_room, has_kitchen, has_rooftop, has_car_parking, owner_direct,
    is_expired, has_warning,
    address_street_new, address_ward_new, address_province_new,
    address_old_raw, address_ward_old, address_district_old
)
VALUES
-- Quan sat 1 (2026-03-01): gia 4.5 ty -> H1. Mo phong crawl dataset dot dau.
(900000001, 'https://alonhadat.com.vn/test/listing-test-900000001.html',
 'test_seed', 'test/seed_scd2.parquet', '2026-03-01 08:00:00+07',
 'Nha test SCD2 merge - khong dung de phan tich thuc te', 'sale', 'Nha pho',
 '2026-03-01',
 4500000000, '4,5 ty', FALSE,
 88.0, '88', FALSE,
 20, 5, 8, 3, 3,
 'Dong Nam', 'So hong',
 NULL, NULL, NULL, NULL, NULL,
 FALSE, FALSE,
 'Duong test', 'Phuong test', 'TP.HCM',
 NULL, NULL, NULL),

-- Quan sat 2 (2026-05-11): van gia 4.5 ty -> H1, khong doi. Mo phong crawl
-- lai lan 2, test dung nguyen tac "duplicate trong Bronze la binh thuong,
-- khong tao version moi neu hash khong doi".
(900000001, 'https://alonhadat.com.vn/test/listing-test-900000001.html',
 'test_seed', 'test/seed_scd2.parquet', '2026-05-11 08:00:00+07',
 'Nha test SCD2 merge - khong dung de phan tich thuc te', 'sale', 'Nha pho',
 '2026-03-01',
 4500000000, '4,5 ty', FALSE,
 88.0, '88', FALSE,
 20, 5, 8, 3, 3,
 'Dong Nam', 'So hong',
 NULL, NULL, NULL, NULL, NULL,
 FALSE, FALSE,
 'Duong test', 'Phuong test', 'TP.HCM',
 NULL, NULL, NULL),

-- Quan sat 3 (2026-08-20): gia doi thanh 4.2 ty -> H2. Mo phong crawl web
-- moi phat hien doi gia -> phai tao version 2 trong listing_history.
(900000001, 'https://alonhadat.com.vn/test/listing-test-900000001.html',
 'test_seed', 'test/seed_scd2.parquet', '2026-08-20 08:00:00+07',
 'Nha test SCD2 merge - khong dung de phan tich thuc te', 'sale', 'Nha pho',
 '2026-03-01',
 4200000000, '4,2 ty', FALSE,
 88.0, '88', FALSE,
 20, 5, 8, 3, 3,
 'Dong Nam', 'So hong',
 NULL, NULL, NULL, NULL, NULL,
 FALSE, FALSE,
 'Duong test', 'Phuong test', 'TP.HCM',
 NULL, NULL, NULL);

COMMIT;
