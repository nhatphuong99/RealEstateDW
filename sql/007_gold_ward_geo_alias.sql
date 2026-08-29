-- ============================================================================
-- sql/007_gold_ward_geo_alias.sql - gold.dim_location_ward_geo_alias
-- Mục đích: ánh xạ ward_new (gold.dim_location) sang fullName (geojson)
-- để Metabase region map khớp đúng vùng.
--
-- Không sửa ward_new trong Silver/Gold vì cả hai chính tả "Hoa/Hòa" đều hợp lệ.
-- Đây chỉ là bảng tra cứu phục vụ BI layer, không thay đổi dữ liệu gốc.
--
-- Riêng "Hóc Môn": lỗi dữ liệu thật, tên chính thức là "Xã Hóc Môn"
-- (NQ 1685/NQ-UBTVQH15, 16/6/2025). Silver đang gán sai thành "Phường Hóc Môn".
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.dim_location_ward_geo_alias (
    ward_new_dw             TEXT PRIMARY KEY,   -- giá trị trong gold.dim_location.ward_new
    ward_fullname_geojson   TEXT NOT NULL,       -- giá trị fullName trong geojson
    note                    TEXT NOT NULL
);


INSERT INTO gold.dim_location_ward_geo_alias (ward_new_dw, ward_fullname_geojson, note) VALUES
    ('Phường Bình Hòa',      'Phường Bình Hoà',      'chính tả oà - òa kiểu cũ-mới'),
    ('Phường Bình Hưng Hòa', 'Phường Bình Hưng Hoà', 'chính tả oà - òa kiểu cũ-mới'),
    ('Phường Chánh Phú Hòa', 'Phường Chánh Phú Hoà', 'chính tả oà - òa kiểu cũ-mới'),
    ('Phường Hòa Bình',      'Phường Hoà Bình',      'chính tả oà - òa kiểu cũ-mới'),
    ('Phường Hòa Hưng',      'Phường Hoà Hưng',      'chính tả oà - òa kiểu cũ-mới'),
    ('Phường Hòa Lợi',       'Phường Hoà Lợi',       'chính tả oà - òa kiểu cũ-mới'),
    ('Phường Phú Thọ Hòa',   'Phường Phú Thọ Hoà',   'chính tả oà - òa kiểu cũ-mới'),
    ('Phường Thới Hòa',      'Phường Thới Hoà',      'chính tả oà - òa kiểu cũ-mới'),
    ('Phường Tân Hòa',       'Phường Tân Hoà',       'chính tả oà - òa kiểu cũ-mới'),
    ('Phường Tân Sơn Hòa',   'Phường Tân Sơn Hoà',   'chính tả oà - òa kiểu cũ-mới'),
    ('Phường Xuân Hòa',      'Phường Xuân Hoà',      'chính tả oà - òa kiểu cũ-mới'),
    ('Phường Đông Hòa',      'Phường Đông Hoà',      'chính tả oà - òa kiểu cũ-mới'),
    ('Xã Hòa Hiệp',          'Xã Hoà Hiệp',          'chính tả oà - òa kiểu cũ-mới'),
    ('Xã Hòa Hội',           'Xã Hoà Hội',           'chính tả oà - òa kiểu cũ-mới'),
    ('Xã Long Hòa',          'Xã Long Hoà',          'chính tả oà - òa kiểu cũ-mới'),
    ('Xã Phú Hòa Đông',      'Xã Phú Hoà Đông',      'chính tả oà - òa kiểu cũ-mới'),
    ('Xã Phước Hòa',         'Xã Phước Hoà',         'chính tả oà - òa kiểu cũ-mới'),
    ('Phường Hóc Môn',       'Xã Hóc Môn',           'Theo NQ 1685/NQ-UBTVQH15 là Xã, không phải Phường')
ON CONFLICT (ward_new_dw) DO UPDATE
    SET ward_fullname_geojson = EXCLUDED.ward_fullname_geojson,
        note = EXCLUDED.note;
