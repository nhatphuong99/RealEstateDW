-- analytics/sql/qa/q5_qa.sql
-- QA và chẩn đoán Q5 (chỉ đọc). Chạy từng khối trong DataGrip (bôi đen khối, Ctrl+Enter).

-- KHỐI A: đối chiếu với sizing. Kỳ vọng so_cap = 2068; so_cap_doi_gia_sizing = 996; so_giam_sizing = 842.
-- so_cap_doi_gia_card (CARD 1, bỏ giá = 0) = 996 - so_doi_gia_co_gia_0.
SELECT
    COUNT(*) AS so_cap,
    COUNT(*) FILTER (WHERE price_vnd IS NOT NULL AND prev_price_vnd IS NOT NULL AND price_vnd <> prev_price_vnd) AS so_cap_doi_gia_sizing,
    COUNT(*) FILTER (WHERE price_vnd IS NOT NULL AND prev_price_vnd IS NOT NULL AND price_vnd < prev_price_vnd) AS so_giam_sizing,
    COUNT(*) FILTER (WHERE price_vnd = 0 OR prev_price_vnd = 0) AS so_cap_co_gia_0,
    COUNT(*) FILTER (WHERE (price_vnd = 0 OR prev_price_vnd = 0) AND price_vnd <> prev_price_vnd) AS so_doi_gia_co_gia_0,
    COUNT(*) FILTER (WHERE price_vnd > 0 AND prev_price_vnd > 0 AND price_vnd <> prev_price_vnd) AS so_cap_doi_gia_card
FROM analytics.vw_listing_versions
WHERE prev_source_name = 'dataset' AND source_name = 'web';

-- KHỐI B: số version mỗi listing. Kỳ vọng: 2.068 listing có 2 version (= 247.068 - 245.000), không listing nào có 3 version trở lên.
SELECT so_version, COUNT(*) AS so_listing
FROM (SELECT listing_id, COUNT(*) AS so_version FROM analytics.vw_listing_versions GROUP BY listing_id) t
GROUP BY so_version
ORDER BY so_version;

-- KHỐI C: phân loại đuôi (cặp có đổi giá, bỏ giá = 0). Cho biết đuôi lớn hay nhỏ trước khi quyết định có cần thêm quy tắc loại.
WITH p AS (
    SELECT (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd AS pct
    FROM analytics.vw_listing_versions v
    WHERE v.prev_source_name = 'dataset' AND v.source_name = 'web'
      AND v.price_vnd > 0 AND v.prev_price_vnd > 0 AND v.price_vnd <> v.prev_price_vnd
)
SELECT
    COUNT(*) AS so_cap_doi_gia,
    COUNT(*) FILTER (WHERE pct <= -0.5) AS giam_tu_50,
    COUNT(*) FILTER (WHERE pct >= 0.5) AS tang_tu_50,
    COUNT(*) FILTER (WHERE pct >= 9) AS tang_tu_10_lan,
    COUNT(*) FILTER (WHERE pct <= -0.9) AS giam_tu_90
FROM p;

-- KHỐI D: 15 cặp có |% đổi| lớn nhất (để mở URL kiểm tra tay, như bước D7). Không đặt alias cho vw_listing_latest.
SELECT
    v.listing_id,
    v.prev_price_vnd,
    v.price_vnd,
    ROUND((100 * (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd)::numeric, 1) AS pct,
    v.days_since_prev_version AS gap_ngay,
    analytics.vw_listing_latest.listing_type,
    analytics.vw_listing_latest.property_type_name,
    analytics.vw_listing_latest.price_per_m2_vnd
FROM analytics.vw_listing_versions v
JOIN analytics.vw_listing_latest ON analytics.vw_listing_latest.listing_id = v.listing_id
WHERE v.prev_source_name = 'dataset' AND v.source_name = 'web'
  AND v.price_vnd > 0 AND v.prev_price_vnd > 0 AND v.price_vnd <> v.prev_price_vnd
ORDER BY ABS((v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd) DESC
LIMIT 15;

-- KHỐI E: cỡ mẫu theo Loại tin (quyết định giá trị mặc định của bộ lọc Loại Tin ở tab Q5)
SELECT
    analytics.vw_listing_latest.listing_type,
    COUNT(*) AS so_cap,
    COUNT(*) FILTER (WHERE v.price_vnd <> v.prev_price_vnd) AS so_cap_doi_gia,
    COUNT(*) FILTER (WHERE v.price_vnd < v.prev_price_vnd) AS so_giam
FROM analytics.vw_listing_versions v
JOIN analytics.vw_listing_latest ON analytics.vw_listing_latest.listing_id = v.listing_id
WHERE v.prev_source_name = 'dataset' AND v.source_name = 'web'
  AND v.price_vnd > 0 AND v.prev_price_vnd > 0
GROUP BY analytics.vw_listing_latest.listing_type
ORDER BY 1;

-- KHỐI F: cột price_change_pct của view dùng đơn vị nào? Một trong hai sai số phải bằng 0 (tỷ lệ hay %).
-- Kỳ vọng: sai_so_ty_le = 0 (hoặc sai_so_phan_tram = 0). Nếu cả hai khác 0, báo lại.
SELECT
    MAX(ABS(price_change_pct - (price_vnd - prev_price_vnd) / prev_price_vnd)) AS sai_so_ty_le,
    MAX(ABS(price_change_pct - 100 * (price_vnd - prev_price_vnd) / prev_price_vnd)) AS sai_so_phan_tram
FROM analytics.vw_listing_versions
WHERE prev_source_name = 'dataset' AND source_name = 'web' AND prev_price_vnd > 0 AND price_change_pct IS NOT NULL;

-- KHỐI G: median khoảng cách ngày theo view (days_since_prev_version). Kỳ vọng khớp sizing khối 3: 14 / 38 / 98 (lệch tối đa 1 ngày).
SELECT
    MIN(days_since_prev_version) AS gap_min,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY days_since_prev_version) AS gap_median,
    MAX(days_since_prev_version) AS gap_max
FROM analytics.vw_listing_versions
WHERE prev_source_name = 'dataset' AND source_name = 'web';

-- KHỐI H: 15 cặp giảm mạnh nhất (đuôi dưới; khối C cho biết có 17 cặp giảm từ 90% trở lên).
-- dien_tich_suy_ra_m2 = giá mới / giá m² của bản mới nhất (giá trị suy ra, dùng để xét giá có hợp lý không, không phải dữ liệu gốc).
SELECT
    v.listing_id,
    v.prev_price_vnd,
    v.price_vnd,
    ROUND((100 * (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd)::numeric, 2) AS pct,
    v.days_since_prev_version AS gap_ngay,
    analytics.vw_listing_latest.listing_type,
    analytics.vw_listing_latest.property_type_name,
    analytics.vw_listing_latest.price_per_m2_vnd,
    ROUND((v.price_vnd / NULLIF(analytics.vw_listing_latest.price_per_m2_vnd, 0))::numeric, 1) AS dien_tich_suy_ra_m2
FROM analytics.vw_listing_versions v
JOIN analytics.vw_listing_latest ON analytics.vw_listing_latest.listing_id = v.listing_id
WHERE v.prev_source_name = 'dataset' AND v.source_name = 'web'
  AND v.price_vnd > 0 AND v.prev_price_vnd > 0 AND v.price_vnd <> v.prev_price_vnd
ORDER BY (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd
LIMIT 15;

-- KHỐI I: dấu hiệu gõ nhầm số 0. Đếm cặp có tỷ số giá mới / giá cũ đúng 10, 100, 1000 lần (hoặc nghịch đảo) và đúng 2 lần.
-- Tỷ số tròn nhiều bất thường là dấu hiệu lỗi nhập liệu, không phải biến động thị trường.
WITH p AS (
    SELECT ROUND(v.price_vnd / v.prev_price_vnd, 4) AS ty_so
    FROM analytics.vw_listing_versions v
    WHERE v.prev_source_name = 'dataset' AND v.source_name = 'web'
      AND v.price_vnd > 0 AND v.prev_price_vnd > 0 AND v.price_vnd <> v.prev_price_vnd
)
SELECT
    COUNT(*) FILTER (WHERE ty_so IN (10, 100, 1000)) AS tang_dung_10_100_1000_lan,
    COUNT(*) FILTER (WHERE ty_so IN (0.1, 0.01, 0.001)) AS giam_dung_10_100_1000_lan,
    COUNT(*) FILTER (WHERE ty_so = 2) AS tang_dung_2_lan,
    COUNT(*) FILTER (WHERE ty_so = 0.5) AS giam_dung_1_nua
FROM p;


-- KHỐI J: phân loại cặp có đổi giá theo dấu hiệu lỗi dữ liệu (chỉ dùng cột đã có, không đụng Gold).
-- sua_don_vi: giá cũ / giá mới (hoặc ngược lại) = diện tích suy ra (sai số 0,5%) -> nhãn "X tỷ / m²" bị nhập là tổng giá, hoặc ngược lại.
-- go_nham_so_0: tỷ số giá mới / giá cũ đúng 10, 100, 1000 lần hoặc nghịch đảo.
-- con_lai: chưa có dấu hiệu lỗi (gồm cả đổi loại tin bán <-> thuê, sẽ tách riêng ở khối K).
WITH p AS (
    SELECT
        analytics.vw_listing_latest.listing_type,
        v.price_vnd / v.prev_price_vnd AS ty_so,
        v.price_vnd / NULLIF(analytics.vw_listing_latest.price_per_m2_vnd, 0) AS dien_tich
    FROM analytics.vw_listing_versions v
    JOIN analytics.vw_listing_latest ON analytics.vw_listing_latest.listing_id = v.listing_id
    WHERE v.prev_source_name = 'dataset' AND v.source_name = 'web'
      AND v.price_vnd > 0 AND v.prev_price_vnd > 0 AND v.price_vnd <> v.prev_price_vnd
), c AS (
    SELECT listing_type,
           CASE
               WHEN dien_tich > 1 AND ABS(1 / ty_so - dien_tich) <= 0.005 * dien_tich THEN 'sua_don_vi'
               WHEN dien_tich > 1 AND ABS(ty_so - dien_tich) <= 0.005 * dien_tich THEN 'sua_don_vi'
               WHEN ROUND(ty_so, 4) IN (10, 100, 1000, 0.1, 0.01, 0.001) THEN 'go_nham_so_0'
               ELSE 'con_lai'
           END AS nhom
    FROM p
)
SELECT listing_type, nhom, COUNT(*) AS so_cap
FROM c
GROUP BY listing_type, nhom
ORDER BY listing_type, nhom;

-- KHỐI L: độ nhạy. So sánh phân vị % đổi giá giữa "tất cả" và "bỏ cặp có dấu hiệu lỗi ở khối J".
-- Tập loại gồm cặp có dấu hiệu lỗi và cặp không có diện tích để đối chiếu (nên n nhỏ hơn tổng "con_lai" ở khối J tối đa vài cặp).
-- Nếu median và % giảm gần như không đổi -> thẻ Q5 giữ nguyên, chỉ ghi giới hạn. Nếu lệch nhiều -> thêm điều kiện loại.
WITH p AS (
    SELECT
        (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd AS pct,
        v.price_vnd / v.prev_price_vnd AS ty_so,
        v.price_vnd / NULLIF(analytics.vw_listing_latest.price_per_m2_vnd, 0) AS dien_tich
    FROM analytics.vw_listing_versions v
    JOIN analytics.vw_listing_latest ON analytics.vw_listing_latest.listing_id = v.listing_id
    WHERE v.prev_source_name = 'dataset' AND v.source_name = 'web'
      AND v.price_vnd > 0 AND v.prev_price_vnd > 0 AND v.price_vnd <> v.prev_price_vnd
), f AS (
    SELECT pct,
           (   (dien_tich > 1 AND ABS(1 / ty_so - dien_tich) <= 0.005 * dien_tich)
            OR (dien_tich > 1 AND ABS(ty_so - dien_tich) <= 0.005 * dien_tich)
            OR ROUND(ty_so, 4) IN (10, 100, 1000, 0.1, 0.01, 0.001)) AS co_loi
    FROM p
)
SELECT 'tat_ca' AS tap, COUNT(*) AS n,
       ROUND((100 * percentile_cont(0.25) WITHIN GROUP (ORDER BY pct))::numeric, 1) AS p25_pct,
       ROUND((100 * percentile_cont(0.50) WITHIN GROUP (ORDER BY pct))::numeric, 1) AS median_pct,
       ROUND((100 * percentile_cont(0.75) WITHIN GROUP (ORDER BY pct))::numeric, 1) AS p75_pct,
       ROUND((100.0 * COUNT(*) FILTER (WHERE pct < 0) / COUNT(*))::numeric, 1) AS ty_le_giam_pct
FROM f
UNION ALL
SELECT 'bo_cap_co_dau_hieu_loi', COUNT(*),
       ROUND((100 * percentile_cont(0.25) WITHIN GROUP (ORDER BY pct))::numeric, 1),
       ROUND((100 * percentile_cont(0.50) WITHIN GROUP (ORDER BY pct))::numeric, 1),
       ROUND((100 * percentile_cont(0.75) WITHIN GROUP (ORDER BY pct))::numeric, 1),
       ROUND((100.0 * COUNT(*) FILTER (WHERE pct < 0) / COUNT(*))::numeric, 1)
FROM f WHERE co_loi IS FALSE;  -- cặp không suy ra được diện tích (co_loi NULL) cũng bị loại khỏi tập này

-- KHỐI K0: tìm cột loại tin theo từng version (để tách cặp đổi loại bán <-> thuê). Chỉ đọc định nghĩa, không sửa gì.
SELECT pg_get_viewdef('analytics.vw_listing_versions'::regclass, true) AS dinh_nghia_view;
