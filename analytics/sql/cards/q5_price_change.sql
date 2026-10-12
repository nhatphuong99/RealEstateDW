-- analytics/sql/cards/q5_price_change.sql
-- Tab Q5 "Listing được web gặp lại có đổi giá không". Chỉ đọc analytics.vw_listing_versions (đã loại version trùng hash).
-- Cặp = version trước (dataset) và version sau (web) của cùng listing_id; chỉ giữ cặp có CẢ HAI giá > 0.
-- Đổi giá = price_vnd <> prev_price_vnd. pct = (giá mới - giá cũ) / giá cũ, tính trong SQL (không dùng cột price_change_pct của view).
-- Mẫu chỉ gồm cặp khác hash (cặp giống hash đã bị loại ở view), nên KHÔNG có tỷ lệ "listing gặp lại có đổi giá"
-- và không đại diện thị trường (docs/metrics.md mục 7 và 10).
-- Field Filter (Dropdown), map vào analytics.vw_listing_latest: {{loai_tin}} -> listing_type
-- KHÔNG đặt alias cho analytics.vw_listing_latest (vw_listing_versions thì được đặt alias v). Card nào cũng lặp lại CTE `pairs`.
-- Chạy thử trên DataGrip: xóa dòng [[AND {{loai_tin}}]] hoặc thay bằng điều kiện thật.

-- CARD 1: "Số cặp có đổi giá" (Number)
WITH pairs AS (
    SELECT
        v.days_since_prev_version AS gap_ngay,
        v.price_vnd,
        v.prev_price_vnd,
        (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd AS pct
    FROM analytics.vw_listing_versions v
    JOIN analytics.vw_listing_latest
      ON analytics.vw_listing_latest.listing_id = v.listing_id
    WHERE v.prev_source_name = 'dataset'
      AND v.source_name = 'web'
      AND v.price_vnd > 0
      AND v.prev_price_vnd > 0
      [[AND {{loai_tin}}]]
)
SELECT COUNT(*) AS so_cap_doi_gia
FROM pairs
WHERE price_vnd <> prev_price_vnd;

-- CARD 2: "Tỷ lệ cặp giảm giá (trong số cặp có đổi giá)" (Number, định dạng %; ẩn khi n < 30)
WITH pairs AS (
    SELECT
        v.days_since_prev_version AS gap_ngay,
        v.price_vnd,
        v.prev_price_vnd,
        (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd AS pct
    FROM analytics.vw_listing_versions v
    JOIN analytics.vw_listing_latest
      ON analytics.vw_listing_latest.listing_id = v.listing_id
    WHERE v.prev_source_name = 'dataset'
      AND v.source_name = 'web'
      AND v.price_vnd > 0
      AND v.prev_price_vnd > 0
      [[AND {{loai_tin}}]]
)
SELECT
    CASE WHEN COUNT(*) >= 30
         THEN ROUND((100.0 * COUNT(*) FILTER (WHERE pct < 0) / COUNT(*))::numeric, 1)
    END AS ty_le_giam_pct
FROM pairs
WHERE price_vnd <> prev_price_vnd;

-- CARD 3: "Phân vị % đổi giá" (Table một dòng: n, p25, median, p75; ẩn khi n < 30; KHÔNG dùng mean)
WITH pairs AS (
    SELECT
        v.days_since_prev_version AS gap_ngay,
        v.price_vnd,
        v.prev_price_vnd,
        (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd AS pct
    FROM analytics.vw_listing_versions v
    JOIN analytics.vw_listing_latest
      ON analytics.vw_listing_latest.listing_id = v.listing_id
    WHERE v.prev_source_name = 'dataset'
      AND v.source_name = 'web'
      AND v.price_vnd > 0
      AND v.prev_price_vnd > 0
      [[AND {{loai_tin}}]]
)
SELECT
    COUNT(*) AS n,
    CASE WHEN COUNT(*) >= 30 THEN ROUND((100 * percentile_cont(0.25) WITHIN GROUP (ORDER BY pct))::numeric, 1) END AS p25_pct,
    CASE WHEN COUNT(*) >= 30 THEN ROUND((100 * percentile_cont(0.50) WITHIN GROUP (ORDER BY pct))::numeric, 1) END AS median_pct,
    CASE WHEN COUNT(*) >= 30 THEN ROUND((100 * percentile_cont(0.75) WITHIN GROUP (ORDER BY pct))::numeric, 1) END AS p75_pct
FROM pairs
WHERE price_vnd <> prev_price_vnd;

-- CARD 4: "Phân bố mức đổi giá" (Bar; trục X = nhom_doi_gia; hai đầu mở để thấy đuôi bất thường)
-- Hai đầu đối xứng (<= -50% và >= +50%) để khớp khối C của q5_qa.sql. Luôn đủ 9 nhóm (nhóm rỗng = 0); ẩn cả card khi tổng số cặp đổi giá < 30. ORDER BY nhom_no vì Metabase xếp trục X theo lần xuất hiện đầu tiên.
WITH pairs AS (
    SELECT
        v.days_since_prev_version AS gap_ngay,
        v.price_vnd,
        v.prev_price_vnd,
        (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd AS pct
    FROM analytics.vw_listing_versions v
    JOIN analytics.vw_listing_latest
      ON analytics.vw_listing_latest.listing_id = v.listing_id
    WHERE v.prev_source_name = 'dataset'
      AND v.source_name = 'web'
      AND v.price_vnd > 0
      AND v.prev_price_vnd > 0
      [[AND {{loai_tin}}]]
),
changed AS (
    SELECT
        pct,
        CASE
            WHEN pct <= -0.50 THEN 1
            WHEN pct <= -0.20 THEN 2
            WHEN pct <= -0.10 THEN 3
            WHEN pct <= -0.05 THEN 4
            WHEN pct <= -0.02 THEN 5
            WHEN pct <   0    THEN 6
            WHEN pct <=  0.10 THEN 7
            WHEN pct <   0.50 THEN 8
            ELSE 9
        END AS nhom_no
    FROM pairs
    WHERE price_vnd <> prev_price_vnd
),
bucket (nhom_no, nhom_doi_gia) AS (
    VALUES (1, 'Giảm từ 50% trở lên'), (2, 'Giảm 20 – 50%'), (3, 'Giảm 10 – 20%'),
           (4, 'Giảm 5 – 10%'), (5, 'Giảm 2 – 5%'), (6, 'Giảm dưới 2%'),
           (7, 'Tăng dưới 10%'), (8, 'Tăng 10 – 50%'), (9, 'Tăng từ 50% trở lên')
)
SELECT
    b.nhom_doi_gia,
    COUNT(c.pct) AS so_cap
FROM bucket b
LEFT JOIN changed c USING (nhom_no)
WHERE (SELECT COUNT(*) FROM changed) >= 30
GROUP BY b.nhom_no, b.nhom_doi_gia
ORDER BY b.nhom_no;

-- CARD 5: "Khoảng cách giữa hai lần quan sát" (Table; ẩn median khi số cặp đổi giá của nhóm < 30)
-- so_cap = mọi cặp có cả hai giá > 0 (cặp khác hash vì lý do bất kỳ); KHÔNG tính tỷ lệ đổi giá (mẫu đã lọc theo hash).
WITH pairs AS (
    SELECT
        v.days_since_prev_version AS gap_ngay,
        v.price_vnd,
        v.prev_price_vnd,
        (v.price_vnd - v.prev_price_vnd) / v.prev_price_vnd AS pct
    FROM analytics.vw_listing_versions v
    JOIN analytics.vw_listing_latest
      ON analytics.vw_listing_latest.listing_id = v.listing_id
    WHERE v.prev_source_name = 'dataset'
      AND v.source_name = 'web'
      AND v.price_vnd > 0
      AND v.prev_price_vnd > 0
      [[AND {{loai_tin}}]]
),
grp AS (
    SELECT
        pct, price_vnd, prev_price_vnd,
        CASE WHEN gap_ngay < 30 THEN 1 WHEN gap_ngay < 60 THEN 2 WHEN gap_ngay < 90 THEN 3 ELSE 4 END AS nhom_no
    FROM pairs
)
SELECT
    CASE nhom_no WHEN 1 THEN 'Dưới 30 ngày' WHEN 2 THEN '30 – 59 ngày' WHEN 3 THEN '60 – 89 ngày' ELSE 'Từ 90 ngày' END AS khoang_cach,
    COUNT(*) AS so_cap,
    COUNT(*) FILTER (WHERE price_vnd <> prev_price_vnd) AS so_cap_doi_gia,
    CASE WHEN COUNT(*) FILTER (WHERE price_vnd <> prev_price_vnd) >= 30
         THEN ROUND((100 * percentile_cont(0.5) WITHIN GROUP (ORDER BY pct) FILTER (WHERE price_vnd <> prev_price_vnd))::numeric, 1)
    END AS median_pct_doi_gia
FROM grp
GROUP BY nhom_no
ORDER BY nhom_no;
