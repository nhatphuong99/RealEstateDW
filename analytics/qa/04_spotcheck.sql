-- analytics/qa/04_spotcheck.sql
-- Mục E của checklist: chọn mẫu spot-check và đối chiếu từng dòng. Chỉ đọc.
-- Mẫu chọn theo MD5(id) nên lặp lại được (cùng dữ liệu thì cùng mẫu).
-- QA được phép đọc silver.listing_history (lấy URL, title, area_raw); view analytics không phụ thuộc Silver.

-- ===== E1a. 2 listing đảo thứ tự, TRÙNG hash (dòng web bị loại khỏi vw_listing_versions) =====
SELECT f.listing_id
FROM gold.vw_fact_report AS f
LEFT JOIN analytics.vw_listing_versions AS v ON v.listing_key = f.listing_key
WHERE v.listing_key IS NULL
GROUP BY f.listing_id
ORDER BY MD5(f.listing_id::TEXT)
LIMIT 2;

-- ===== E1b. 2 listing đảo thứ tự nhưng ĐỔI hash (cả hai dòng đều được giữ) =====
SELECT l.listing_id
FROM analytics.vw_listing_latest AS l
JOIN gold.vw_fact_report AS g ON g.listing_key = l.listing_key
WHERE NOT g.is_current
  AND NOT EXISTS (
      SELECT 1
      FROM gold.vw_fact_report AS f
      LEFT JOIN analytics.vw_listing_versions AS v ON v.listing_key = f.listing_key
      WHERE f.listing_id = l.listing_id AND v.listing_key IS NULL
  )
ORDER BY MD5(l.listing_id::TEXT)
LIMIT 2;

-- ===== E2. 2 listing đổi giá thật (1 giảm, 1 tăng), cặp dataset -> web =====
(SELECT 'giảm' AS nhom, listing_id, price_change_pct
 FROM analytics.vw_listing_versions
 WHERE is_price_change AND is_cross_source AND price_change_pct < 0
 ORDER BY MD5(listing_id::TEXT) LIMIT 1)
UNION ALL
(SELECT 'tăng', listing_id, price_change_pct
 FROM analytics.vw_listing_versions
 WHERE is_price_change AND is_cross_source AND price_change_pct > 0
 ORDER BY MD5(listing_id::TEXT) LIMIT 1);

-- ===== CHI TIẾT 1 LISTING (dùng cho E1, E2): đổi 0 thành listing_id cần kiểm =====
SELECT
    f.listing_key,
    f.source_name,
    f.valid_from,
    f.valid_to,
    f.is_current,
    f.posted_date,
    f.price_vnd,
    f.is_expired,
    s.row_hash AS row_hash_silver,
    (v.listing_key IS NOT NULL) AS in_versions,
    v.version_no,
    v.prev_price_vnd,
    v.price_change_pct,
    v.days_since_prev_version,
    (l.listing_key IS NOT NULL) AS is_latest,
    s.listing_url
FROM gold.vw_fact_report AS f
LEFT JOIN analytics.vw_listing_versions AS v ON v.listing_key = f.listing_key
LEFT JOIN analytics.vw_listing_latest AS l ON l.listing_key = f.listing_key
LEFT JOIN silver.listing_history AS s ON s.listing_key = f.listing_key
WHERE f.listing_id = 16871015   -- đổi 0 thành listing_id
ORDER BY f.valid_from, f.listing_key;

-- ===== E3. 2 listing area_suspect: 1 nhóm tỷ lệ [8,12), 1 nhóm [80,120) =====
(SELECT 'ratio 8-12' AS nhom, l.listing_key, l.area_m2, l.length_m, l.width_m,
        ROUND(l.area_ratio, 2) AS area_ratio, s.area_raw, s.title, s.listing_url
 FROM analytics.vw_listing_latest AS l
 JOIN silver.listing_history AS s ON s.listing_key = l.listing_key
 WHERE l.area_suspect AND l.area_ratio >= 8 AND l.area_ratio < 12
 ORDER BY MD5(l.listing_key::TEXT) LIMIT 1)
UNION ALL
(SELECT 'ratio 80-120', l.listing_key, l.area_m2, l.length_m, l.width_m,
        ROUND(l.area_ratio, 2), s.area_raw, s.title, s.listing_url
 FROM analytics.vw_listing_latest AS l
 JOIN silver.listing_history AS s ON s.listing_key = l.listing_key
 WHERE l.area_suspect AND l.area_ratio >= 80 AND l.area_ratio < 120
 ORDER BY MD5(l.listing_key::TEXT) LIMIT 1);

-- ===== E4. Thuê tòa nhà thương mại: 2 tin được giữ ở phân khúc riêng, 2 tin bị loại bởi R4/R7 =====
(SELECT 'giữ (phân khúc tòa nhà)' AS nhom, l.listing_key, l.price_vnd, l.area_m2,
        l.price_per_m2_vnd, l.exclude_reason, s.title, s.listing_url
 FROM analytics.vw_listing_latest AS l
 JOIN silver.listing_history AS s ON s.listing_key = l.listing_key
 WHERE l.is_commercial_building AND l.exclude_reason IS NULL AND l.is_usable
 ORDER BY MD5(l.listing_key::TEXT) LIMIT 2)
UNION ALL
(SELECT 'bị loại', l.listing_key, l.price_vnd, l.area_m2,
        l.price_per_m2_vnd, l.exclude_reason, s.title, s.listing_url
 FROM analytics.vw_listing_latest AS l
 JOIN silver.listing_history AS s ON s.listing_key = l.listing_key
 WHERE l.is_commercial_building AND l.exclude_reason IS NOT NULL AND l.is_usable
 ORDER BY MD5(l.listing_key::TEXT) LIMIT 2);

-- ===== E5a. Múi giờ của phiên làm việc (kỳ vọng Asia/Ho_Chi_Minh) =====
SHOW timezone;

-- ===== E5b. Đếm trực tiếp theo khoảng thời gian +07, độc lập với AT TIME ZONE; kỳ vọng 0 dòng =====
SELECT c.crawl_date, c.run_count AS view_run_count, d.n AS direct_count
FROM analytics.vw_crawl_coverage AS c
CROSS JOIN LATERAL (
    SELECT COUNT(*) AS n
    FROM pipeline.run_state AS r
    WHERE r.started_at >= (c.crawl_date::TEXT || ' 00:00:00+07')::timestamptz
      AND r.started_at <  (c.crawl_date::TEXT || ' 00:00:00+07')::timestamptz + INTERVAL '1 day'
) AS d
WHERE c.run_count <> d.n;

-- ===== E5c. Các run sát nửa đêm (xem tay: ngày địa phương có đúng không) =====
SELECT r.run_id,
       r.started_at AT TIME ZONE 'Asia/Ho_Chi_Minh' AS started_local,
       (r.started_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS crawl_date_local,
       r.started_at AT TIME ZONE 'UTC' AS started_utc
FROM pipeline.run_state AS r
WHERE (r.started_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::time >= '23:30'
   OR (r.started_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::time < '00:30'
ORDER BY r.started_at
LIMIT 10;

-- ===== E6. Các run recovered/incomplete của ngày 2026-08-31 (4 run theo D5b) =====
SELECT run_id, started_at, ended_at, ended_at - started_at AS duration,
       stopped_reason, detail_pages_done
FROM pipeline.run_state
WHERE started_at >= '2026-08-31 00:00:00+07' AND started_at < '2026-09-01 00:00:00+07'
  AND stopped_reason IN ('recovered', 'incomplete')
ORDER BY started_at;
