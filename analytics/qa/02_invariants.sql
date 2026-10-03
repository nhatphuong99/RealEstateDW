-- analytics/qa/02_invariants.sql
-- Bất biến của 3 view nền. Kỳ vọng: MỖI truy vấn trả 0 dòng. Chỉ đọc.

-- (1) listing trùng ở bản mới nhất
SELECT listing_id FROM analytics.vw_listing_latest GROUP BY 1 HAVING COUNT(*) > 1;

-- (2) listing có hai dòng cùng MAX(valid_from) (chọn bản mới nhất sẽ tùy ý)
SELECT f.listing_id, f.valid_from, COUNT(*) AS n_rows
FROM gold.vw_fact_report AS f
JOIN (SELECT listing_id, MAX(valid_from) AS max_valid_from
      FROM gold.vw_fact_report GROUP BY listing_id) AS m
  ON m.listing_id = f.listing_id AND m.max_valid_from = f.valid_from
GROUP BY f.listing_id, f.valid_from
HAVING COUNT(*) > 1;

-- (3) hai version liên tiếp cùng hash còn sót
SELECT listing_id FROM (
    SELECT listing_id, row_hash,
           LAG(row_hash) OVER (PARTITION BY listing_id ORDER BY version_no) AS prev_hash
    FROM analytics.vw_listing_versions
) AS x
WHERE row_hash = prev_hash;

-- (4) is_analysis_ready không được NULL
SELECT listing_id FROM analytics.vw_listing_latest WHERE is_analysis_ready IS NULL;
