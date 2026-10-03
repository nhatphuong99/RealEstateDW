-- analytics/sql/00_inspect_columns.sql
-- Chỉ đọc. Chạy trên DataGrip trước/sau khi chạy 01_create_base_views.sql.
-- Cột Gold/pipeline đã đối chiếu với sql/schema_full.sql; file này kiểm giá trị thật trong dữ liệu.

-- (1) Giá trị phân loại để khớp CASE trong view (kỳ vọng: 'Cần bán', 'Cho thuê')
SELECT listing_type, COUNT(*) AS n FROM gold.vw_fact_report GROUP BY 1 ORDER BY 2 DESC;
SELECT province_old, COUNT(*) AS n FROM gold.vw_fact_report GROUP BY 1 ORDER BY 2 DESC;
SELECT source_name, COUNT(*) AS n FROM gold.vw_fact_report GROUP BY 1 ORDER BY 2 DESC;

-- (2) stopped_reason của DAG 2 (xem có giá trị 'recovered'/hết proxy để quyết định lọc run_count)
SELECT stopped_reason, COUNT(*) AS n FROM pipeline.run_state GROUP BY 1 ORDER BY 2 DESC;

-- (3) Object nào đang phụ thuộc vào gold.vw_fact_report (chạy lại sau khi tạo analytics)
SELECT DISTINCT dep_ns.nspname AS dependent_schema, dep_cls.relname AS dependent_view
FROM pg_depend AS d
JOIN pg_rewrite AS r ON r.oid = d.objid
JOIN pg_class AS dep_cls ON dep_cls.oid = r.ev_class
JOIN pg_namespace AS dep_ns ON dep_ns.oid = dep_cls.relnamespace
WHERE d.refobjid = 'gold.vw_fact_report'::regclass
  AND dep_cls.oid <> d.refobjid
ORDER BY 1, 2;
