import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import psycopg2
from datetime import date

from bronze_crawler_core import run_crawl
from bronze_crawler_postgres_store import PostgresPartQueueStore, DW_DSN
from bronze_crawler_io import part_exists_on_source, download_part, verify_part, upload_part

# --- Tao 1 dong crawl_run THAT (khong dung id gia nua, vi co FOREIGN KEY) ---
with psycopg2.connect(DW_DSN) as conn, conn.cursor() as cur:
    cur.execute(
        """
        INSERT INTO crawl.crawl_run (run_date, run_no, status)
        VALUES (%s, 2, 'running')
        ON CONFLICT (run_date, run_no) DO UPDATE SET status = 'running'
        RETURNING id;
        """,
        (date.today(),),
    )
    run_id = cur.fetchone()[0]
    conn.commit()

print(f"[SMOKE TEST] Dung crawl_run id that: {run_id}")

store = PostgresPartQueueStore()
store.insert_new_parts([5, 6, 7])  # CHI seed 2 part de test, khong dam ca 77

result = run_crawl(
    store=store,
    part_exists_fn=part_exists_on_source,
    download_fn=download_part,
    verify_fn=verify_part,
    upload_fn=lambda pn, content: upload_part(pn, content, load_date=str(date.today()), crawl_no=1),
    run_id=run_id,   # <-- dung id THAT, khong con la 999
    consecutive_failure_limit=3,
)
print("Thanh cong:", result.succeeded)
print("That bai:", result.failed)

# Cap nhat lai crawl_run cho gon (khong bat buoc, chi de sach du lieu test)
with psycopg2.connect(DW_DSN) as conn, conn.cursor() as cur:
    cur.execute(
        "UPDATE crawl.crawl_run SET status='completed', finished_at=now() WHERE id=%s;",
        (run_id,),
    )
    conn.commit()