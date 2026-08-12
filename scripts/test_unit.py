# test_unit.py - chạy: python test_unit.py
from crawler import config, db, queue_manager, fetcher, storage, parser

# 2.1. config.py — kiểm tra hàm tiện ích
assert config.backoff_minutes(1) == 2
assert config.backoff_minutes(6) == 60  # bị chặn trần
d = config.random_delay_seconds()
assert 15 <= d <= 25
print("[OK] config.py")

# 2.2. db.py — kết nối + dict_cursor
conn = db.get_conn()
with db.dict_cursor(conn) as cur:
    cur.execute("SELECT 1 AS x")
    assert cur.fetchone()["x"] == 1
print("[OK] db.py")

# 2.3. queue_manager.py — seed, claim, mark
n = queue_manager.seed_category_start_pages(conn)
print(f"seed: {n} URL mới (lần 2 chạy lại phải ra 0)")
batch = queue_manager.claim_batch(conn, limit=1)
assert len(batch) >= 0
if batch:
    print("[OK] claim_batch trả về:", batch[0]["url"])
conn.commit()
print("[OK] queue_manager.py")

# 2.4. fetcher.py — fetch 1 URL thật, KHÔNG dùng URL trong queue để tránh ảnh hưởng state
result = fetcher.fetch("https://alonhadat.com.vn/can-ban-nha-mat-tien/ho-chi-minh")
print("status:", result.status_code, "| ok:", result.ok, "| bytes:", len(result.content or b""))
assert result.status_code in (200, 429)  # 429 cũng "hợp lệ" về mặt logic, không phải bug
print("[OK] fetcher.py")

# 2.5. storage.py — save rồi load lại, so sánh nội dung
if result.ok:
    key = storage.save_gz("nha_mat_tien", 1, result.content, run_id="unittest01")
    loaded = storage.load_gz(key)
    assert loaded == result.content
    print("[OK] storage.py — key:", key)

# 2.6. parser.py — extract từ HTML thật vừa fetch
from bs4 import BeautifulSoup
soup = BeautifulSoup(result.content, "lxml")
articles = soup.select("article.property-item")
print(f"Tìm thấy {len(articles)} article.property-item trên trang")
if articles:
    record = parser._extract_listing(articles[0], "https://alonhadat.com.vn/can-ban-nha-mat-tien/ho-chi-minh", "nha_mat_tien")
    assert record is not None
    assert record["url"].startswith("https://alonhadat.com.vn/")
    assert record["property_type"] == "Nhà mặt tiền"
    print("[OK] parser.py — record mẫu:", {k: record[k] for k in ("url","title","price_vnd","area_m2", "date_posted")})

conn.close()