"""
Test: test_bronze_crawler_resume.py
Kiem tra logic cua bronze_crawler_core.run_crawl() BANG CACH GIA LAP toan bo
DB/CDN/S3 trong RAM - KHONG can Postgres/mang that/AWS that. Muc dich la
xac nhan dung hanh vi RESUME va PHAT HIEN PART MOI truoc khi chay tren
production.

Cach chay: python tests/test_bronze_crawler_resume.py
(khong dung framework pytest de don gian hoa, chi can python thuan)
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from bronze_crawler_core import run_crawl  # noqa: E402


# -----------------------------------------------------------------------------
# Cac lop / ham GIA LAP (fake) - thay the cho Postgres/CDN/S3 that
# -----------------------------------------------------------------------------
class InMemoryPartQueueStore:
    """Gia lap bang crawl.dataset_part_queue bang 1 dict trong RAM."""

    def __init__(self):
        self.parts = {}  # part_number -> dict(status=..., ...)

    def seed(self, part_number, status="pending"):
        """Ham tien ich CHI DUNG TRONG TEST de thiet lap trang thai ban dau."""
        self.parts[part_number] = {"status": status}

    # ---- Cac ham bat buoc theo interface PartQueueStore ----
    def get_max_known_part(self):
        return max(self.parts.keys(), default=0)

    def insert_new_parts(self, part_numbers):
        for pn in part_numbers:
            if pn not in self.parts:
                self.parts[pn] = {"status": "pending"}

    def reset_stuck_downloading(self, older_than_minutes):
        count = 0
        for rec in self.parts.values():
            if rec["status"] == "downloading":
                rec["status"] = "pending"
                count += 1
        return count

    def list_processable_parts(self):
        return sorted(pn for pn, rec in self.parts.items() if rec["status"] in ("pending", "failed"))

    def claim_part(self, part_number, run_id):
        rec = self.parts.get(part_number)
        if rec is None or rec["status"] not in ("pending", "failed"):
            return False
        rec["status"] = "downloading"
        rec["claimed_by_run_id"] = run_id
        return True

    def mark_success(self, part_number, **kwargs):
        rec = self.parts[part_number]
        rec["status"] = "success"
        rec.update(kwargs)

    def mark_failed(self, part_number, error):
        rec = self.parts[part_number]
        rec["status"] = "failed"
        rec["last_error"] = error


def make_fake_cdn(available_parts: set):
    """Gia lap CDN: chi cac part_number trong available_parts moi 'ton tai'."""
    def part_exists_fn(pn):
        return pn in available_parts
    return part_exists_fn


def make_fake_downloader(fail_parts: set = frozenset()):
    """
    Gia lap tai file. fail_parts la tap part_number se LUON loi (dung de test
    retry/circuit breaker). Ghi lai danh sach da "goi download" de assert.
    """
    calls = []

    def download_fn(pn):
        calls.append(pn)
        if pn in fail_parts:
            raise RuntimeError(f"gia lap loi tai part {pn}")
        return f"noi-dung-gia-lap-part-{pn}".encode("utf-8")

    return download_fn, calls


def fake_verify_fn(part_number, content):
    return {"actual_rows": 10000}


def make_fake_uploader():
    """Gia lap S3: luu noi dung vao dict trong RAM thay vi goi boto3 that."""
    uploaded = {}

    def upload_fn(pn, content):
        s3_key = f"bronze/2026-08-15/crawl-2/part{pn}.parquet"
        uploaded[pn] = content
        return {"s3_key": s3_key, "sha256": f"fake-sha-{pn}"}

    return upload_fn, uploaded


def no_sleep(_seconds):
    """Thay the time.sleep that trong test de chay nhanh, khong can cho that."""
    pass


# -----------------------------------------------------------------------------
# TEST 1: dung theo dung kich ban ban yeu cau -
# "Truoc do nguon co 10 file, da down het 10 file. Lan crawl sau kiem tra
#  xuat hien n file moi. Down tiep n file do."
# -----------------------------------------------------------------------------
def test_resume_khi_co_part_moi_xuat_hien():
    store = InMemoryPartQueueStore()
    for i in range(1, 11):
        store.seed(i, status="success")  # gia lap: 10 file dau da down xong tu truoc

    # Nguon "that" gio day co 13 part (part 11, 12, 13 MOI xuat hien them)
    part_exists_fn = make_fake_cdn(available_parts=set(range(1, 14)))
    download_fn, download_calls = make_fake_downloader()
    upload_fn, uploaded_store = make_fake_uploader()

    result = run_crawl(
        store=store,
        part_exists_fn=part_exists_fn,
        download_fn=download_fn,
        verify_fn=fake_verify_fn,
        upload_fn=upload_fn,
        run_id="test-run-resume",
        sleep_fn=no_sleep,
    )

    # --- Kiem tra: chi phat hien va tai DUNG 3 file moi (11, 12, 13) ---
    assert result.new_parts_discovered == [11, 12, 13], \
        f"Ky vong phat hien [11,12,13], thuc te {result.new_parts_discovered}"
    assert download_calls == [11, 12, 13], \
        f"Ky vong CHI goi download cho [11,12,13] (khong dong lai 1-10), thuc te {download_calls}"
    assert result.succeeded == [11, 12, 13]
    assert result.failed == []
    assert not result.circuit_breaker_tripped

    # --- Kiem tra: 10 file cu KHONG bi dong lai, van giu status 'success' ---
    for pn in range(1, 11):
        assert store.parts[pn]["status"] == "success", f"Part {pn} khong duoc dong lai nhung status sai"

    # --- Kiem tra: 3 file moi da chuyen sang 'success' va co mat trong "S3" gia lap ---
    for pn in [11, 12, 13]:
        assert store.parts[pn]["status"] == "success"
        assert pn in uploaded_store

    print("PASS: test_resume_khi_co_part_moi_xuat_hien")


# -----------------------------------------------------------------------------
# TEST 2: circuit breaker dung SOM khi loi LIEN TIEP vuot nguong,
# nhung KHONG anh huong cac part DA xu ly THANH CONG truoc do.
# -----------------------------------------------------------------------------
def test_circuit_breaker_dung_som_khi_loi_lien_tiep():
    store = InMemoryPartQueueStore()
    for i in range(1, 11):
        store.seed(i, status="pending")

    part_exists_fn = make_fake_cdn(available_parts=set())  # khong co part moi trong test nay
    # Part 3, 4, 5 se loi LIEN TIEP (dung 3 = nguong circuit breaker mac dinh trong test)
    download_fn, download_calls = make_fake_downloader(fail_parts={3, 4, 5})
    upload_fn, uploaded_store = make_fake_uploader()

    result = run_crawl(
        store=store,
        part_exists_fn=part_exists_fn,
        download_fn=download_fn,
        verify_fn=fake_verify_fn,
        upload_fn=upload_fn,
        run_id="test-run-circuit-breaker",
        consecutive_failure_limit=3,
        per_part_retries=0,  # tat retry de test nhanh & de doan (khong phu thuoc backoff)
        sleep_fn=no_sleep,
    )

    assert result.succeeded == [1, 2], f"Ky vong [1,2] thanh cong truoc khi gap loi, thuc te {result.succeeded}"
    assert result.failed == [3, 4, 5], f"Ky vong [3,4,5] loi lien tiep, thuc te {result.failed}"
    assert result.circuit_breaker_tripped is True
    assert result.stopped_at_part == 5

    # Part 6..10 CHUA duoc xu ly (dung som) -> van con 'pending' cho lan chay sau
    for pn in range(6, 11):
        assert store.parts[pn]["status"] == "pending", f"Part {pn} le ra chua duoc xu ly"

    # Part 1, 2 (thanh cong truoc do) khong bi anh huong boi circuit breaker
    assert store.parts[1]["status"] == "success"
    assert store.parts[2]["status"] == "success"

    print("PASS: test_circuit_breaker_dung_som_khi_loi_lien_tiep")


# -----------------------------------------------------------------------------
# TEST 3: 1 part loi (khong du de kich hoat circuit breaker) KHONG chan cac
# part sau no trong CUNG 1 luot chay - dung theo dung gia dinh hien tai.
# -----------------------------------------------------------------------------
def test_mot_loi_don_le_khong_chan_cac_part_sau():
    store = InMemoryPartQueueStore()
    for i in range(1, 6):
        store.seed(i, status="pending")

    part_exists_fn = make_fake_cdn(available_parts=set())
    # Chi part 3 loi (khong lien tiep voi part nao khac) -> KHONG du nguong circuit breaker (3)
    download_fn, download_calls = make_fake_downloader(fail_parts={3})
    upload_fn, uploaded_store = make_fake_uploader()

    result = run_crawl(
        store=store,
        part_exists_fn=part_exists_fn,
        download_fn=download_fn,
        verify_fn=fake_verify_fn,
        upload_fn=upload_fn,
        run_id="test-run-single-failure",
        consecutive_failure_limit=3,
        per_part_retries=0,
        sleep_fn=no_sleep,
    )

    # Tat ca 5 part deu duoc XU LY (khong bi chan boi part 3 loi)
    assert download_calls == [1, 2, 3, 4, 5], download_calls
    assert result.succeeded == [1, 2, 4, 5]
    assert result.failed == [3]
    assert not result.circuit_breaker_tripped

    print("PASS: test_mot_loi_don_le_khong_chan_cac_part_sau")


# -----------------------------------------------------------------------------
# TEST 4: tham so max_parts_per_run (danh dau "[CHI DUNG DE TEST]" trong core)
# gioi han dung so part xu ly trong 1 LAN GOI, phan con lai KHONG bi anh
# huong (van 'pending'), lan chay sau (khong truyen gioi han) se xu ly tiep.
# -----------------------------------------------------------------------------
def test_max_parts_per_run_gioi_han_so_part_xu_ly():
    store = InMemoryPartQueueStore()
    for i in range(1, 8):
        store.seed(i, status="pending")

    part_exists_fn = make_fake_cdn(available_parts=set())
    download_fn, download_calls = make_fake_downloader()
    upload_fn, uploaded_store = make_fake_uploader()

    # --- Lan chay 1: gioi han chi xu ly 2 part ---
    result1 = run_crawl(
        store=store,
        part_exists_fn=part_exists_fn,
        download_fn=download_fn,
        verify_fn=fake_verify_fn,
        upload_fn=upload_fn,
        run_id="test-run-limit-1",
        sleep_fn=no_sleep,
        max_parts_per_run=2,  # <-- CHI xu ly 2 part dau tien (1, 2)
    )

    assert download_calls == [1, 2], f"Lan 1 ky vong chi tai [1,2], thuc te {download_calls}"
    assert result1.succeeded == [1, 2]
    for pn in range(3, 8):
        assert store.parts[pn]["status"] == "pending", f"Part {pn} khong duoc dong nhung phai van 'pending'"

    # --- Lan chay 2: KHONG truyen gioi han -> tu dong xu ly tiep phan con lai (3..7) ---
    result2 = run_crawl(
        store=store,
        part_exists_fn=part_exists_fn,
        download_fn=download_fn,
        verify_fn=fake_verify_fn,
        upload_fn=upload_fn,
        run_id="test-run-limit-2",
        sleep_fn=no_sleep,
        # khong truyen max_parts_per_run -> mac dinh None -> khong gioi han
    )

    assert download_calls == [1, 2, 3, 4, 5, 6, 7], download_calls  # gop ca 2 lan goi
    assert result2.succeeded == [3, 4, 5, 6, 7]
    for pn in range(1, 8):
        assert store.parts[pn]["status"] == "success"

    print("PASS: test_max_parts_per_run_gioi_han_so_part_xu_ly")


if __name__ == "__main__":
    test_resume_khi_co_part_moi_xuat_hien()
    test_circuit_breaker_dung_som_khi_loi_lien_tiep()
    test_mot_loi_don_le_khong_chan_cac_part_sau()
    test_max_parts_per_run_gioi_han_so_part_xu_ly()
    print("\nTAT CA TEST DEU PASS.")
