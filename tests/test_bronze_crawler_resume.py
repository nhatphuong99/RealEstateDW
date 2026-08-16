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

    def seed(self, part_number, status="pending", **extra_fields):
        """Ham tien ich CHI DUNG TRONG TEST de thiet lap trang thai ban dau."""
        rec = {"status": status}
        rec.update(extra_fields)
        self.parts[part_number] = rec

    # ---- Cac ham bat buoc theo interface PartQueueStore ----
    def get_max_known_part(self):
        return max(self.parts.keys(), default=0)

    def get_known_part_numbers(self):
        return set(self.parts.keys())

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

    def list_success_parts_with_keys(self):
        return [
            (pn, rec.get("s3_key"))
            for pn, rec in self.parts.items()
            if rec["status"] == "success"
        ]


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


# -----------------------------------------------------------------------------
# TEST 5: mo phong DUNG kich ban ban dua ra - nguon co LO HONG RAI RAC o giua
# khoang DA BIET (part3, part23, part24, part27-29 bi thieu du da biet toi part30).
# scan_and_fill_gaps() phai tu phat hien va bo sung TAT CA cac lo hong nay,
# bat ke vi tri, khong chi o gan bien.
# -----------------------------------------------------------------------------
def test_gap_scan_phat_hien_va_bo_sung_part_bi_thieu_o_giua():
    store = InMemoryPartQueueStore()
    # Da "biet" (co trong queue) toi part 30, NHUNG cac so nay CHUA TUNG duoc
    # dua vao queue: 3, 23, 24, 27, 28, 29 (dung theo vi du ban dua ra)
    known_now = [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                 17, 18, 19, 20, 21, 22, 25, 26, 30]
    for pn in known_now:
        store.seed(pn, status="success")  # gia lap: da tai thanh cong tu truoc

    # Nguon THAT co DAY DU part 1..30 (bao gom ca cac so dang bi "thieu" trong queue)
    part_exists_fn = make_fake_cdn(available_parts=set(range(1, 31)))
    download_fn, download_calls = make_fake_downloader()
    upload_fn, uploaded_store = make_fake_uploader()

    result = run_crawl(
        store=store,
        part_exists_fn=part_exists_fn,
        download_fn=download_fn,
        verify_fn=fake_verify_fn,
        upload_fn=upload_fn,
        run_id="test-run-gap-scan",
        sleep_fn=no_sleep,
    )

    expected_gaps = [3, 23, 24, 27, 28, 29]

    assert sorted(result.gap_parts_found) == expected_gaps, \
        f"Ky vong phat hien lo hong {expected_gaps}, thuc te {sorted(result.gap_parts_found)}"
    assert result.new_parts_discovered == [], \
        "Khong co part nao VUOT BIEN 30 trong test nay (nguon chi co toi 30)"
    assert sorted(download_calls) == expected_gaps, \
        f"Chi duoc tai DUNG cac part bi thieu, khong dong lai part da 'success', thuc te {download_calls}"
    assert sorted(result.succeeded) == expected_gaps

    for pn in expected_gaps:
        assert store.parts[pn]["status"] == "success", f"Part {pn} phai duoc bo sung thanh cong"
    # Cac part da 'success' tu truoc KHONG bi dong lai
    for pn in known_now:
        assert store.parts[pn]["status"] == "success"

    print("PASS: test_gap_scan_phat_hien_va_bo_sung_part_bi_thieu_o_giua")


# -----------------------------------------------------------------------------
# TEST 6: discover_new_parts KHONG dung ngay khi gap 1 lo hong nho GAN BIEN -
# ma van tim tiep duoc cac part MOI o xa hon, nho miss_tolerance.
# -----------------------------------------------------------------------------
def test_discover_new_parts_vuot_qua_lo_hong_nho_gan_bien():
    store = InMemoryPartQueueStore()
    for i in range(1, 11):
        store.seed(i, status="success")  # da biet toi part 10

    # Nguon that: part 11 BI THIEU (lo hong ngay sau bien), nhung part 12, 13
    # van TON TAI. Neu dung logic cu (dung khi gap 1 so thieu dau tien), se
    # KHONG BAO GIO tim thay part 12, 13.
    part_exists_fn = make_fake_cdn(available_parts={1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13})
    download_fn, download_calls = make_fake_downloader()
    upload_fn, uploaded_store = make_fake_uploader()

    result = run_crawl(
        store=store,
        part_exists_fn=part_exists_fn,
        download_fn=download_fn,
        verify_fn=fake_verify_fn,
        upload_fn=upload_fn,
        run_id="test-run-miss-tolerance",
        sleep_fn=no_sleep,
        miss_tolerance=5,
    )

    # Van phat hien duoc 12, 13 du part 11 bi thieu ngay truoc do
    assert result.new_parts_discovered == [12, 13], \
        f"Ky vong van tim thay [12,13] du part 11 thieu, thuc te {result.new_parts_discovered}"
    assert result.succeeded == [12, 13]
    # Part 11 KHONG duoc them vao queue (vi thuc su khong ton tai tren nguon)
    assert 11 not in store.parts

    print("PASS: test_discover_new_parts_vuot_qua_lo_hong_nho_gan_bien")


# -----------------------------------------------------------------------------
# TEST 7: phat hien truong hop AI DO XOA FILE TREN S3 truc tiep (khong phai
# do crawler gay ra), trong khi Postgres van con ghi status='success'.
# reconcile_missing_storage_objects() phai phat hien va tai lai DUNG part do.
# -----------------------------------------------------------------------------
def test_reconcile_phat_hien_file_bi_xoa_tren_s3():
    store = InMemoryPartQueueStore()
    for i in range(1, 6):
        store.seed(i, status="success", s3_key=f"bronze/2026-08-15/crawl-1/part{i}.parquet")

    # Gia lap: ai do da xoa THU CONG file cua part 3 tren S3 (VD tren console),
    # nhung KHONG ai cap nhat lai Postgres -> Postgres van ghi 'success' sai su that.
    deleted_s3_keys = {"bronze/2026-08-15/crawl-1/part3.parquet"}

    def s3_object_exists_fn(key):
        return key not in deleted_s3_keys

    part_exists_fn = make_fake_cdn(available_parts=set(range(1, 6)))
    download_fn, download_calls = make_fake_downloader()
    upload_fn, uploaded_store = make_fake_uploader()

    result = run_crawl(
        store=store,
        part_exists_fn=part_exists_fn,
        download_fn=download_fn,
        verify_fn=fake_verify_fn,
        upload_fn=upload_fn,
        s3_object_exists_fn=s3_object_exists_fn,
        run_id="test-run-reconcile",
        sleep_fn=no_sleep,
    )

    assert result.reconciled_missing_parts == [3], \
        f"Ky vong phat hien [3] bi mat file S3, thuc te {result.reconciled_missing_parts}"
    assert download_calls == [3], \
        f"Chi duoc tai lai DUNG part 3 (khong dong lai 1,2,4,5), thuc te {download_calls}"
    assert result.succeeded == [3]
    assert store.parts[3]["status"] == "success"  # da duoc tai lai va thanh cong

    # Cac part khac (file S3 van con nguyen) KHONG bi dong lai
    for pn in [1, 2, 4, 5]:
        assert pn not in download_calls
        assert store.parts[pn]["status"] == "success"

    print("PASS: test_reconcile_phat_hien_file_bi_xoa_tren_s3")


# -----------------------------------------------------------------------------
# TEST 8: khong truyen s3_object_exists_fn (VD: cac test khac o tren) thi
# BO QUA hoan toan buoc reconcile, khong loi, khong anh huong hanh vi cu.
# -----------------------------------------------------------------------------
def test_khong_truyen_s3_object_exists_fn_thi_bo_qua_reconcile():
    store = InMemoryPartQueueStore()
    store.seed(1, status="success", s3_key="bronze/x/part1.parquet")

    part_exists_fn = make_fake_cdn(available_parts=set())
    download_fn, download_calls = make_fake_downloader()
    upload_fn, uploaded_store = make_fake_uploader()

    result = run_crawl(
        store=store,
        part_exists_fn=part_exists_fn,
        download_fn=download_fn,
        verify_fn=fake_verify_fn,
        upload_fn=upload_fn,
        # KHONG truyen s3_object_exists_fn
        run_id="test-run-no-reconcile-fn",
        sleep_fn=no_sleep,
    )

    assert result.reconciled_missing_parts == []
    assert download_calls == []
    assert store.parts[1]["status"] == "success"  # khong bi dong gi ca

    print("PASS: test_khong_truyen_s3_object_exists_fn_thi_bo_qua_reconcile")


if __name__ == "__main__":
    test_resume_khi_co_part_moi_xuat_hien()
    test_circuit_breaker_dung_som_khi_loi_lien_tiep()
    test_mot_loi_don_le_khong_chan_cac_part_sau()
    test_max_parts_per_run_gioi_han_so_part_xu_ly()
    test_gap_scan_phat_hien_va_bo_sung_part_bi_thieu_o_giua()
    test_discover_new_parts_vuot_qua_lo_hong_nho_gan_bien()
    test_reconcile_phat_hien_file_bi_xoa_tren_s3()
    test_khong_truyen_s3_object_exists_fn_thi_bo_qua_reconcile()
    print("\nTAT CA TEST DEU PASS.")
