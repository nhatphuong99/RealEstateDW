"""
Test: test_bronze_crawler_simulation.py
Kiem tra make_capped_part_exists_fn() trong bronze_crawler_simulation.py -
CHI la 1 lop boc mong, nhung can dam bao dung 2 tinh chat quan trong:
1) Che dung hoan toan cac candidate > max_visible_part (khong goi ham that).
2) KHONG nuot loi cua ham that doi voi cac candidate <= max_visible_part
   (vi day la loi THAT CAN duoc biet, khong phai "khong ton tai").

Cach chay: python tests/test_bronze_crawler_simulation.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from bronze_crawler_simulation import make_capped_part_exists_fn  # noqa: E402


def test_capped_fn_che_dung_candidate_vuot_nguong():
    real_calls = []

    def real_fn(pn):
        real_calls.append(pn)
        return True  # gia lap: nguon that co TAT CA moi so (ke ca vuot nguong)

    capped_fn = make_capped_part_exists_fn(real_fn, max_visible_part=55)

    assert capped_fn(50) is True
    assert capped_fn(55) is True   # bang dung nguong -> van duoc phep
    assert capped_fn(56) is False  # vuot nguong -> False, KHONG goi ham that
    assert capped_fn(100) is False

    # Chi 2 candidate <= nguong moi thuc su goi ham that (50, 55)
    assert real_calls == [50, 55], f"Ky vong chi goi real_fn cho [50,55], thuc te {real_calls}"

    print("PASS: test_capped_fn_che_dung_candidate_vuot_nguong")


def test_capped_fn_khong_nuot_loi_that():
    class FakeError(Exception):
        pass

    def real_fn_raises(pn):
        raise FakeError(f"loi gia lap cho part {pn}")

    capped_fn = make_capped_part_exists_fn(real_fn_raises, max_visible_part=60)

    # Candidate trong nguong -> loi PHAI duoc nem ra ngoai (khong duoc nuot)
    try:
        capped_fn(10)
        raise AssertionError("Le ra phai raise FakeError nhung khong thay")
    except FakeError:
        pass  # dung nhu ky vong

    # Candidate VUOT nguong -> tra ve False NGAY, KHONG goi real_fn -> KHONG loi
    assert capped_fn(61) is False

    print("PASS: test_capped_fn_khong_nuot_loi_that")


if __name__ == "__main__":
    test_capped_fn_che_dung_candidate_vuot_nguong()
    test_capped_fn_khong_nuot_loi_that()
    print("\nTAT CA TEST DEU PASS.")
