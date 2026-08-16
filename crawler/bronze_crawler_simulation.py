"""
Module: bronze_crawler_simulation.py

CHI DUNG CHO MUC DICH DEMO/HOC TAP (mo phong "du lieu moi phat sinh dan theo
thoi gian" tren 1 nguon von di CO DINH voi 77 part) - KHONG duoc dung trong
luong production that.

Ly do tach rieng khoi bronze_crawler_io.py: bronze_crawler_io.py phai la
IO THAT, khong pha tron logic mo phong vao, de tranh nham lan "day la code
production" khi doc lai sau nay. Module nay chi la 1 LOP BOC (wrapper) rat
mong quanh ham kiem tra ton tai that.
"""

from __future__ import annotations

from typing import Callable


def make_capped_part_exists_fn(
    real_part_exists_fn: Callable[[int], bool],
    max_visible_part: int,
) -> Callable[[int], bool]:
    """
    Boc 1 ham kiem tra ton tai THAT (VD part_exists_on_source trong
    bronze_crawler_io.py) lai, GIOI HAN tam nhin toi da o max_visible_part.

    - candidate <= max_visible_part: goi ham that binh thuong (VAN CO THE
      raise loi that, VD PartCheckError - KHONG nuot loi, chi gioi han pham
      vi duoc phep "nhin thay").
    - candidate > max_visible_part: tra ve False NGAY, KHONG goi mang - coi
      nhu phan du lieu nay "chua duoc cong bo" o thoi diem mo phong hien tai.

    Dung ham nay de wrap part_exists_fn truyen vao run_crawl() trong cac
    DAG demo - KHONG dung trong DAG production that (o do phai truyen thang
    part_exists_on_source, khong wrap, de crawler thay dung tinh trang that
    cua nguon).
    """
    def capped_fn(candidate: int) -> bool:
        if candidate > max_visible_part:
            return False
        return real_part_exists_fn(candidate)

    return capped_fn
