"""analytics/qa/recompute_q2_cell.py

Tính lại premium mặt tiền so với hẻm của MỘT ô Q2 (phường × nhóm diện tích) bằng pandas,
để so với card Q2 và SQL. Chạy tay từ thư mục gốc project, không đưa vào pipeline:
    python analytics/qa/recompute_q2_cell.py
Muốn kiểm ô khác thì sửa 2 hằng số WARD và AREA_BAND ngay trong file này
(không truyền tiếng Việt qua dòng lệnh, tránh lỗi mã hóa trên Windows).
Đọc analytics/qa/_out/latest_ready.csv (xuất từ analytics.vw_listing_latest, is_analysis_ready).
"""
import argparse
import sys

import pandas as pd

WARD = "Phường Khánh Hội"
AREA_BAND = "4_ge_100"
TYPE_FRONT = "Nhà mặt tiền"
TYPE_ALLEY = "Nhà trong hẻm"
MIN_N = 30


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--raw", default="analytics/qa/_out/latest_ready.csv")
    args = ap.parse_args()

    # Đọc mọi cột dạng text để giữ nguyên chuỗi, chỉ ép kiểu số cho giá/m²
    df = pd.read_csv(args.raw, dtype=str, keep_default_na=False)
    df["price_per_m2_vnd"] = pd.to_numeric(df["price_per_m2_vnd"])

    cell = df[
        (df["deal_type"] == "sale")
        & (df["ward_new"] == WARD)
        & (df["area_band"] == AREA_BAND)
        & (df["property_type_name"].isin([TYPE_FRONT, TYPE_ALLEY]))
    ]
    stats = cell.groupby("property_type_name")["price_per_m2_vnd"].agg(n="count", median="median")
    if set(stats.index) != {TYPE_FRONT, TYPE_ALLEY}:
        print("Ô không đủ cả hai loại hình, kiểm lại WARD và AREA_BAND:", list(stats.index))
        return 1

    for name, row in stats.iterrows():
        print(f"{name}: n={int(row['n'])}, median={row['median'] / 1e6:.2f} triệu/m²")
    premium = stats.loc[TYPE_FRONT, "median"] / stats.loc[TYPE_ALLEY, "median"] - 1
    print(f"Premium = {100 * premium:.1f}%")
    enough = bool((stats["n"] >= MIN_N).all())
    print("Đủ mẫu (cả hai nhóm n >= 30):", "CÓ" if enough else "KHÔNG")
    return 0


if __name__ == "__main__":
    sys.exit(main())
