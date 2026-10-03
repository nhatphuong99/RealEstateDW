"""analytics/qa/recompute_median.py

Tính lại median/p25/p75 giá/m² bán bằng pandas và so với kết quả SQL (05_median_by_group.sql).
Chạy tay từ thư mục gốc project, không đưa vào pipeline:
    python analytics/qa/recompute_median.py
Thoát mã 0 nếu khớp, 1 nếu lệch.
"""
import argparse
import sys

import numpy as np
import pandas as pd

KEYS = ["property_type_name", "district_old"]
STATS = ["n", "p25", "median", "p75"]


def load_raw(path: str) -> pd.DataFrame:
    # Đọc mọi cột dạng text, giữ chuỗi rỗng (district_old có thể là '' trong Gold)
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df["price_per_m2_vnd"] = pd.to_numeric(df["price_per_m2_vnd"])
    return df


def recompute(raw: pd.DataFrame, min_n: int) -> pd.DataFrame:
    sale = raw[raw["deal_type"] == "sale"]
    grouped = sale.groupby(KEYS)["price_per_m2_vnd"]
    res = grouped.agg(
        n="count",
        p25=lambda s: s.quantile(0.25),
        median="median",
        p75=lambda s: s.quantile(0.75),
    ).reset_index()
    return res[res["n"] >= min_n]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--raw", default="analytics/qa/_out/latest_ready.csv")
    ap.add_argument("--sql", default="analytics/qa/_out/median_by_group.csv")
    ap.add_argument("--min-n", type=int, default=30)
    args = ap.parse_args()

    py = recompute(load_raw(args.raw), args.min_n)
    sql = pd.read_csv(args.sql, dtype={k: str for k in KEYS}, keep_default_na=False)

    merged = py.merge(sql, on=KEYS, how="outer", suffixes=("_py", "_sql"), indicator=True)
    only_py = merged[merged["_merge"] == "left_only"]
    only_sql = merged[merged["_merge"] == "right_only"]
    both = merged[merged["_merge"] == "both"]

    print(f"Số nhóm đạt n >= {args.min_n}: pandas={len(py)}, SQL={len(sql)}, chung={len(both)}")
    if len(only_py) or len(only_sql):
        print("Nhóm chỉ có ở pandas:\n", only_py[KEYS].to_string(index=False))
        print("Nhóm chỉ có ở SQL:\n", only_sql[KEYS].to_string(index=False))

    ok = len(only_py) == 0 and len(only_sql) == 0
    for col in STATS:
        diff = (both[f"{col}_py"] - both[f"{col}_sql"]).abs()
        match = np.isclose(both[f"{col}_py"], both[f"{col}_sql"], rtol=1e-9, atol=1e-6)
        print(f"{col:>6}: sai số lớn nhất = {diff.max():.3e}, nhóm lệch = {int((~match).sum())}")
        ok = ok and bool(match.all())

    print("KẾT QUẢ:", "KHỚP" if ok else "LỆCH")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
