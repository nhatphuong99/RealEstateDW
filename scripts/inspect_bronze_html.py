"""
Script: inspect_bronze_html.py
Muc dich: Doc truc tiep file parquet mau (co san local), lay ra vai record
HTML that de PHAN TICH THU CONG truoc khi xac nhan selector dung trong
parse_to_staging.py (dac biet la phan "core fields": title/price/area/address
hien dang la best-guess, chua duoc xac nhan qua sample that).

Script nay KHONG dong bo/upload/parse chinh thuc len S3 hay Postgres - chi
phuc vu muc dich kham pha du lieu (data exploration), tuong tu cach da lam
de tao ra alonhadat_data_source_analysis.md truoc day.

Cach dung:
    # Xem thong ke tong quan + kham pha itemprop/section.moreinfor1 cua 5 record dau
    python inspect_bronze_html.py --n 5

    # Lay mau ngau nhien 10 record thay vi lay 10 dong dau file
    python inspect_bronze_html.py --n 10 --random

    # Luu rieng file HTML ra o dia de mo bang trinh duyet (view-source) doi chieu tay
    python inspect_bronze_html.py --n 5 --save-html --out-dir data/html_samples

    # Xem 1 record cu the theo url (chua 1 doan text trong url, vi du id tin)
    python inspect_bronze_html.py --url-contains "12345678"

    # Chi xem bao cao tong hop, khong can doc chi tiet tung record (do it nhieu)
    python inspect_bronze_html.py --n 50 --quiet
"""

import argparse
import re
from collections import Counter
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

DEFAULT_FILE = "data/raw/alonhadat-10000-8digit.parquet"


# -----------------------------------------------------------------------------
# Doc du lieu
# -----------------------------------------------------------------------------
def load_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"[LOI] Khong tim thay file: {path.resolve()}")
    df = pd.read_parquet(path)
    required = {"url", "crawl_date", "html"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"[LOI] File parquet thieu cot bat buoc: {missing}")
    return df


def print_overview(df: pd.DataFrame):
    print("=" * 80)
    print("TONG QUAN FILE PARQUET")
    print("=" * 80)
    print(f"So dong            : {len(df)}")
    print(f"crawl_date min     : {df['crawl_date'].min()}")
    print(f"crawl_date max     : {df['crawl_date'].max()}")

    html_lengths = df["html"].map(lambda b: len(b) if isinstance(b, (bytes, bytearray)) else len(str(b)))
    print(f"Do dai HTML (byte) min/median/max: {html_lengths.min()} / {int(html_lengths.median())} / {html_lengths.max()}")

    dup_urls = df["url"].duplicated().sum()
    print(f"So url bi trung lap trong file mau: {dup_urls}")
    print()


# -----------------------------------------------------------------------------
# Phan tich chi tiet 1 record
# -----------------------------------------------------------------------------
def html_bytes_to_text(html_bytes) -> str:
    if isinstance(html_bytes, (bytes, bytearray)):
        return html_bytes.decode("utf-8", errors="replace")
    return str(html_bytes)


def inspect_one(url: str, crawl_date, html_bytes, verbose: bool = True) -> dict:
    """
    Phan tich 1 record HTML, in ra man hinh cac thong tin can de doi chieu
    voi selector dang dung trong parse_to_staging.py. Tra ve dict summary
    de gom lai thanh bao cao tong hop o cuoi.
    """
    html_text = html_bytes_to_text(html_bytes)
    soup = BeautifulSoup(html_text, "lxml")

    summary = {"url": url, "crawl_date": crawl_date}

    if verbose:
        print("-" * 80)
        print(f"URL         : {url}")
        print(f"crawl_date  : {crawl_date}")

    # --- article.property (container chinh, theo tai lieu phan tich da xac nhan) ---
    article = soup.find("article", class_="property")
    summary["has_article_property"] = article is not None
    if verbose:
        print(f"article.property tim thay: {summary['has_article_property']}")
    scope = article if article is not None else soup

    # --- Tat ca itemprop xuat hien trong trang (giup xac nhan CORE_ITEMPROP_CANDIDATES) ---
    itemprop_els = scope.find_all(attrs={"itemprop": True})
    itemprop_info = []
    for el in itemprop_els:
        prop = el.get("itemprop")
        text = el.get_text(strip=True)
        text_preview = (text[:60] + "...") if len(text) > 60 else text
        itemprop_info.append((prop, el.name, text_preview))
    summary["itemprops_found"] = [p for p, _, _ in itemprop_info]

    if verbose:
        print(f"\nCac itemprop tim thay ({len(itemprop_info)}):")
        for prop, tag, text_preview in itemprop_info:
            print(f"  - itemprop='{prop}'  <{tag}>  text='{text_preview}'")

    # --- section.moreinfor1: xem cau truc that (tr/td hay dang khac?) ---
    section = scope.find("section", class_="moreinfor1")
    summary["has_moreinfor1"] = section is not None
    labels_found = []
    if section is not None:
        rows = section.find_all("tr")
        if verbose:
            print(f"\nsection.moreinfor1 -> so <tr> tim thay: {len(rows)}")
        if rows:
            for row in rows:
                cells = row.find_all(["td", "th"])
                cell_texts = [c.get_text(strip=True) for c in cells]
                labels_found.extend(cell_texts[0::2])  # gia dinh label o vi tri chan (0,2,4,...)
                if verbose:
                    print(f"    tr -> {cell_texts}")
        else:
            # Khong co <tr> -> in HTML tho ben trong de tu xem cau truc thuc te
            if verbose:
                print("  (Khong co <tr> -> in HTML tho ben trong section.moreinfor1 de xem cau truc)")
                print("  " + section.prettify()[:1500].replace("\n", "\n  "))
    else:
        if verbose:
            print("\n[CANH BAO] Khong tim thay section.moreinfor1 trong record nay.")
    summary["labels_found"] = labels_found

    # --- div.warning ---
    warning_div = soup.find("div", class_="warning")
    summary["has_warning"] = warning_div is not None
    if verbose and warning_div is not None:
        print(f"\ndiv.warning: {warning_div.get_text(strip=True)}")

    # --- doi chieu listing_id tu URL (khong can HTML de suy ra) ---
    m = re.search(r"-(\d+)\.html?$", url)
    listing_id_from_url = m.group(1) if m else None
    summary["listing_id_from_url"] = listing_id_from_url
    if verbose:
        print(f"\nlisting_id suy tu URL: {listing_id_from_url}")
        print()

    return summary


# -----------------------------------------------------------------------------
# Bao cao tong hop tren nhieu record (giup dien CORE_ITEMPROP_CANDIDATES /
# ENRICHMENT_LABEL_MAP trong parse_to_staging.py mot cach co can cu thuc nghiem)
# -----------------------------------------------------------------------------
def print_aggregate_report(summaries: list):
    print("=" * 80)
    print(f"BAO CAO TONG HOP TREN {len(summaries)} RECORD MAU")
    print("=" * 80)

    n_article = sum(s["has_article_property"] for s in summaries)
    n_moreinfor1 = sum(s["has_moreinfor1"] for s in summaries)
    n_warning = sum(s["has_warning"] for s in summaries)
    print(f"So record co article.property  : {n_article}/{len(summaries)}")
    print(f"So record co section.moreinfor1: {n_moreinfor1}/{len(summaries)}")
    print(f"So record co div.warning       : {n_warning}/{len(summaries)}")

    itemprop_counter = Counter()
    for s in summaries:
        itemprop_counter.update(set(s["itemprops_found"]))
    print(f"\nTan suat itemprop xuat hien (tren {len(summaries)} record):")
    if itemprop_counter:
        for prop, count in itemprop_counter.most_common():
            print(f"  - itemprop='{prop}': {count}/{len(summaries)} record")
    else:
        print("  (khong tim thay itemprop nao -> can xem lai gia dinh ve schema.org microdata)")

    label_counter = Counter()
    for s in summaries:
        label_counter.update(s["labels_found"])
    print(f"\nTan suat label tim thay trong section.moreinfor1:")
    if label_counter:
        for label, count in label_counter.most_common():
            print(f"  - '{label}': {count}/{len(summaries)} record")
    else:
        print("  (khong tim thay label nao)")
    print()


# -----------------------------------------------------------------------------
# Luu HTML rieng ra file de mo bang trinh duyet (view-source) doi chieu thu cong
# -----------------------------------------------------------------------------
def save_html_samples(df: pd.DataFrame, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    for _, row in df.iterrows():
        m = re.search(r"-(\d+)\.html?$", row["url"])
        listing_id = m.group(1) if m else "unknown"
        out_path = out_dir / f"{listing_id}.html"
        out_path.write_text(html_bytes_to_text(row["html"]), encoding="utf-8")
        print(f"[INFO] Da luu: {out_path}  (url goc: {row['url']})")
    print(f"\n[XONG] Da luu {len(df)} file HTML vao {out_dir.resolve()}")
    print("=> Mo cac file nay bang trinh duyet (hoac VSCode) de view-source doi chieu voi selector.")


def main():
    parser = argparse.ArgumentParser(description="Kham pha HTML trong file parquet mau de xac nhan selector")
    parser.add_argument("--file", default=DEFAULT_FILE, help="Duong dan file parquet local")
    parser.add_argument("--n", type=int, default=5, help="So record mau can phan tich (mac dinh 5)")
    parser.add_argument("--random", action="store_true", help="Lay mau ngau nhien thay vi n dong dau file")
    parser.add_argument("--seed", type=int, default=42, help="Seed khi lay mau ngau nhien")
    parser.add_argument("--url-contains", default=None, help="Chi phan tich record co url chua chuoi nay")
    parser.add_argument("--save-html", action="store_true", help="Luu HTML cua cac record mau ra file rieng")
    parser.add_argument("--out-dir", default="data/html_samples", help="Thu muc luu HTML mau (dung voi --save-html)")
    parser.add_argument("--quiet", action="store_true", help="Chi in bao cao tong hop, khong in chi tiet tung record")
    args = parser.parse_args()

    df = load_parquet(Path(args.file))
    print_overview(df)

    if args.url_contains:
        sample = df[df["url"].str.contains(args.url_contains, na=False)]
        if sample.empty:
            raise SystemExit(f"[LOI] Khong tim thay record nao co url chua '{args.url_contains}'")
    elif args.random:
        sample = df.sample(n=min(args.n, len(df)), random_state=args.seed)
    else:
        sample = df.head(args.n)

    summaries = []
    for _, row in sample.iterrows():
        summaries.append(inspect_one(row["url"], row["crawl_date"], row["html"], verbose=not args.quiet))

    print_aggregate_report(summaries)

    if args.save_html:
        save_html_samples(sample, Path(args.out_dir))


if __name__ == "__main__":
    main()
