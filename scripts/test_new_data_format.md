# Read the Alonhadat Crawled-Data Parquet Parts with Python

## Dataset

- Files: `part1.parquet` through `part77.parquet`
- URL pattern: `https://cdn.cuhuuhoang.com/alonhadat/partN.parquet`, where `N` is from `1` to `77`
- Total rows: `764,212`
- Rows per file: `10,000` in parts 1-76 and `4,212` in part 77
- Compression: Parquet with Zstandard compression
- Columns:
  - `url`: string
  - `crawl_date`: timestamp
  - `html`: raw HTML bytes

## Install

```bash
python3 -m pip install pyarrow
```

## Download All 77 Parts

The full dataset is about 3.4 GiB. This downloads one file at a time and skips files that already exist:

```python
from pathlib import Path
from urllib.request import urlretrieve

base_url = "https://cdn.cuhuuhoang.com/alonhadat"
output_dir = Path("alonhadat")
output_dir.mkdir(parents=True, exist_ok=True)

for part_number in range(1, 78):
    name = f"part{part_number}.parquet"
    path = output_dir / name
    if path.exists():
        print(f"Skipping {path}")
        continue

    print(f"Downloading {name}")
    urlretrieve(f"{base_url}/{name}", path)
```

## Inspect One Part

```python
import pyarrow.parquet as pq

parquet = pq.ParquetFile("alonhadat/part1.parquet")
print(parquet.schema_arrow)
print("rows:", parquet.metadata.num_rows)
```

Expected Arrow types are `string`, `timestamp[us]`, and `binary`.

## Read All Parts as One Dataset

Pass the files in numeric order so `part10.parquet` does not come before `part2.parquet`:

```python
from pathlib import Path
import pyarrow.dataset as ds

files = [str(Path("alonhadat") / f"part{n}.parquet") for n in range(1, 78)]
dataset = ds.dataset(files, format="parquet")

print(dataset.schema)
print("rows:", dataset.count_rows())
```

## Stream Rows in Batches

Stream the dataset instead of loading all raw HTML into memory:

```python
for batch in dataset.to_batches(
    batch_size=1_000,
    columns=["url", "crawl_date", "html"],
):
    urls = batch.column("url").to_pylist()
    crawl_dates = batch.column("crawl_date").to_pylist()
    html_documents = batch.column("html").to_pylist()

    for url, crawl_date, html_bytes in zip(urls, crawl_dates, html_documents):
        # html_bytes is bytes. Preserve it as bytes unless text is required.
        print(url, crawl_date, len(html_bytes))
```

## Decode HTML

Most pages can be decoded as UTF-8. Use replacement for malformed byte sequences:

```python
html_text = html_bytes.decode("utf-8", errors="replace")
```

For charset detection:

```bash
python3 -m pip install charset-normalizer
```

```python
from charset_normalizer import from_bytes

match = from_bytes(html_bytes).best()
html_text = str(match) if match is not None else html_bytes.decode("utf-8", errors="replace")
```

## Parse HTML

```bash
python3 -m pip install beautifulsoup4 lxml
```

```python
from bs4 import BeautifulSoup

soup = BeautifulSoup(html_bytes, "lxml")
title = soup.title.get_text(" ", strip=True) if soup.title else None
text = soup.get_text(" ", strip=True)

print(title)
print(text[:500])
```

Passing `bytes` directly lets the parser inspect HTML charset declarations.

## Read Selected Columns

Reading only `url` and `crawl_date` avoids loading the much larger HTML values:

```python
table = dataset.to_table(columns=["url", "crawl_date"])
print(table.to_pandas().head())
```

Filter rows by URL while selecting only the required columns:

```python
import pyarrow.compute as pc
import pyarrow.dataset as ds

table = dataset.to_table(
    columns=["url", "crawl_date"],
    filter=pc.match_substring(ds.field("url"), "alonhadat.com.vn"),
)
print(table.num_rows)
```

## Extract a Sample HTML File

```python
from pathlib import Path

batch = next(
    dataset.to_batches(
        batch_size=1,
        columns=["url", "crawl_date", "html"],
    )
)

url = batch.column("url")[0].as_py()
crawl_date = batch.column("crawl_date")[0].as_py()
html_bytes = batch.column("html")[0].as_py()

Path("sample.html").write_bytes(html_bytes)
print(url, crawl_date, len(html_bytes))
```

Do not convert the `html` column to Base64 unless a downstream interface cannot carry binary values. Parquet already stores it efficiently as binary data.