# Read the Alonhadat Crawled-Data Parquet with Python

## Dataset

- Download: <https://cdn.cuhuuhoang.com/1m/alonhadat-10000.parquet>
- Rows: `10,000`
- Compression: Parquet with Zstandard compression
- SHA-256: `1c65ead24a99363a9784c9e140a3ea6950fb1277b3eee2b8a848db9932acc9f6`
- Columns:
  - `url`: string
  - `crawl_date`: timestamp
  - `html`: raw HTML bytes

## Install

```bash
python3 -m pip install pyarrow
```

## Download and Verify

```python
from pathlib import Path
from urllib.request import urlretrieve
import hashlib

source = "https://cdn.cuhuuhoang.com/1m/alonhadat-10000.parquet"
path = Path("alonhadat-10000.parquet")
expected_sha256 = "1c65ead24a99363a9784c9e140a3ea6950fb1277b3eee2b8a848db9932acc9f6"

urlretrieve(source, path)

digest = hashlib.sha256(path.read_bytes()).hexdigest()
if digest != expected_sha256:
    raise ValueError(f"Checksum mismatch: {digest}")
```

For a large future dump, calculate the checksum without loading the file into memory:

```python
import hashlib

digest = hashlib.sha256()
with open("alonhadat-10000.parquet", "rb") as stream:
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(chunk)

print(digest.hexdigest())
```

## Inspect the Schema

```python
import pyarrow.parquet as pq

parquet = pq.ParquetFile("alonhadat-10000.parquet")
print(parquet.schema_arrow)
print("rows:", parquet.metadata.num_rows)
```

Expected Arrow types are `string`, `timestamp[us]`, and `binary`.

## Stream Rows in Batches

Do this instead of loading a large dump into memory at once:

```python
import pyarrow.parquet as pq

parquet = pq.ParquetFile("alonhadat-10000.parquet")

for batch in parquet.iter_batches(
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

Most sampled pages decode as UTF-8. Use replacement for malformed byte sequences:

```python
html_text = html_bytes.decode("utf-8", errors="replace")
```

If accurate charset handling is important, detect it from the HTML bytes:

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

## Read Selected Columns or Rows

Read only URL and crawl date:

```python
import pyarrow.parquet as pq

table = pq.read_table(
    "alonhadat-10000.parquet",
    columns=["url", "crawl_date"],
)
print(table.to_pandas().head())
```

Filter with PyArrow Dataset:

```python
import pyarrow.dataset as ds
import pyarrow.compute as pc

dataset = ds.dataset("alonhadat-10000.parquet", format="parquet")
table = dataset.to_table(
    columns=["url", "crawl_date"],
    filter=pc.match_substring(ds.field("url"), "/tags/"),
)
print(table.num_rows)
```

## Extract a Sample HTML File

```python
from pathlib import Path
import pyarrow.parquet as pq

batch = next(
    pq.ParquetFile("alonhadat-10000.parquet").iter_batches(
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
