# 🏠 Real Estate Data Pipeline & Data Warehouse — TP.HCM

> Hệ thống thu thập, xử lý và phân tích dữ liệu giá bất động sản TP.HCM (nguồn: [alonhadat.com.vn](https://alonhadat.com.vn)), xây dựng theo kiến trúc Medallion (Bronze → Silver → Gold), tự động hóa toàn trình bằng Apache Airflow, phục vụ dashboard phân tích giá theo Quận/Phường trên Metabase.

**Đồ án cuối khóa DEP305x — Kỹ thuật Dữ liệu**

![Airflow](https://img.shields.io/badge/Apache%20Airflow-3.3.0-017CEE?logo=apacheairflow&logoColor=white)
![Spark](https://img.shields.io/badge/PySpark-4.2.0-E25A1C?logo=apachespark&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS%20S3-Bronze%20Layer-FF9900?logo=amazons3&logoColor=white)
![Metabase](https://img.shields.io/badge/Metabase-v0.63-509EE3?logo=metabase&logoColor=white)
![Docker](https://img.shields.io/badge/Docker%20Compose-Self--hosted-2496ED?logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-Academic--Use-lightgrey)

---

## Mục lục

- [🏠 Real Estate Data Pipeline \& Data Warehouse — TP.HCM](#-real-estate-data-pipeline--data-warehouse--tphcm)
  - [Mục lục](#mục-lục)
  - [Giới thiệu](#giới-thiệu)
  - [Kiến trúc tổng quan](#kiến-trúc-tổng-quan)
  - [Tính năng chính](#tính-năng-chính)
  - [Tech stack](#tech-stack)
  - [Cấu trúc thư mục](#cấu-trúc-thư-mục)
  - [Bắt đầu nhanh](#bắt-đầu-nhanh)
    - [Yêu cầu](#yêu-cầu)
    - [Cài đặt](#cài-đặt)
    - [Truy cập](#truy-cập)
  - [Vận hành pipeline](#vận-hành-pipeline)
  - [Thiết kế dữ liệu](#thiết-kế-dữ-liệu)
  - [Dashboard](#dashboard)
  - [Quy mô \& hiệu năng](#quy-mô--hiệu-năng)
  - [Tuân thủ thu thập dữ liệu](#tuân-thủ-thu-thập-dữ-liệu)
  - [Giới hạn hiện tại](#giới-hạn-hiện-tại)
  - [Tác giả](#tác-giả)
  - [Giấy phép](#giấy-phép)

---

## Giới thiệu

Thị trường bất động sản TP.HCM thiếu một nguồn dữ liệu giá tổng hợp, minh bạch, cập nhật theo thời gian ở cấp Quận/Phường. Dự án xây dựng một **data pipeline hoàn chỉnh**, biến dữ liệu tin đăng bất động sản dạng bán cấu trúc (giá, diện tích, vị trí, mô tả tự do tiếng Việt) thành dữ liệu có cấu trúc, đo lường được xu hướng giá/m² theo khu vực và loại hình BĐS theo thời gian.

Điểm nổi bật kỹ thuật: dữ liệu nguồn **bẩn, bán cấu trúc** (giá ghi nhiều định dạng: "2.5 tỷ", "25 triệu/m²", "Thỏa thuận"...), pipeline tự phục hồi khi crash giữa chừng, quản lý lịch sử thay đổi giá theo thời gian bằng SCD Type 2.

## Kiến trúc tổng quan

```
        CDN Dataset (77 part)          alonhadat.com.vn (crawl)
          DAG 1 — chạy tay 1 lần         DAG 2 — @hourly
                  └──────────────┬──────────────┘
                                 ▼
          ┌─────────────────────────────────────┐
          │           BRONZE · AWS S3           │
          │  url / crawl_date / html (Parquet)  │
          └─────────────────────────────────────┘
                                 │  DAG 3 — Spark parse + merge SCD2
                                 ▼
          ┌─────────────────────────────────────┐
          │         SILVER · PostgreSQL         │
          │     listing_history (SCD Type 2)    │
          └─────────────────────────────────────┘
                                 │  DAG 4 — SQL full-refresh idempotent
                                 ▼
          ┌─────────────────────────────────────┐
          │          GOLD · PostgreSQL          │
          │    1 Fact + 5 Dimension (Kimball)   │
          └─────────────────────────────────────┘
                                 │
                                 ▼
          ┌─────────────────────────────────────┐
          │      Metabase — 3 tab · 13 card     │
          └─────────────────────────────────────┘
```

Apache Airflow điều phối toàn bộ chuỗi DAG 2 → 3 → 4 tự động mỗi giờ. DAG 1 đứng ngoài chuỗi, chỉ chạy tay khi cần nạp lại dữ liệu gốc.

## Tính năng chính

| # | Tính năng | Mô tả |
|---|---|---|
| 1 | **Crawler tự động, 2 nguồn** | DAG 1 nạp dataset CDN có sẵn (1 lần); DAG 2 crawl trực tiếp web theo lịch `@hourly`, tự xoay proxy/User-Agent, tự phục hồi khi crash |
| 2 | **Data cleaning pipeline** | Spark parse HTML → chuẩn hóa giá/VNĐ, diện tích/m², tách số phòng ngủ/hướng/pháp lý từ bảng thuộc tính, xử lý hàng loạt trường hợp ngoại lệ dữ liệu thực tế |
| 3 | **SCD Type 2** | Theo dõi đầy đủ lịch sử thay đổi giá theo thời gian trên `silver.listing_history` |
| 4 | **Data Warehouse (Star Schema)** | `fact_listing_price` + 5 dimension (Kimball), tối ưu cho truy vấn phân tích |
| 5 | **Orchestration** | 4 DAG Airflow nối chuỗi tự động, retry, concurrency control, tự phục hồi run bị crash cứng |
| 6 | **Dashboard phân tích** | Metabase — giá TB/m² theo Quận/Phường (region map), xu hướng theo thời gian, phân bố theo loại hình BĐS |

## Tech stack

| Layer | Công nghệ |
|---|---|
| Orchestration | Apache Airflow 3.3.0 (CeleryExecutor, Docker Compose) |
| Xử lý dữ liệu | PySpark 4.2.0 (local mode) |
| Raw storage | AWS S3 (Bronze layer) |
| Data Warehouse | PostgreSQL 16 (Silver + Gold) |
| BI / Dashboard | Metabase v0.63 |
| Ngôn ngữ | Python 3, SQL |
| Thư viện chính | `requests`, `BeautifulSoup4`/`lxml`, `psycopg2`, `boto3`, `pyarrow` |
| Hạ tầng | Docker Compose (Airflow + PostgreSQL + Redis + Metabase), self-hosted |

## Cấu trúc thư mục

```
RealEstateDW/
├── crawler/          # DAG 1 (dataset) + DAG 2 (web crawl)
├── dags/              # Khai báo lịch chạy/retry cho 4 DAG
├── parser/             # DAG 3 (Spark parse) + DAG 4 (Silver→Gold SQL)
├── sql/
│   ├── schema_full.sql  # DDL hợp nhất toàn bộ DB (pipeline/silver/gold)
│   ├── dashboard_metabase_queries.sql # Các query cho dashboard trên metabase
│   └── queries/             # merge_scd2, etl_silver_to_gold, validate, diagnose
├── config.py                # Cấu hình dùng chung (DSN, S3 bucket...)
├── docker-compose.yaml
├── Dockerfile
└── requirements.txt
```


## Bắt đầu nhanh

### Yêu cầu

- Docker & Docker Compose
- Tài khoản AWS (S3 bucket riêng cho Bronze layer, IAM user quyền tối thiểu)
- ≥ 4GB RAM, 2 CPU, 10GB disk trống cho Docker

### Cài đặt

```bash
# 1. Clone repo & tạo file môi trường
git clone <repo-url> && cd RealEstateDW
cp .env.example .env
# Điền: POSTGRES_DW_*, FERNET_KEY, AWS credentials, S3_BRONZE_BUCKET...

# 2. Build image & khởi tạo Airflow (chạy 1 lần)
docker compose build
docker compose up airflow-init

# 3. Khởi động toàn bộ hệ thống
docker compose up -d
```

Áp DDL cho Data Warehouse (chạy 1 lần, sau khi `postgres-dw` đã khởi động):

```powershell
# Windows PowerShell
$env:PGCLIENTENCODING = 'UTF8'
psql "$env:POSTGRES_DW_DSN_LOCAL" -f sql/schema_full.sql
```

```bash
# Linux / macOS
PGCLIENTENCODING=UTF8 psql "$POSTGRES_DW_DSN_LOCAL" -f sql/schema_full.sql
```

### Truy cập

| Dịch vụ | URL | Tài khoản mặc định |
|---|---|---|
| Airflow UI | http://localhost:8080 | `airflow` / `airflow` |
| Metabase | http://localhost:3000 | Tạo tài khoản ở lần truy cập đầu |
| PostgreSQL DW | `localhost:5433` | theo `.env` |

## Vận hành pipeline

1. Bật DAG `dataset_loader`, trigger **thủ công 1 lần** để nạp dữ liệu dataset ban đầu lên Bronze (S3).
2. Bật DAG `web_crawler` — tự chạy `@hourly`, tự động kéo theo `bronze_to_silver` → `silver_to_gold` mỗi chu kỳ.
3. Theo dõi tiến độ qua Airflow UI hoặc trực tiếp các bảng control-plane trong schema `pipeline` (Postgres).
4. Dữ liệu sẵn sàng cho Metabase ngay sau khi DAG 4 (`silver_to_gold`) chạy thành công.

## Thiết kế dữ liệu

- **Bronze**: Parquet thô trên S3, schema thống nhất `url` / `crawl_date` / `html`, immutable.
- **Silver**: `silver.listing_history` — SCD Type 2 trên 5 trường biến động (giá, diện tích, trạng thái hết hạn/cảnh báo).
- **Gold**: Star Schema Kimball — `fact_listing_price` (Observation-grain) + `dim_date`, `dim_location`, `dim_property_type`, `dim_source`, `dim_property_features`.

Toàn bộ DDL nằm trong [`sql/schema_full.sql`](./sql/schema_full.sql).

## Dashboard

Metabase — 3 tab, 13 card, đọc từ view `gold.vw_fact_report`:

| Tab | Nội dung |
|---|---|
| Tổng quan | Tổng số tin, giá TB/trung vị/m², bản đồ giá theo Phường/Xã & Quận/Huyện |
| Theo khu vực | Top 10 khu vực giá cao/thấp nhất, bảng tổng hợp theo Quận/Huyện |
| Xu hướng & phân bố | Xu hướng giá theo tháng, phân bố theo loại hình BĐS, tỷ lệ Cần bán/Cho thuê |

## Quy mô & hiệu năng

- ~250.000 dòng ở Silver/Fact Gold, dự tính mỗi ngày crawl thêm ~1000-1500 url.
- Crawl web: giới hạn ~45 phút/chu kỳ hourly, tự dừng khi đạt ngưỡng an toàn.
- ETL Silver → Gold: full-refresh idempotent trong 1 transaction SQL.

## Tuân thủ thu thập dữ liệu

- Đã kiểm tra `robots.txt` và điều khoản sử dụng của nguồn trước khi crawl.
- Giới hạn tốc độ crawl (delay ngẫu nhiên giữa các request, time-box mỗi run) — tôn trọng tải máy chủ nguồn.
- Dự án học thuật, phi thương mại, phạm vi quy mô nhỏ.

## Giới hạn hiện tại

- Phụ thuộc cấu trúc HTML của alonhadat.com.vn — trang nguồn đổi selector sẽ cần cập nhật lại `parser/bronze_to_silver_core.py`.
- Proxy free có tỷ lệ chết cao, ảnh hưởng tốc độ crawl thực tế theo giờ.
- Chạy Spark ở chế độ local, giới hạn 1 SparkSession đồng thời — chưa tối ưu cho quy mô dữ liệu lớn hơn nhiều lần hiện tại.
- Dashboard hiện tập trung vào giá/m² và số lượng tin; chưa khai thác các thuộc tính junk dimension (hướng nhà, pháp lý, tiện ích).

## Tác giả

**Nguyễn Lý Nhật Phương**, dưới sự hướng dẫn của mentor **Cù Hữu Hoàng** — đồ án cuối khóa Kỹ thuật Dữ liệu.

## Giấy phép

Đồ án học thuật, phi thương mại — thực hiện trong khuôn khổ môn **Đồ án cuối khóa Kỹ thuật Dữ liệu**.
