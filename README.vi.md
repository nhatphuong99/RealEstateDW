# Data Warehouse Bất Động Sản TP. Hồ Chí Minh

🇻🇳 Tiếng Việt (file này) · 🇬🇧 [English](README.md)

[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.3.0-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![PySpark](https://img.shields.io/badge/PySpark-4.2.0-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![AWS S3](https://img.shields.io/badge/AWS-S3-FF9900?logo=amazons3&logoColor=white)](https://aws.amazon.com/s3/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Metabase](https://img.shields.io/badge/Metabase-v0.63-509EE3?logo=metabase&logoColor=white)](https://www.metabase.com/)

---

## Tóm tắt

Đây là đồ án data engineering cá nhân, xây dựng pipeline biến dữ liệu tin đăng bất động sản tiếng Việt thành data warehouse phục vụ phân tích. Project kết hợp web crawling, xử lý HTML bằng PySpark, theo dõi lịch sử giá bằng SCD Type 2, mô hình hóa dimensional trên PostgreSQL, điều phối bằng Airflow và trực quan hóa với Metabase.

## Demo & Số liệu chính

- **Hơn 243K** tin đăng trong lần chạy đầy đủ gần nhất
- **SCD Type 2** lưu lịch sử giá, với **hơn 6.000** tin có nhiều phiên bản được theo dõi
- **4 Airflow DAG** cho các tầng Bronze, Silver và Gold
- **3 tab dashboard** và **13 card Metabase** cho phân tích khu vực, xu hướng và phân bố

Repository có screenshot dashboard và Airflow trong thư mục [`assets/`](assets/). Bộ dữ liệu seed lịch sử ban đầu là dữ liệu private và không được đưa vào repository. Thư mục [`data/`](data/) chỉ chứa một số file parquet nhỏ từ nhánh web crawler để chạy thử local.

> Project phục vụ mục đích trình bày portfolio và phát triển local. Các số liệu được lấy từ những lần chạy đầy đủ trong môi trường local, không phải từ production deployment.

## Mục lục

- [Data Warehouse Bất Động Sản TP. Hồ Chí Minh](#data-warehouse-bất-động-sản-tp-hồ-chí-minh)
  - [Tóm tắt](#tóm-tắt)
  - [Demo \& Số liệu chính](#demo--số-liệu-chính)
  - [Mục lục](#mục-lục)
  - [Bài toán thực tế](#bài-toán-thực-tế)
  - [Project này làm gì](#project-này-làm-gì)
  - [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)
  - [Tech Stack](#tech-stack)
  - [Mô hình dữ liệu](#mô-hình-dữ-liệu)
  - [Luồng xử lý Pipeline](#luồng-xử-lý-pipeline)
  - [Kết quả \& Số liệu thực tế](#kết-quả--số-liệu-thực-tế)
  - [Điểm nhấn kỹ thuật](#điểm-nhấn-kỹ-thuật)
  - [Chất lượng \& độ tin cậy dữ liệu](#chất-lượng--độ-tin-cậy-dữ-liệu)
  - [Dashboard](#dashboard)
  - [Điều phối](#điều-phối)
  - [Cấu trúc thư mục](#cấu-trúc-thư-mục)
  - [Hướng dẫn chạy thử](#hướng-dẫn-chạy-thử)
  - [Tác giả](#tác-giả)

---

## Bài toán thực tế

Thị trường bất động sản TP.HCM hiện thiếu một nguồn dữ liệu giá tổng hợp, minh bạch, cập nhật theo thời gian ở cấp độ Quận/Phường. Người mua nhà, nhà đầu tư, môi giới nhỏ hiện phải tự tổng hợp thủ công giá rao bán từ nhiều nguồn — một quy trình chậm, dễ sai lệch vì tin đăng trùng lặp, giá "ảo" để câu khách, và mô tả tự do không đồng nhất.

Project này xây dựng một tầng phân tích tự phục vụ (self-serve analytics) trên dữ liệu tin đăng thô từ **alonhadat.com.vn**, biến HTML bán cấu trúc tiếng Việt thành các chỉ số giá/m² sạch, sẵn sàng phân tích xu hướng theo Phường/Quận/loại hình bất động sản.

## Project này làm gì

- **Thu thập** tin đăng bất động sản từ 2 nguồn: một bộ dữ liệu nạp 1 lần ban đầu (không public) và một luồng crawl trực tiếp chạy liên tục theo giờ.
- **Parse** HTML tự do, lộn xộn (nhiều định dạng giá khác nhau, cách ghi diện tích không nhất quán, thiếu trường dữ liệu) thành bản ghi có cấu trúc bằng Spark.
- **Theo dõi lịch sử giá theo thời gian** bằng kỹ thuật Slowly Changing Dimension (SCD Type 2) — mỗi lần giá 1 tin thay đổi được lưu thành 1 phiên bản mới, không ghi đè lên bản cũ.
- **Mô hình hóa** dữ liệu đã sạch thành star schema kiểu Kimball, tối ưu cho truy vấn phân tích.
- **Tự động hóa** toàn bộ pipeline end-to-end bằng Apache Airflow, có cơ chế tự phục hồi khi crash.
- **Trực quan hóa** kết quả qua dashboard Metabase gồm 3 tab, 13 card, có bản đồ khu vực tương tác, xu hướng giá theo thời gian và phân bố theo loại hình.

## Kiến trúc hệ thống

Hệ thống theo **kiến trúc Medallion** (Bronze → Silver → Gold), toàn bộ được điều phối bởi Airflow:

```
  +-------------+   +------------------+
  | Dataset seed |   | alonhadat.com.vn |
  | private      |   |                  |
  +-------------+   +------------------+
         |                    |
         v                    v
  +----------------+  +-----------------------+
  | DAG 1          |  | DAG 2                 |
  | dataset_loader |  | web_crawler           |
  | (chạy 1 lần)   |  | (@hourly, xoay proxy, |
  |                |  | tự resume khi crash)  |
  +----------------+  +-----------------------+
           |                      |
           v                      v
     +---------------------------------+
     | BRONZE - AWS S3 (Parquet thô)   |
     | schema: url | crawl_date | html |
     +---------------------------------+
                      |
                      v
    +-----------------------------------+
    | DAG 3 - bronze_to_silver          |
    | PySpark: parse HTML -> dữ liệu có |
    | cấu trúc, merge SCD Type 2        |
    +-----------------------------------+
                      |
                      v
  +------------------------------------------+
  | SILVER - PostgreSQL                      |
  | listing_history (SCD2), parse_quarantine |
  +------------------------------------------+
                      |
                      v
     +---------------------------------+
     | DAG 4 - silver_to_gold          |
     | ETL SQL full-refresh idempotent |
     | + tự động validate sau khi load |
     +---------------------------------+
                      |
                      v
    +----------------------------------+
    | GOLD - PostgreSQL (Star Schema)  |
    | 1 Fact + 5 Dim, thiết kế Kimball |
    +----------------------------------+
                      |
                      v
             +-----------------+
             | Metabase        |
             | 3 tab / 13 card |
             +-----------------+
```

**Chuỗi điều phối:** `DAG 2 (@hourly) → DAG 3 → DAG 4`, dùng chung 1 `run_id` để dễ truy vết. `DAG 1` nạp bộ dữ liệu lịch sử private khi có sẵn và cũng nối tiếp sang chuỗi xử lý phía sau.

## Tech Stack

| Tầng | Công nghệ | Lý do lựa chọn |
|---|---|---|
| Điều phối | **Apache Airflow 3.3.0** (CeleryExecutor) | Lập lịch, retry, quản lý phụ thuộc giữa các DAG, theo dõi trạng thái task khi crash |
| Thu thập dữ liệu | **Python** (`requests`, `BeautifulSoup`) | Trang nguồn render phía server, không cần chạy JS |
| Raw storage (Bronze) | **AWS S3** (Parquet) | Lưu trữ bền vững, chi phí thấp cho dữ liệu raw immutable |
| Xử lý phân tán | **PySpark 4.2.0** (local mode) | Parse HTML quy mô lớn qua `mapPartitions`, mô phỏng đúng công cụ big data thực tế |
| Data Warehouse | **PostgreSQL 16** (self-hosted, Docker) | Silver (SCD2) + Gold (star schema) |
| BI / Dashboard | **Metabase v0.63** | Dựng nhanh, phù hợp demo trực tiếp khi phỏng vấn |
| Hạ tầng | **Docker Compose** | Điều phối local có thể tái tạo cho 8+ service |

**Nguyên tắc thiết kế:** ưu tiên công nghệ mã nguồn mở / free-tier thay vì triển khai full-cloud (không dùng RDS/Redshift) để giữ chi phí gần như 0đ cho một đồ án học thuật, nhưng vẫn thể hiện đầy đủ kỹ năng về cloud, xử lý phân tán và điều phối pipeline.

## Mô hình dữ liệu

**Tầng Silver** — `listing_history`, bảng SCD Type 2 trong đó mỗi dòng là 1 *phiên bản giá đã quan sát* của 1 tin đăng. Một phiên bản được đóng lại (`valid_to`, `is_current = FALSE`) và mở phiên bản mới khi phát hiện thay đổi ở giá, tình trạng thỏa thuận, trạng thái hết hạn, cờ cảnh báo, hoặc diện tích — theo dõi qua cột `row_hash` tự sinh.

**Tầng Gold** — Star schema kiểu Kimball, Fact table ở **grain Observation** (1 dòng Fact = 1 phiên bản SCD2 ở Silver, không phải 1 tin đăng):

![Sơ đồ ERD star schema — fact_listing_price với 5 dimension](assets/erd/star_schema.png)

- **Trục xu hướng (trend axis):** `posted_date` (ngày tin thực sự được đăng).
- **Trường lineage:** `valid_from` / `valid_to` / `is_current` copy từ Silver chỉ phục vụ mục đích audit — không dùng để phân tích xu hướng.
- **Phạm vi khu vực:** TP.HCM, Bình Dương và Bà Rịa – Vũng Tàu được gộp thống nhất theo địa giới hành chính mới sau sáp nhập.

## Luồng xử lý Pipeline

| DAG | Mục đích | Lịch chạy | Cơ chế chính |
|---|---|---|---|
| **DAG 1** — `dataset_loader` | Nạp một lần bộ dữ liệu lịch sử private | Chạy tay | Dynamic task mapping, resumable qua `pipeline.dataset_part_state` |
| **DAG 2** — `web_crawler` | Crawl trực tiếp alonhadat.com.vn liên tục | `@hourly` | State machine crawl loop, pool proxy xoay vòng, buffer checkpoint ra S3, tự phục hồi khi crash |
| **DAG 3** — `bronze_to_silver` | Parse HTML thô thành trường có cấu trúc, merge vào lịch sử SCD2 | Do DAG 1/2 trigger | PySpark `mapPartitions`, cách ly (quarantine) bản ghi không parse được thay vì làm fail cả batch |
| **DAG 4** — `silver_to_gold` | Nạp star schema và validate tính toàn vẹn dữ liệu | Do DAG 3 trigger | Transaction SQL full-refresh idempotent + 5 check tự động sau khi load |

## Kết quả & Số liệu thực tế

Số liệu từ lần chạy đầy đủ gần nhất trong môi trường local (snapshot giữa tháng 9/2026; kết quả crawler thay đổi khi có tin mới):

| Chỉ số | Giá trị |
|---|---|
| Tổng số tin đang theo dõi (còn hiệu lực, trong phạm vi) | **~243,8K** |
| Giá trung bình/m² toàn TP.HCM | **165,39 triệu/m²** |
| Giá trung vị/m² toàn TP.HCM | **126,19 triệu/m²** |
| Số tin có từ 2 phiên bản giá trở lên (SCD2 bắt được thay đổi giá thật) | **6.000+** |
| Tỷ lệ parse thành công | **100%** (chưa có bản ghi nào trong `parse_quarantine`) |
| Thời gian chạy trung bình 1 lần DAG 2 hourly (không retry) | **~50 phút** |
| Thông lượng crawl (stress test 1 tuần treo máy) | **10.000+ URL mới** được phát hiện qua crawl hourly liên tục |

**Phân bố theo loại hình BĐS** (tin trong phạm vi):

| Loại hình | Số tin | Giá TB/m² |
|---|---|---|
| Nhà mặt tiền | 91.283 | ~230 triệu |
| Biệt thự, nhà liền kề | 14.369 | ~153 triệu |
| Nhà trong hẻm | 128.352 | ~126 triệu |
| Phòng trọ, nhà trọ | 3.991 | ~75 triệu |
| Căn hộ chung cư | 5.793 | ~38 triệu |

**Tỷ lệ loại tin:** 93,79% Cần bán · 6,21% Cho thuê

## Điểm nhấn kỹ thuật

- **Thiết kế phân tầng, dễ test:** tách logic nghiệp vụ khỏi Postgres, S3, HTTP và Spark qua các module `*_core.py` / `*_io.py` cùng interface được inject.
- **Ingestion có thể resume:** trạng thái crawler và dữ liệu Bronze đã buffer hỗ trợ khôi phục sau khi run bị gián đoạn.
- **Load warehouse tin cậy:** SQL idempotent, lịch sử SCD Type 2 và Gold transaction giúp retry an toàn.
- **Kiểm soát chất lượng dữ liệu:** bản ghi lỗi được quarantine, outlier được gắn cờ và các check sau load tự động kiểm tra row count, key và giá trị đã sanitize.

## Chất lượng & độ tin cậy dữ liệu

- **Cách ly, không crash:** HTML không parse được được đưa vào `parse_quarantine` kèm HTML gốc và lý do lỗi — batch tiếp tục xử lý thay vì fail toàn bộ.
- **Gắn cờ outlier thay vì xóa:** giá/diện tích vượt ngưỡng hợp lý được gắn cờ (`price_is_outlier`, `area_is_outlier`) thay vì âm thầm loại bỏ, giữ nguyên khả năng quan sát bất thường cho analyst.
- **Validate tự động sau mỗi lần load:** 5 check chạy sau mỗi lần nạp Gold (khớp số dòng với Silver, mỗi tin chỉ 1 dòng `is_current`, khóa ngoại không NULL, outlier đã được gắn cờ đầy đủ, diện tích nằm trong ngưỡng đã sanitize) — check nào fail sẽ làm fail task Airflow thay vì âm thầm đẩy dữ liệu sai.
- **Có công cụ chẩn đoán sẵn:** 1 query chẩn đoán riêng chỉ ra chính xác JOIN dimension nào đang làm rớt dòng khi số dòng không khớp, thay vì phải đoán.

## Dashboard

Xây dựng trên Metabase — 3 tab, 13 card, dùng chung 1 view báo cáo (`gold.vw_fact_report`) để mọi câu hỏi đều nhất quán logic lọc.

| Tab | Nội dung |
|---|---|
| **Tổng quan** | Tổng số tin đang theo dõi, giá trung bình/trung vị/m² toàn thành phố, bản đồ khu vực tương tác theo Phường và theo Quận |
| **Theo khu vực** | Top 10 Phường giá cao/thấp nhất, bảng tổng hợp theo Quận/Huyện |
| **Xu hướng & phân bố** | Xu hướng giá theo tháng theo từng loại hình BĐS, số lượng tin theo thời gian, phân bố theo loại hình, tỷ lệ tin bán/cho thuê |

**Tổng quan — tổng số tin, giá TB/trung vị, bản đồ giá theo Phường:**

![Tab Tổng quan — số liệu và bản đồ giá khu vực](assets/dashboard/01_overview_summary.png)

**Xu hướng — giá TB/m² theo tháng, theo loại hình BĐS:**

![Xu hướng giá theo tháng theo loại hình BĐS](assets/dashboard/02_trend_by_property_type.png)

**Theo khu vực — Top 10 Phường/Xã giá thấp nhất (sau sáp nhập):**

![Bảng Top 10 Phường/Xã giá thấp nhất](assets/dashboard/03_top10_cheapest_wards.png)

**Theo khu vực — Top 10 Phường/Xã giá cao nhất:**

![Bảng Top 10 Phường/Xã giá cao nhất](assets/dashboard/04_top10_highest_wards.png)

> Phường đắt nhất, **Phường Sài Gòn** (Quận 1 cũ), giá trung bình **~662 triệu/m²** — hơn 4 lần giá trung bình toàn thành phố — trong khi 10 Phường/Xã rẻ nhất (chủ yếu các huyện ngoại thành cũ như Củ Chi, Bến Cát) dưới 14 triệu/m². Khoảng cách ~47 lần trong cùng 1 địa giới hành chính minh họa rõ vì sao phân tích cần chi tiết đến cấp Phường/Xã thay vì chỉ dừng ở cấp Quận/Thành phố.

**Phân bố — theo loại hình BĐS và tỷ lệ bán/cho thuê:**
![Phân bố theo loại hình BĐS và tỷ lệ bán/cho thuê](assets/dashboard/05_distribution_and_split.png)

**Phân bố — giá trung bình/m² theo loại hình BĐS:**
![Giá trung bình/m² theo loại hình BĐS](assets/dashboard/06_avg_price_by_property_type.png)

## Điều phối

![Danh sách 4 DAG Airflow kèm lịch sử chạy](assets/airflow/dags_overview.png)

Airflow điều phối 4 DAG, retry task lỗi và cung cấp khả năng quan sát các run theo lịch hoặc chạy thủ công. Screenshot thể hiện các DAG và lịch sử chạy trong quá trình phát triển local.

## Cấu trúc thư mục

```
RealEstateDW/
├── assets/                 # Ảnh dùng cho README (sơ đồ ERD, screenshot dashboard + Airflow)
│   ├── erd/star_schema.png
│   ├── dashboard/*.png
│   └── airflow/dags_overview.png
│
├── crawler/                # DAG 1 (dataset) + DAG 2 (web crawler)
│   ├── config.py
│   ├── proxy_manager.py
│   ├── dataset_loader_core.py / dataset_loader_io.py
│   └── web_crawler_core.py / web_crawler_io.py
│
├── dags/                   # Chỉ khai báo lịch chạy Airflow — không chứa logic nghiệp vụ
│   ├── dataset_loader.py
│   ├── web_crawler.py
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
│
├── data/                   # Sample data
│
├── maps/                   # GeoJson dùng cho Metabase, dựa trên dữ liệu từ gis.vn
│   ├── hcm_post_merge_wards_metabase_without_condao.geojson
│   ├── hcm_post_merge_wards_metabase.geojson
│   ├── hcm_pre_merge_districts_metabase_without_condao.geojson
│   └── hcm_pre_merge_districts_metabase.geojson
│
├── parser/                 # DAG 3 (Spark parse) + DAG 4 (Silver -> Gold SQL)
│   ├── config.py
│   ├── bronze_to_silver_core.py / bronze_to_silver_io.py
│   ├── bronze_file_state_io.py
│   └── silver_to_gold_io.py
│
├── sql/
│   ├── schema_full.sql             # DDL đầy đủ: 3 schema, 16 bảng, 2 hàm, 1 view
│   ├── dashboard_metabase_queries.sql
│   └── queries/
│       ├── merge_scd2_listing_history.sql
│       ├── etl_silver_to_gold.sql
│       ├── validate_gold_load.sql
│       └── diagnose_gold_join_loss.sql
│
├── docker-compose.yaml     # Airflow + PostgreSQL (x2) + Redis + Metabase
├── Dockerfile                # Airflow 3.3.0 + Java + JDBC driver
└── requirements.txt
```

## Hướng dẫn chạy thử

**Yêu cầu:** Docker và Docker Compose. Chỉ cần AWS credentials và S3 bucket khi chạy đầy đủ luồng Bronze; các file parquet mẫu cho phép chạy thử local mà không cần bộ seed private.

```bash
# 1. Clone repo
git clone https://github.com/nhatphuong99/RealEstateDW.git
cd RealEstateDW

# 2. Cấu hình môi trường
cp .env.example .env
# chỉnh .env: thông tin Postgres, secret Airflow và cấu hình S3 nếu cần

# 3. Khởi động toàn bộ stack
docker compose up -d

# 4. Khởi tạo schema database
psql -h localhost -p 5433 -U dw_admin -d real_estate_dw -f sql/schema_full.sql

# 5. Mở Airflow UI tại http://localhost:8080 và unpause các DAG
# 6. Mở Metabase tại http://localhost:3000 và kết nối tới postgres-dw
```

> Đây là cấu hình cho local development (`docker-compose.yaml` được chỉnh sửa từ file compose tham chiếu chính thức của Airflow). Không khuyến nghị dùng nguyên bản này cho production.

Bộ dữ liệu seed lịch sử private và CDN endpoint tương ứng được cố ý lược bỏ khỏi repository. Để chạy đầy đủ luồng historical load, cần cung cấp parquet tương đương và cấu hình các biến dataset trong `.env`; nếu không, hãy dùng các file mẫu từ web crawler để kiểm tra các bước downstream.

`dataset_loader` không thể chạy chỉ với repository này vì input lịch sử là private. Chỉ sử dụng DAG này sau khi cung cấp input riêng.

## Tác giả

**Nguyễn Lý Nhật Phương**

- **GitHub:** [nhatphuong99](https://github.com/nhatphuong99)
- **Email:** [Nhatphuong NL](mailto:nlnhatphuong@gmail.com)

