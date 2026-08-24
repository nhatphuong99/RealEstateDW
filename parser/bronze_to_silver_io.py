"""
parser/bronze_to_silver_io.py

I/O layer cho ETL Bronze -> Silver (Phase 2). Import boto3/pyspark/
psycopg2 — đây là module DUY NHẤT trong package parser được phép import
3 thư viện I/O này, đúng nguyên tắc tách biệt core/io đã áp dụng cho
crawler/web_crawler_core.py & crawler/web_crawler_io.py.

parser/bronze_to_silver_core.py (pure logic, Phase 1, đã hoàn thành)
KHÔNG import module này — chỉ nhận tham số qua function argument.

Thứ tự trong file: Task 9 (SparkSession) -> Task 10 (parse_partition) ->
Task 11 (đọc Bronze qua s3a://) -> Task 12 (split + ghi JDBC), đúng thứ
tự phụ thuộc: Task 12 dùng output Task 10, Task 11 cung cấp input cho
Task 10, cả 3 đều cần SparkSession từ Task 9.
"""

from __future__ import annotations

import glob
import os
import sys
from decimal import Decimal
from typing import Iterator, Optional
from urllib.parse import urlparse

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from parser.bronze_to_silver_core import ParseError, ParsedListing, parse_listing_html
from parser.config import (
    SPARK_APP_NAME,
    SPARK_DRIVER_MEMORY,
    SPARK_JARS_DIR,
    SPARK_JARS_PACKAGES,
    SPARK_MASTER,
    get_postgres_dsn,
    get_spark_s3a_hadoop_conf,
    s3a_uri,
)


# ---------------------------------------------------------------------------
# Task 9 — Dựng SparkSession
# ---------------------------------------------------------------------------


def _collect_jars(jars_dir: str) -> str:
    """Gom TẤT CẢ *.jar trong jars_dir thành 1 chuỗi phân cách bởi dấu
    phẩy (đúng định dạng tham số spark.jars). Dùng glob thay vì liệt kê
    tên file cứng — tên aws-java-sdk-bundle-*.jar có version ĐỘNG
    (Dockerfile tự resolve đúng version khớp hadoop-aws lúc build), hard-
    code tên dễ lệch giữa các lần build image khác nhau.
    """
    jar_paths = sorted(glob.glob(os.path.join(jars_dir, "*.jar")))
    if not jar_paths:
        raise RuntimeError(
            f"Không tìm thấy jar nào trong {jars_dir!r} — kiểm tra lại "
            "Dockerfile đã tải đủ JDBC driver + hadoop-aws + "
            "aws-java-sdk-bundle chưa."
        )
    return ",".join(jar_paths)


def build_spark_session() -> SparkSession:
    """Dựng SparkSession dùng chung cho toàn bộ ETL Bronze->Silver.

    Đọc config trực tiếp từ parser/config.py (module-level constant) —
    hàm này thuộc I/O layer nên được phép đọc config trực tiếp (khác với
    core luôn nhận tham số qua argument — Phương án B đã chốt cho Task 9).

    2 phần cấu hình:
      1. spark.jars — nạp JDBC driver (ghi Postgres, Task 12) + hadoop-aws
         + aws-java-sdk-bundle (đọc s3a://, Task 11) cùng lúc.
      2. fs.s3a.* — set qua tiền tố "spark.hadoop." để Spark tự đẩy vào
         hadoopConfiguration() của SparkContext lúc khởi tạo, không cần
         gọi tay spark.sparkContext._jsc.hadoopConfiguration().set(...)
         sau khi session đã dựng xong.
    """
    if not os.getenv("AIRFLOW_HOME"):
        venv_scripts = os.path.dirname(sys.executable)
        os.environ["PATH"] = venv_scripts + os.pathsep + os.environ.get("PATH", "")
        os.environ["PYSPARK_PYTHON"] = "python"
        os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

    builder = (
        SparkSession.builder.appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.pyspark.python", "python")
        .config("spark.pyspark.driver.python", "python")
    )

    # Docker nạp bộ JAR ETL riêng trong /opt/spark-jars. Trên Windows,
    # không truyền toàn bộ 276 JAR đi kèm PySpark qua spark.jars vì sẽ vượt
    # giới hạn độ dài command line; Spark tự dùng classpath mặc định của nó.
    if os.getenv("AIRFLOW_HOME"):
        builder = builder.config("spark.jars", _collect_jars(SPARK_JARS_DIR))
    elif os.getenv("SPARK_JARS_DIR"):
        builder = builder.config("spark.jars", _collect_jars(SPARK_JARS_DIR))
    else:
        builder = builder.config("spark.jars.packages", SPARK_JARS_PACKAGES)

    for key, value in get_spark_s3a_hadoop_conf().items():
        builder = builder.config(f"spark.hadoop.{key}", value)

    return builder.getOrCreate()


# ---------------------------------------------------------------------------
# Task 10 — parse_partition(): wrapper mapPartitions gọi parse_listing_html()
# ---------------------------------------------------------------------------

# Thứ tự cột PHẢI khớp _to_output_row() bên dưới — khớp đúng thứ tự cột
# silver.listing_staging_batch (005_etl_bronze_to_silver_control.sql),
# CHỈ THIẾU row_hash (Postgres tự tính, Spark không gửi giá trị này —
# quyết định đã chốt ở Task 12b). Thêm error_reason/raw_html ở cuối cho
# nhánh quarantine.
UNIFIED_PARSE_SCHEMA = StructType(
    [
        StructField("listing_id", LongType(), nullable=True),
        StructField("listing_url", StringType(), nullable=False),
        StructField("source_part", StringType(), nullable=True),
        StructField("source_bronze_key", StringType(), nullable=False),
        StructField("crawl_date", TimestampType(), nullable=False),
        StructField("title", StringType(), nullable=True),
        StructField("listing_type", StringType(), nullable=True),
        StructField("property_type", StringType(), nullable=True),
        StructField("posted_date", DateType(), nullable=True),
        StructField("price_vnd", DecimalType(16, 0), nullable=True),
        StructField("price_raw", StringType(), nullable=True),
        StructField("price_is_negotiable", BooleanType(), nullable=True),
        StructField("area_m2", DecimalType(10, 2), nullable=True),
        StructField("area_raw", StringType(), nullable=True),
        StructField("area_is_undetermined", BooleanType(), nullable=True),
        StructField("length_m", DecimalType(6, 2), nullable=True),
        StructField("width_m", DecimalType(6, 2), nullable=True),
        StructField("street_width_m", DecimalType(6, 2), nullable=True),
        StructField("floors", ShortType(), nullable=True),
        StructField("bedrooms", ShortType(), nullable=True),
        StructField("orientation", StringType(), nullable=True),
        StructField("legal_status", StringType(), nullable=True),
        StructField("has_dining_room", BooleanType(), nullable=True),
        StructField("has_kitchen", BooleanType(), nullable=True),
        StructField("has_rooftop", BooleanType(), nullable=True),
        StructField("has_car_parking", BooleanType(), nullable=True),
        StructField("owner_direct", BooleanType(), nullable=True),
        StructField("is_expired", BooleanType(), nullable=True),
        StructField("has_warning", BooleanType(), nullable=True),
        StructField("address_street_new", StringType(), nullable=True),
        StructField("address_ward_new", StringType(), nullable=True),
        StructField("address_province_new", StringType(), nullable=True),
        StructField("address_old_raw", StringType(), nullable=True),
        StructField("address_ward_old", StringType(), nullable=True),
        StructField("address_district_old", StringType(), nullable=True),
        # --- 2 cột chỉ có giá trị ở nhánh quarantine ---
        StructField("error_reason", StringType(), nullable=True),
        StructField("raw_html", BinaryType(), nullable=True),
    ]
)


def _decimal_or_none(value: Optional[Decimal]) -> Optional[Decimal]:
    # Decimal(None) sẽ lỗi -> pass-through None, Spark tự hiểu là SQL NULL.
    return value


def _to_output_row(result) -> Row:
    """Map ParsedListing | ParseError -> Row đúng thứ tự UNIFIED_PARSE_SCHEMA.

    Không dùng dict trực tiếp (Row(**kwargs)) vì thứ tự field trong Row
    dựng từ dict không đảm bảo khớp StructType khi Spark toDF(schema) —
    an toàn nhất là liệt kê positional đúng thứ tự.
    """
    if isinstance(result, ParsedListing):
        return Row(
            result.listing_id,
            result.listing_url,
            result.source_part,
            result.source_bronze_key,
            result.crawl_date,
            result.title,
            result.listing_type,
            result.property_type,
            result.posted_date,
            _decimal_or_none(result.price_vnd),
            result.price_raw,
            result.price_is_negotiable,
            _decimal_or_none(result.area_m2),
            result.area_raw,
            result.area_is_undetermined,
            _decimal_or_none(result.length_m),
            _decimal_or_none(result.width_m),
            _decimal_or_none(result.street_width_m),
            result.floors,
            result.bedrooms,
            result.orientation,
            result.legal_status,
            result.has_dining_room,
            result.has_kitchen,
            result.has_rooftop,
            result.has_car_parking,
            result.owner_direct,
            result.is_expired,
            result.has_warning,
            result.address_street_new,
            result.address_ward_new,
            result.address_province_new,
            result.address_old_raw,
            result.address_ward_old,
            result.address_district_old,
            None,  # error_reason
            None,  # raw_html
        )

    if isinstance(result, ParseError):
        # 28 cột giữa crawl_date và error_reason (title..address_district_old)
        # đều None — chỉ success mới có giá trị.
        return Row(
            None,  # listing_id
            result.listing_url,
            None,  # source_part
            result.source_bronze_key,
            result.crawl_date,
            *([None] * 28),  # title..address_district_old
            result.error_reason,
            result.raw_html,
        )

    raise TypeError(f"parse_listing_html() trả kiểu không mong đợi: {type(result)!r}")


def parse_partition(source_part: str, source_bronze_key: str):
    """Factory trả về hàm dùng cho rdd.mapPartitions() — đóng gói source_part/
    source_bronze_key (cố định cho CẢ 1 file Bronze đang xử lý) qua closure,
    vì parse_listing_html() cần 2 tham số này nhưng chúng KHÔNG có sẵn trong
    từng dòng parquet (chỉ có url/crawl_date/html theo Bronze schema thống
    nhất — xem source_and_bronze_analysis.md).

    Dùng mapPartitions (không phải UDF) — lý do đã chốt ở Phase 2 Task 10:
    ~30 field output không vectorize được (parser gọi BeautifulSoup per-
    row), mapPartitions tránh overhead serialize từng row riêng lẻ như UDF
    thường gặp phải.
    """

    def _process_partition(rows: Iterator[Row]) -> Iterator[Row]:
        for row in rows:
            result = parse_listing_html(
                html=row.html,
                listing_url=row.url,
                crawl_date=row.crawl_date,
                source_part=source_part,
                source_bronze_key=source_bronze_key,
            )
            yield _to_output_row(result)

    return _process_partition


# ---------------------------------------------------------------------------
# Task 11 — Đọc 1 file parquet Bronze cụ thể từ S3 qua s3a:// (Phương án A)
# ---------------------------------------------------------------------------

# Schema Bronze THỐNG NHẤT giữa DAG 1 (dataset) và DAG 2 (web) — theo
# source_and_bronze_analysis.md: url, crawl_date, html (raw bytes, KHÔNG
# Base64). Đây là "hợp đồng" giữa 2 DAG ingestion và parser chung — validate
# ngay lúc đọc để fail sớm, rõ ràng, thay vì để lỗi lộ ra mù mờ sau này ở
# Task 10 (vd AttributeError: 'Row' object has no attribute 'html').
_BRONZE_REQUIRED_COLUMNS = {"url", "crawl_date", "html"}


def _validate_bronze_schema(df: DataFrame, s3_key: str) -> None:
    actual_columns = set(df.columns)
    missing = _BRONZE_REQUIRED_COLUMNS - actual_columns
    if missing:
        raise RuntimeError(
            f"File Bronze {s3_key!r} thiếu cột bắt buộc {missing} — "
            f"cột hiện có: {sorted(actual_columns)}. Kiểm tra lại DAG "
            "ingestion (dataset_loader/web_crawler) có đúng schema thống "
            "nhất không."
        )


def read_bronze_parquet(spark: SparkSession, s3_key: str) -> DataFrame:
    """Đọc 1 file parquet Bronze cụ thể từ S3 qua s3a:// (Phương án A đã
    chốt — Spark đọc trực tiếp, không tải tạm qua boto3).

    s3_key: đường dẫn tương đối trong bucket, vd
    'bronze/dataset/part=1.parquet' hoặc 'bronze/web/2026-08-24/xxx.parquet'
    — LUÔN đọc ĐÚNG 1 file cụ thể (không dùng wildcard '*.parquet') vì
    control-plane (crawl.bronze_file_state, Task 18-19) xử lý theo TỪNG
    file, cần biết chính xác file nào đang parse để update status đúng
    dòng. Nếu Phase 5 cần tăng tốc, sẽ là run_etl_bronze_to_silver()
    (Task 19) tự loop/submit song song gọi hàm này nhiều lần — không sửa
    lại hàm này.

    Chỉ select đúng 3 cột bắt buộc (không select(*)) — phòng trường hợp
    file Bronze có thêm cột thừa ngoài dự kiến, giữ output luôn đúng hợp
    đồng schema cho Task 10 (parse_partition() chỉ cần row.url/
    row.crawl_date/row.html).
    """
    uri = s3a_uri(s3_key)
    df = spark.read.parquet(uri)
    _validate_bronze_schema(df, s3_key)
    return df.select("url", "crawl_date", "html")


# ---------------------------------------------------------------------------
# Task 12 — Split success/quarantine (12a) + ghi JDBC (12b)
# ---------------------------------------------------------------------------

# Cột thuộc silver.listing_staging_batch — đúng UNIFIED_PARSE_SCHEMA,
# TRỪ error_reason/raw_html (chỉ quarantine mới có) và row_hash (Postgres
# tự tính qua GENERATED STORED, Spark KHÔNG gửi giá trị này — quyết định
# đã chốt Task 12b, tránh lệch công thức hash Python/Postgres).
_STAGING_COLUMNS = [
    "listing_id", "listing_url", "source_part", "source_bronze_key",
    "crawl_date", "title", "listing_type", "property_type", "posted_date",
    "price_vnd", "price_raw", "price_is_negotiable", "area_m2", "area_raw",
    "area_is_undetermined", "length_m", "width_m", "street_width_m",
    "floors", "bedrooms", "orientation", "legal_status", "has_dining_room",
    "has_kitchen", "has_rooftop", "has_car_parking", "owner_direct",
    "is_expired", "has_warning", "address_street_new", "address_ward_new",
    "address_province_new", "address_old_raw", "address_ward_old",
    "address_district_old",
]


def split_success_and_quarantine(combined_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Task 12a — tách combined_df (output của Task 10, schema =
    UNIFIED_PARSE_SCHEMA) thành 2 DataFrame dựa vào error_reason IS NULL,
    thay vì gọi mapPartitions() 2 lần riêng (sẽ chạy parse_listing_html()
    2 LẦN, tốn gấp đôi công parse HTML — đã loại phương án đó).
    """
    success_df = combined_df.filter(col("error_reason").isNull()).select(*_STAGING_COLUMNS)

    # parse_quarantine dùng tên cột "url" (không phải "listing_url") —
    # xem 005_etl_bronze_to_silver_control.sql. Alias lại cho khớp đích.
    quarantine_df = (
        combined_df.filter(col("error_reason").isNotNull())
        .select(
            col("listing_url").alias("url"),
            "crawl_date",
            "source_bronze_key",
            "error_reason",
            "raw_html",
        )
    )
    return success_df, quarantine_df


def _jdbc_url_and_properties(dsn: str) -> tuple[str, dict[str, str]]:
    """Chuyển DSN dạng 'postgresql://user:pass@host:port/db' (dùng bởi
    psycopg2, get_postgres_dsn()) sang jdbc:postgresql://host:port/db +
    properties riêng user/password (Spark JDBC KHÔNG parse được DSN kiểu
    psycopg2 trực tiếp, cần 2 phần tách biệt).
    """
    parsed = urlparse(dsn)
    jdbc_url = f"jdbc:postgresql://{parsed.hostname}:{parsed.port}{parsed.path}"
    properties = {
        "user": parsed.username or "",
        "password": parsed.password or "",
        "driver": "org.postgresql.Driver",
    }
    return jdbc_url, properties


def write_staging_and_quarantine(combined_df: DataFrame) -> tuple[int, int]:
    """Task 12b — ghi 2 DataFrame vào silver.listing_staging_batch và
    silver.parse_quarantine qua JDBC.

    Mode CHỈ 'append' — KHÔNG dùng 'overwrite' (SaveMode.Overwrite của
    Spark JDBC sẽ DROP+CREATE lại bảng theo schema Spark tự suy luận,
    XÓA MẤT cột GENERATED row_hash — đã loại phương án này ở bước phân
    tích trước, xem row_hash trong 005_etl_bronze_to_silver_control.sql).

    TRUNCATE silver.listing_staging_batch trước mỗi batch KHÔNG nằm
    trong hàm này — đó là trách nhiệm của run_etl_bronze_to_silver()
    (Phase 4, Task 19, orchestrator), giữ đúng single responsibility:
    hàm này chỉ ghi, không quản lý vòng đời batch.

    Trả về (success_count, quarantine_count) để Task 13 smoke test đối
    chiếu với SELECT COUNT(*) thực tế trên Postgres.
    """
    success_df, quarantine_df = split_success_and_quarantine(combined_df)

    # Cache trước khi count() + write() — tránh Spark chạy lại toàn bộ
    # mapPartitions() (gồm cả BeautifulSoup parse) 2 lần cho cùng 1 DataFrame.
    success_df.cache()
    quarantine_df.cache()

    success_count = success_df.count()
    quarantine_count = quarantine_df.count()

    jdbc_url, properties = _jdbc_url_and_properties(get_postgres_dsn())

    success_df.write.jdbc(
        url=jdbc_url,
        table="silver.listing_staging_batch",
        mode="append",
        properties=properties,
    )
    quarantine_df.write.jdbc(
        url=jdbc_url,
        table="silver.parse_quarantine",
        mode="append",
        properties=properties,
    )

    success_df.unpersist()
    quarantine_df.unpersist()

    return success_count, quarantine_count