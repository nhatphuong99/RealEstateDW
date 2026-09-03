"""
parser/bronze_to_silver_io.py

Thành phần 3 (ETL Bronze -> Silver) — I/O layer, module duy nhất trong
parser được phép import boto3/pyspark. Thứ tự: dựng SparkSession (Bước 2)
-> đọc Bronze S3 (Bước 2) -> parse_partition (Bước 3) -> split + ghi JDBC (Bước 5).
"""

from __future__ import annotations

import glob
import os
import tempfile
from decimal import Decimal
from typing import Iterator, Optional
from urllib.parse import urlparse

import boto3
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    BinaryType, BooleanType, DateType, DecimalType, LongType, ShortType,
    StringType, StructField, StructType, TimestampType,
)

from parser.bronze_to_silver_core import ParseError, ParsedListing, is_in_scope, parse_listing_html
from parser.config import (
    BRONZE_TMP_DIR_PREFIX,
    SPARK_APP_NAME,
    SPARK_DRIVER_MEMORY,
    SPARK_JARS_DIR,
    SPARK_MASTER,
    get_postgres_dsn,
    get_s3_bucket,
)


# ---------------------------------------------------------------------------
# Bước 2 — Dựng SparkSession
# ---------------------------------------------------------------------------


def _collect_jars(jars_dir: str) -> str:
    jar_paths = sorted(glob.glob(os.path.join(jars_dir, "*.jar")))
    if not jar_paths:
        raise RuntimeError(
            f"Không tìm thấy jar nào trong {jars_dir!r} — kiểm tra lại "
            "Dockerfile đã tải JDBC driver chưa."
        )
    return ",".join(jar_paths)


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.jars", _collect_jars(SPARK_JARS_DIR))
        # Tắt vectorized reader: cột html dài biến động lớn, dễ OOM.
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Bước 3 — parse_partition(): wrapper mapPartitions gọi parse_listing_html()
# ---------------------------------------------------------------------------

# Thứ tự cột phải khớp _to_output_row() và silver.listing_staging_batch,
# trừ row_hash (Postgres tự tính) và thêm error_reason/raw_html cho quarantine.
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
        StructField("price_is_outlier", BooleanType(), nullable=True),
        StructField("area_m2", DecimalType(10, 2), nullable=True),
        StructField("area_raw", StringType(), nullable=True),
        StructField("area_is_undetermined", BooleanType(), nullable=True),
        StructField("area_is_outlier", BooleanType(), nullable=True),
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
        StructField("address_province_old", StringType(), nullable=True),
        # 2 cột chỉ có giá trị ở nhánh quarantine
        StructField("error_reason", StringType(), nullable=True),
        StructField("raw_html", BinaryType(), nullable=True),
    ]
)


def _decimal_or_none(value: Optional[Decimal]) -> Optional[Decimal]:
    # Decimal(None) sẽ lỗi -> pass-through None, Spark tự hiểu là SQL NULL.
    return value


def _to_output_row(result) -> Row:
    """Map ParsedListing | ParseError -> Row theo đúng thứ tự UNIFIED_PARSE_SCHEMA.
    Liệt kê positional (không dùng dict) vì Row(**kwargs) không đảm bảo
    khớp thứ tự StructType."""
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
            result.price_is_outlier,
            _decimal_or_none(result.area_m2),
            result.area_raw,
            result.area_is_undetermined,
            result.area_is_outlier,
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
            result.address_province_old,
            None,  # error_reason
            None,  # raw_html
        )

    if isinstance(result, ParseError):
        # 30 cột giữa crawl_date và error_reason đều None — chỉ success mới có giá trị.
        return Row(
            None,  # listing_id
            result.listing_url,
            None,  # source_part
            result.source_bronze_key,
            result.crawl_date,
            *([None] * 32),  # title..address_province_old
            result.error_reason,
            result.raw_html,
        )

    raise TypeError(f"parse_listing_html() trả kiểu không mong đợi: {type(result)!r}")


def parse_partition(source_part: str, source_bronze_key: str):
    """Factory cho rdd.mapPartitions(): đóng gói source_part/source_bronze_key
    qua closure. Dùng mapPartitions thay UDF để tránh overhead serialize
    từng row (parser gọi BeautifulSoup per-row)."""

    def _process_partition(rows: Iterator[Row]) -> Iterator[Row]:
        for row in rows:
            result = parse_listing_html(
                html=row.html,
                listing_url=row.url,
                crawl_date=row.crawl_date,
                source_part=source_part,
                source_bronze_key=source_bronze_key,
            )
            # Tin ngoài phạm vi -> bỏ qua lặng lẽ, không ghi staging lẫn quarantine.
            if isinstance(result, ParsedListing) and not is_in_scope(result):
                continue
            yield _to_output_row(result)

    return _process_partition


# ---------------------------------------------------------------------------
# Bước 2 — Đọc 1 file parquet Bronze cụ thể từ S3
# ---------------------------------------------------------------------------

_BRONZE_REQUIRED_COLUMNS = {"url", "crawl_date", "html"}


def _validate_bronze_schema(df: DataFrame, s3_key: str) -> None:
    actual_columns = set(df.columns)
    missing = _BRONZE_REQUIRED_COLUMNS - actual_columns
    if missing:
        raise RuntimeError(
            f"File Bronze {s3_key!r} thiếu cột bắt buộc {missing} — "
            f"cột hiện có: {sorted(actual_columns)}."
        )


def download_bronze_file(s3_key: str) -> tempfile.TemporaryDirectory:
    """Tải 1 file Bronze về thư mục tạm local, không cần SparkSession —
    chạy trước khi JVM khởi động, tránh JVM chiếm CPU làm nghẽn download."""
    s3_client = boto3.client("s3")
    bucket = get_s3_bucket()
    tmp_dir = tempfile.TemporaryDirectory(prefix=BRONZE_TMP_DIR_PREFIX)
    local_path = os.path.join(tmp_dir.name, os.path.basename(s3_key))
    s3_client.download_file(bucket, s3_key, local_path)
    return tmp_dir


def read_bronze_parquet(spark: SparkSession, local_path: str, s3_key: str) -> DataFrame:
    """Đọc file Bronze đã tải sẵn vào Spark DataFrame. Vẫn phải .cache() +
    .count() ngay trong hàm do lazy evaluation."""
    df = spark.read.parquet(local_path)
    _validate_bronze_schema(df, s3_key)
    result_df = df.select("url", "crawl_date", "html").repartition(8).cache()
    result_df.count()
    return result_df


# ---------------------------------------------------------------------------
# Bước 5 — Split success/quarantine + ghi JDBC
# ---------------------------------------------------------------------------

# Cột silver.listing_staging_batch theo UNIFIED_PARSE_SCHEMA, trừ
# error_reason/raw_html (chỉ quarantine) và row_hash (Postgres tự tính).
_STAGING_COLUMNS = [
    "listing_id", "listing_url", "source_part", "source_bronze_key",
    "crawl_date", "title", "listing_type", "property_type", "posted_date",
    "price_vnd", "price_raw", "price_is_negotiable", "price_is_outlier",
    "area_m2", "area_raw", "area_is_undetermined", "area_is_outlier",
    "length_m", "width_m", "street_width_m", "floors", "bedrooms",
    "orientation", "legal_status", "has_dining_room", "has_kitchen",
    "has_rooftop", "has_car_parking", "owner_direct", "is_expired",
    "has_warning", "address_street_new", "address_ward_new",
    "address_province_new", "address_old_raw", "address_ward_old",
    "address_district_old", "address_province_old",
]


def split_success_and_quarantine(combined_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Tách theo error_reason IS NULL, tránh gọi mapPartitions() 2 lần."""
    success_df = combined_df.filter(col("error_reason").isNull()).select(*_STAGING_COLUMNS)

    # parse_quarantine dùng tên cột "url" (không phải "listing_url") — alias lại.
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
    """Chuyển DSN psycopg2 sang JDBC URL + user/password."""
    parsed = urlparse(dsn)
    jdbc_url = f"jdbc:postgresql://{parsed.hostname}:{parsed.port}{parsed.path}"
    properties = {
        "user": parsed.username or "",
        "password": parsed.password or "",
        "driver": "org.postgresql.Driver",
    }
    return jdbc_url, properties


def write_staging_and_quarantine(combined_df: DataFrame) -> tuple[int, int]:
    """Ghi vào silver.listing_staging_batch + silver.parse_quarantine qua
    JDBC (mode='append' — giữ cột GENERATED row_hash). TRUNCATE staging
    trước mỗi batch do orchestrator quản lý, không nằm trong hàm này."""
    success_df, quarantine_df = split_success_and_quarantine(combined_df)

    # Cache trước count()+write() — tránh Spark chạy lại mapPartitions() 2 lần.
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
