"""
parser/config.py

Cấu hình ĐẶC THÙ cho package parser (Spark IO layer, Phase 2 trở đi).
Chỉ chứa tham số riêng của parser (SparkSession, S3A connector). Tham số
DÙNG CHUNG với crawler/ (DSN Postgres, S3 bucket, AWS region,
RUNNING_IN_CONTAINER) import lại từ config.py gốc — KHÔNG định nghĩa
trùng, đúng "Phương án B" đã chốt.

`parser/bronze_to_silver_core.py` (logic thuần) KHÔNG import module này —
core chỉ nhận tham số qua function argument, giữ đúng nguyên tắc tách
biệt logic khỏi I/O đã áp dụng nhất quán cho crawler/web_crawler_core.py.
"""

import importlib.util
import os
from pathlib import Path

# ---------------------------------------------------------------------
# Import config.py GỐC (root, dùng chung toàn project) bằng đường dẫn
# tuyệt đối qua importlib — KHÔNG dùng `import config` trực tiếp vì phụ
# thuộc PYTHONPATH/cwd lúc chạy (khác nhau giữa host và trong container
# Airflow, dễ ModuleNotFoundError hoặc import nhầm module trùng tên tùy
# ngữ cảnh gọi). Cùng tinh thần "cố định theo vị trí file" như cách
# crawler/config.py đang tìm .env, áp dụng lại ở đây cho việc import.
# ---------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ROOT_CONFIG_PATH = _PROJECT_ROOT / "config.py"

_spec = importlib.util.spec_from_file_location("root_config", _ROOT_CONFIG_PATH)
if _spec is None or _spec.loader is None:
    raise RuntimeError(f"Không tìm thấy config.py gốc tại: {_ROOT_CONFIG_PATH}")
root_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(root_config)

# Re-export các hàm/biến DÙNG CHUNG — nơi khác trong package parser chỉ
# cần `from parser.config import get_postgres_dsn, get_s3_bucket`, không
# cần biết chúng thực ra định nghĩa ở root.
get_postgres_dsn = root_config.get_postgres_dsn
get_s3_bucket = root_config.get_s3_bucket
RUNNING_IN_CONTAINER = root_config.RUNNING_IN_CONTAINER
AWS_REGION = root_config.AWS_REGION


# ---------------------------------------------------------------------
# SparkSession (Task 9 — build_spark_session() trong bronze_to_silver_io.py)
# ---------------------------------------------------------------------
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "bronze_to_silver")
# local[*] = dùng hết core máy đang chạy — đủ cho quy mô đồ án (1-2 file
# test ở Phase 2, tối đa 77 part ~764k dòng ở Phase 5), không cần cluster
# thật.
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")

# Thư mục chứa TẤT CẢ jar cần nạp vào Spark (JDBC driver + hadoop-aws +
# aws-java-sdk-bundle) — Dockerfile tải cả 3 vào đây lúc build.
# build_spark_session() tự glob toàn bộ *.jar trong thư mục này thay vì
# liệt kê tên file cứng, vì tên file aws-java-sdk-bundle-*.jar có version
# ĐỘNG (Dockerfile tự resolve đúng version khớp hadoop-aws lúc build) —
# hard-code tên ở đây dễ lệch nếu version đổi giữa các lần build.
SPARK_JARS_DIR = os.getenv("SPARK_JARS_DIR", "/opt/spark-jars")


# ---------------------------------------------------------------------
# S3A connector (Task 11 — Spark đọc trực tiếp s3a://, Phương án A đã chốt)
# ---------------------------------------------------------------------
# Đọc thẳng AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY đã có sẵn trong .env
# qua provider chuẩn của Hadoop SDK — KHÔNG re-export credentials qua biến
# Python nào ở đây, giữ đúng nguyên tắc "không tăng diện lộ credential".
#
# LƯU Ý: hadoop-aws 3.5.0 dùng AWS SDK V2 (HADOOP-18073, xem Dockerfile) ->
# PHẢI dùng đúng class provider của package software.amazon.awssdk.*,
# class cũ com.amazonaws.auth.EnvironmentVariableCredentialsProvider (v1)
# sẽ không có trên classpath nữa -> ClassNotFoundException lúc chạy job.
SPARK_S3A_CREDENTIALS_PROVIDER = (
    "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider"
)


def get_spark_s3a_hadoop_conf() -> dict[str, str]:
    """Trả dict các key `fs.s3a.*` cần set vào SparkSession.builder.config()..."""
    return {
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "fs.s3a.aws.credentials.provider": SPARK_S3A_CREDENTIALS_PROVIDER,
        "fs.s3a.endpoint.region": AWS_REGION,
        # Hadoop 3.5.0 (HADOOP-19559) đổi cơ chế chọn stream đọc S3A sang
        # key "fs.s3a.input.stream.type" (KHÔNG PHẢI
        # "fs.s3a.analytics.accelerator.enabled" như bản PR gốc
        # HADOOP-19348 — đã thử flag đó, KHÔNG có tác dụng vì code 3.5.0
        # không còn đọc key cũ). Giá trị mặc định "analytics" trong
        # 3.5.0 đúng cho trường hợp Spark+Parquet (giống hệt use-case ở
        # đây) -> cần "classic" (stream truyền thống) để KHÔNG đòi hỏi
        # jar analyticsaccelerator-s3 riêng (chưa tải, không cần ở quy
        # mô đồ án).
        "fs.s3a.input.stream.type": "classic",
    }


def s3a_uri(key: str) -> str:
    """Dựng URI s3a://<bucket>/<key> từ 1 S3 key — dùng ở Task 11 khi đọc
    parquet Bronze cụ thể: `spark.read.parquet(s3a_uri(key))`."""
    return f"s3a://{get_s3_bucket()}/{key}"