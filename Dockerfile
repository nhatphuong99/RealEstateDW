FROM apache/airflow:3.3.0

USER root

# default-jre-headless -> tạo symlink JAVA_HOME ổn định (/usr/lib/jvm/default-java)
# bất kể amd64/arm64, không cần biết chính xác tên gói openjdk-XX-...
# Spark 4.x yêu cầu Java 17+; Debian bookworm (base image) mặc định JRE 17 -> khớp.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Tải JDBC driver để Spark ghi/đọc Postgres qua .write.jdbc()/.read.jdbc()
RUN mkdir -p /opt/spark-jars \
    && curl -fL -o /opt/spark-jars/postgresql-42.7.13.jar \
       https://jdbc.postgresql.org/download/postgresql-42.7.13.jar \
    && chown -R airflow:0 /opt/spark-jars

USER airflow

# Cài TẤT CẢ Python deps (bao gồm pyspark) NGAY LÚC BUILD image,
# thay vì _PIP_ADDITIONAL_REQUIREMENTS (cài lại MỖI LẦN container khởi động
# — quá chậm cho pyspark, gói tải về ~300MB mỗi lần start).
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt