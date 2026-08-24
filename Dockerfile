FROM apache/airflow:3.3.0

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# JDBC driver — ghi/đọc Postgres qua .write.jdbc()/.read.jdbc()
RUN mkdir -p /opt/spark-jars \
    && curl -fL -o /opt/spark-jars/postgresql-42.7.13.jar \
       https://jdbc.postgresql.org/download/postgresql-42.7.13.jar

# S3A connector — đọc trực tiếp s3a:// (Task 11, Phương án A).
# PySpark 4.2.0 đóng gói Hadoop 3.5 -> cần hadoop-aws 3.5.0.
# QUAN TRỌNG: từ Hadoop 3.4.0 trở đi, hadoop-aws đã chuyển sang AWS SDK V2
# (HADOOP-18073) -> dependency KHÔNG còn là com.amazonaws:aws-java-sdk-bundle
# (v1) nữa mà là software.amazon.awssdk:bundle (v2). Version 2.35.4 lấy
# TRỰC TIẾP từ release notes chính thức Hadoop 3.5.0 (không tự resolve qua
# .pom nữa — lần trước resolve động bị lỗi vì giả định sai dependency v1,
# pin cứng theo nguồn công bố chính thức là cách an toàn hơn ở đây).
RUN curl -fL -o /opt/spark-jars/hadoop-aws-3.5.0.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar \
    && curl -fL -o /opt/spark-jars/bundle-2.35.4.jar \
        https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar \
    && chown -R airflow:0 /opt/spark-jars

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt