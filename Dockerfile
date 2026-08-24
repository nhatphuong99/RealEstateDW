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

# S3A connector đầy đủ cho Hadoop 3.5.0 — Task 11, Phương án A.
# 3 jar dưới đây PHẢI đi cùng nhau, đã đối chiếu đúng version qua pom.xml
# chính thức của hadoop-aws:3.5.0 (Maven Central) + pom.xml của Apache
# Spark (dùng để xác nhận cặp version bundle/analyticsaccelerator tương
# thích, vì Spark tự build/test với đúng cặp version này):
#   1. hadoop-aws-3.5.0.jar        — S3AFileSystem, đọc s3a://
#   2. bundle-2.35.4.jar           — AWS SDK V2 (Hadoop 3.4+ đã chuyển
#      hẳn sang SDK V2, không còn dùng com.amazonaws:aws-java-sdk-bundle
#      của SDK V1 nữa — HADOOP-18073)
#   3. analyticsaccelerator-s3-1.3.1.jar — dependency SCOPE=COMPILE của
#      hadoop-aws:3.5.0 (xác nhận qua .pom chính thức), KHÔNG PHẢI
#      optional dù tính năng Analytics Accelerator có bật hay không —
#      thiếu jar này S3AFileSystem không LOAD ĐƯỢC (NoClassDefFoundError
#      ngay cả khi fs.s3a.input.stream.type=classic).
RUN curl -fL -o /opt/spark-jars/hadoop-aws-3.5.0.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar \
    && curl -fL -o /opt/spark-jars/bundle-2.35.4.jar \
        https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar \
    && curl -fL -o /opt/spark-jars/analyticsaccelerator-s3-1.3.1.jar \
        https://repo1.maven.org/maven2/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar \
    && chown -R airflow:0 /opt/spark-jars

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt