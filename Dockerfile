FROM apache/airflow:3.3.0

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# JDBC driver — ghi/đọc Postgres qua .write.jdbc()/.read.jdbc().
RUN mkdir -p /opt/spark-jars \
    && curl -fL -o /opt/spark-jars/postgresql-42.7.13.jar \
       https://jdbc.postgresql.org/download/postgresql-42.7.13.jar \
    && chown -R airflow:0 /opt/spark-jars

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt