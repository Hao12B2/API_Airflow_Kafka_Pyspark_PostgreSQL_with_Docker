FROM apache/airflow:latest-python3.12

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install requests json5 kafka-python apache-airflow apache-airflow-providers-apache-spark pyspark psycopg2-binary