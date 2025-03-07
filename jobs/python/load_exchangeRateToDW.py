from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType
from pyspark.sql.functions import from_json, to_timestamp, col
import time

spark = SparkSession \
    .builder \
    .appName("ExchangeRateToDW") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

schema = StructType([
    StructField("date", StringType(), True),
    StructField("base", StringType(), True),
    StructField("USD", FloatType(), True),
    StructField("EUR", FloatType(), True),
    StructField("GBP", FloatType(), True),
    StructField("SEK", FloatType(), True),
    StructField("CHF", FloatType(), True),
    StructField("INR", FloatType(), True),
    StructField("CNY", FloatType(), True),
    StructField("HKD", FloatType(), True),
    StructField("JPY", FloatType(), True),
    StructField("THB", FloatType(), True),
    StructField("VND", FloatType(), True)
])

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9092, kafka2:9093, kafka3:9094") \
    .option("subscribe", "exchangerate") \
    .option("startingOffsets", "earliest") \
    .load()

df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

df = df.withColumn("date", to_timestamp(col("date"), "EEE, dd MMM yyyy HH:mm:ss Z"))

query = df.write \
    .mode("append") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/airflow") \
    .option("dbtable", "exchangerate") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .save() 

spark.stop()

# spark-submit --master spark://spark-master:7077 --jars /opt/bitnami/spark/jars/kafka-postgresql/spark-sql-kafka-0-10_2.12-3.5.5.jar,/opt/bitnami/spark/jars/kafka-postgresql/kafka-clients-3.4.0.jar,/opt/bitnami/spark/jars/kafka-postgresql/postgresql-42.7.5.jar,/opt/bitnami/spark/jars/kafka-postgresql/commons-pool2-2.11.1.jar,/opt/bitnami/spark/jars/kafka-postgresql/spark-streaming-kafka-0-10_2.12-3.3.0.jar,/opt/bitnami/spark/jars/kafka-postgresql/spark-token-provider-kafka-0-10_2.12-3.3.0.jar jobs/python/load_exchangeRateToDW.py
# docker-compose exec postgres psql -U airflow -d airflow -c 'CREATE TABLE IF NOT EXISTS "exchangerate" ( date TIMESTAMP, base VARCHAR(255), USD FLOAT, EUR FLOAT, GBP FLOAT, SEK FLOAT, CHF FLOAT, INR FLOAT, CNY FLOAT, HKD FLOAT, JPY FLOAT, THB FLOAT, VND FLOAT );'
# docker-compose exec postgres psql -U airflow -d airflow -c 'SELECT * FROM exchangerate LIMIT 10;'