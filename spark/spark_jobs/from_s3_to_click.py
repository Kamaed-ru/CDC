from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_timestamp,
    to_date
)
import os
import sys

process_date = sys.argv[1]

S3_ENDPOINT_URL = os.environ["S3_ENDPOINT_URL"]
S3_BUCKET = os.environ["S3_BUCKET"]
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

CLICKHOUSE_HOST = os.environ["CLICKHOUSE_HOST"]
CLICKHOUSE_DB = os.environ["CLICKHOUSE_DB"]
CLICKHOUSE_USER = os.environ["CLICKHOUSE_USER"]
CLICKHOUSE_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]

spark = (
    SparkSession.builder
    .appName("LoadOrdersToClickHouse")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT_URL)
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

silver_path = (
    f"s3a://{S3_BUCKET}/silver/orders/"
    f"event_date={process_date}"
)

try:
    df = spark.read.parquet(silver_path)
except Exception:
    print(f"No data found: {silver_path}")
    spark.stop()
    sys.exit(0)

df = (
    df
    .withColumn(
        "event_time",
        to_timestamp(
            "event_time",
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
        )
    )
    .withColumn(
        "event_date",
        to_date("event_time")
    )
    .select(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "price",
        "status",
        "event_time",
        "event_date"
    )
)

df.printSchema()

(
    df.write
    .format("jdbc")
    .option(
        "url",
        f"jdbc:clickhouse://{CLICKHOUSE_HOST}:8123/{CLICKHOUSE_DB}"
    )
    .option(
        "driver",
        "com.clickhouse.jdbc.ClickHouseDriver"
    )
    .option(
        "dbtable",
        "orders"
    )
    .option(
        "user",
        CLICKHOUSE_USER
    )
    .option(
        "password",
        CLICKHOUSE_PASSWORD
    )
    .mode("append")
    .save()
)

spark.stop()