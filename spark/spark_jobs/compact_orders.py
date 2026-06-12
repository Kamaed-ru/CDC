from pyspark.sql import SparkSession
import os
import sys

process_date = sys.argv[1]

S3_ENDPOINT_URL = os.environ["S3_ENDPOINT_URL"]
S3_BUCKET = os.environ["S3_BUCKET"]
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

spark = (
    SparkSession.builder
    .appName("CompactOrders")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT_URL)
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

raw_path = (
    f"s3a://{S3_BUCKET}/raw/orders/"
    f"event_date={process_date}"
)

silver_path = (
    f"s3a://{S3_BUCKET}/silver/orders/"
    f"event_date={process_date}"
)

try:
    df = spark.read.parquet(raw_path)
except Exception:
    print(f"No data found: {raw_path}")
    spark.stop()
    sys.exit(0)

(
    df
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(silver_path)
)

spark.stop()
