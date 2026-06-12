from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_date,
    hour,
    to_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)


spark = (
    SparkSession.builder
    .appName("KafkaToS3")
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "http://minio:9000"
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        "minioadmin"
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        "minioadmin"
    )
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    .getOrCreate()
)

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType()),
    StructField("status", StringType()),
    StructField("event_time", StringType())
])

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_ts", to_timestamp("event_time"))
    .withColumn("event_date", to_date("event_ts"))
    .withColumn("event_hour", hour("event_ts"))
)

query = (
    parsed_df.writeStream
    .format("parquet")
    .option(
        "path",
        "s3a://cdc-bucket/raw/orders"
    )
    .option(
        "checkpointLocation",
        "s3a://cdc-bucket/checkpoints/orders"
    )
    .partitionBy("event_date")
    .trigger(processingTime="1 minute")
    .outputMode("append")
    .start()
)

query.awaitTermination()
