# Hitung event di lake/events pada S3 lewat Spark, dijalankan di dalam container spark
import os

from pyspark.sql import SparkSession

bucket = os.environ["S3_BUCKET_NAME"]
region = os.environ["AWS_REGION"]
prefix = os.environ.get("S3_PREFIX", "lake")

spark = (
    SparkSession.builder.master("local[2]")
    .appName("check-lake")
    .config("spark.ui.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

events = spark.read.parquet(f"s3a://{bucket}/{prefix}/events")
print("lake_events total", events.count(), "distinct event_id", events.select("event_id").distinct().count())
