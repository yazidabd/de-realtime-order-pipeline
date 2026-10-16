# Job batch compact: baca bronze lake/events, buang duplikat event_id, tulis hasilnya sebagai silver lake/silver/events
import os
import sys
import time

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# Baca env bertipe string, nilai kosong dianggap tidak diisi lalu pakai default
def env_str(name, default):
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip()


# Kumpulkan konfigurasi job dari environment variable
def load_config():
    return {
        "checkpoint_dir": env_str("CHECKPOINT_DIR", "/checkpoints"),
        "bucket": env_str("S3_BUCKET_NAME", ""),
        "prefix": env_str("S3_PREFIX", "lake").strip("/"),
        "region": env_str("AWS_REGION", "ap-southeast-1"),
    }


# Bangun SparkSession, overwrite partisi dinamis supaya hanya partisi yang ditulis ulang yang diganti
def build_spark(config):
    builder = (
        SparkSession.builder.appName("de-realtime-order-compact")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if config["bucket"]:
        builder = (
            builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{config['region']}.amazonaws.com")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# Tentukan lokasi data lake: S3 kalau bucket diisi, kalau tidak folder lokal di dalam data/checkpoints
def lake_base_path(config):
    if config["bucket"]:
        return f"s3a://{config['bucket']}/{config['prefix']}"
    return f"{config['checkpoint_dir']}/local_lake"


# Ambil satu baris per event_id, yang publish_ts paling awal, lalu tambah kolom event_month untuk partisi silver
def build_silver(bronze):
    ordering = Window.partitionBy("event_id").orderBy(
        F.col("publish_ts").asc(), F.col("publish_time").asc()
    )
    return (
        bronze.withColumn("row_rank", F.row_number().over(ordering))
        .filter(F.col("row_rank") == 1)
        .drop("row_rank", "publish_date")
        .withColumn("event_month", F.date_format("event_ts", "yyyy-MM"))
    )


# Cetak ringkasan hasil compact, kembalikan True kalau silver tepat satu baris per event_id unik di bronze
def report_and_verify(bronze_rows, distinct_ids, silver):
    silver_rows = silver.count()
    silver_distinct = silver.select("event_id").distinct().count()
    print(f"compact: bronze rows {bronze_rows}", flush=True)
    print(f"compact: bronze distinct event_id {distinct_ids}", flush=True)
    print(f"compact: duplikat dibuang {bronze_rows - silver_rows}", flush=True)
    print(f"compact: silver rows {silver_rows}", flush=True)
    for row in silver.groupBy("event_type").count().orderBy("event_type").collect():
        print(f"compact: silver {row['event_type']} {row['count']}", flush=True)
    ok = silver_rows == distinct_ids and silver_distinct == silver_rows
    print(f"compact: verifikasi {'OK' if ok else 'GAGAL'}", flush=True)
    return ok, silver_rows


# Alur utama: baca bronze, dedup, verifikasi, tulis silver, lalu baca ulang silver untuk memastikan jumlahnya sama
def main():
    config = load_config()
    spark = build_spark(config)
    base = lake_base_path(config)
    bronze_path = f"{base}/events"
    silver_path = f"{base}/silver/events"
    print(f"compact: bronze {bronze_path}", flush=True)

    try:
        bronze = spark.read.parquet(bronze_path).persist()
        bronze_rows = bronze.count()
    except AnalysisException as error:
        print(f"compact: bronze tidak bisa dibaca: {error}", flush=True)
        return 1
    if bronze_rows == 0:
        print("compact: bronze kosong, tidak ada yang diproses", flush=True)
        return 0

    distinct_ids = bronze.select("event_id").distinct().count()
    silver = build_silver(bronze).persist()
    ok, silver_rows = report_and_verify(bronze_rows, distinct_ids, silver)
    if not ok:
        print("compact: silver tidak ditulis karena verifikasi gagal", flush=True)
        return 1

    print("compact: menulis silver, satu file per partisi", flush=True)
    started = time.monotonic()
    (
        silver.repartition("event_type", "event_month")
        .write.mode("overwrite")
        .partitionBy("event_type", "event_month")
        .parquet(silver_path)
    )
    print(f"compact: tulis selesai dalam {time.monotonic() - started:.0f} detik", flush=True)
    written = spark.read.parquet(silver_path).count()
    print(f"compact: silver ditulis ke {silver_path}, terbaca ulang {written} baris", flush=True)
    return 0 if written == silver_rows else 1


if __name__ == "__main__":
    sys.exit(main())
