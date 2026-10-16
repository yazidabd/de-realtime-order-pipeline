# Job Spark Structured Streaming: baca landing zone, validasi, dedup, lalu tulis ke Parquet dan MongoDB
import os
import sys
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

import sinks
from schemas import EVENT_SCHEMA, NON_NEGATIVE_FIELDS, REQUIRED_FIELDS

# Format timestamp event: 2017-10-02T10:56:33.000Z
TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"


# Baca env bertipe string, nilai kosong dianggap tidak diisi lalu pakai default
def env_str(name, default):
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip()


# Kumpulkan konfigurasi job dari environment variable
def load_config():
    return {
        "landing_dir": env_str("LANDING_DIR", "/landing"),
        "checkpoint_dir": env_str("CHECKPOINT_DIR", "/checkpoints"),
        "trigger_seconds": int(env_str("TRIGGER_INTERVAL_SECONDS", "10")),
        "max_files": int(env_str("MAX_FILES_PER_TRIGGER", "60")),
        "window_duration": env_str("WINDOW_DURATION", "1 minute"),
        "watermark_delay": env_str("WATERMARK_DELAY", "30 minutes"),
        "bucket": env_str("S3_BUCKET_NAME", ""),
        "prefix": env_str("S3_PREFIX", "lake").strip("/"),
        "region": env_str("AWS_REGION", "ap-southeast-1"),
    }


# Bangun SparkSession lokal, konfigurasi s3a hanya dipasang kalau S3_BUCKET_NAME diisi
def build_spark(config):
    builder = (
        SparkSession.builder.appName("de-realtime-order-pipeline")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
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


# Kolom boolean: True kalau kolom tidak null dan tidak kosong
def is_present(name):
    column = F.col(name)
    return column.isNotNull() & (F.trim(column.cast("string")) != "")


# Kolom boolean: True kalau semua field wajib untuk tipe event pada baris itu terisi
def required_fields_ok():
    expression = F.lit(False)
    for event_type, fields in REQUIRED_FIELDS.items():
        checks = [is_present("event_id"), is_present("event_time")]
        checks.extend(is_present(field) for field in fields)
        expression = F.when(F.col("event_type") == event_type, reduce(lambda a, b: a & b, checks)).otherwise(
            expression
        )
    return expression


# Kolom boolean: True kalau salah satu field numerik bernilai negatif
def has_negative_value():
    checks = [F.coalesce(F.col(field) < 0, F.lit(False)) for field in NON_NEGATIVE_FIELDS]
    return reduce(lambda a, b: a | b, checks)


# Baca landing zone sebagai stream, parse timestamp, lalu beri error_reason pada event yang tidak lolos validasi
def read_and_validate(spark, config):
    raw = (
        spark.readStream.schema(EVENT_SCHEMA)
        .option("maxFilesPerTrigger", config["max_files"])
        .json(config["landing_dir"])
    )
    parsed = raw.withColumn("event_ts", F.to_timestamp("event_time", TIME_FORMAT)).withColumn(
        "publish_ts", F.to_timestamp("publish_time", TIME_FORMAT)
    )
    known_type = F.coalesce(F.col("event_type").isin(*REQUIRED_FIELDS.keys()), F.lit(False))
    return parsed.withColumn(
        "error_reason",
        F.when(~known_type, "unknown_event_type")
        .when(~required_fields_ok(), "missing_field")
        .when(F.col("event_ts").isNull() | F.col("publish_ts").isNull(), "bad_timestamp")
        .when(has_negative_value(), "negative_value"),
    )


# Pisahkan event valid (diberi watermark publish_ts, dedup per event_id) dan event ditolak
def build_streams(spark, config):
    tagged = read_and_validate(spark, config)
    rejected = tagged.filter(F.col("error_reason").isNotNull())
    clean = (
        tagged.filter(F.col("error_reason").isNull())
        .withColumn(
            "order_status_value",
            F.when(F.col("event_type") == "order_created", F.col("order_status")).otherwise(
                F.col("new_status")
            ),
        )
        .withColumn("publish_date", F.to_date("publish_ts"))
        .withWatermark("publish_ts", config["watermark_delay"])
        .dropDuplicatesWithinWatermark(["event_id"])
        .drop("error_reason")
    )
    return clean, rejected


# Jalankan satu query yang menulis Parquet (append) dengan partisi tertentu
def start_parquet(df, name, path, partitions, config):
    return (
        df.writeStream.queryName(name)
        .format("parquet")
        .outputMode("append")
        .option("path", path)
        .option("checkpointLocation", f"{config['checkpoint_dir']}/{name}")
        .partitionBy(*partitions)
        .trigger(processingTime=f"{config['trigger_seconds']} seconds")
        .start()
    )


# Jalankan satu query yang menyerahkan tiap micro-batch ke fungsi foreachBatch
def start_foreach(df, name, batch_fn, config):
    return (
        df.writeStream.queryName(name)
        .foreachBatch(batch_fn)
        .outputMode("append")
        .option("checkpointLocation", f"{config['checkpoint_dir']}/{name}")
        .trigger(processingTime=f"{config['trigger_seconds']} seconds")
        .start()
    )


# Listener yang mencetak ringkasan tiap micro-batch (nama query, nomor batch, baris, laju, durasi) untuk analisis beban
class ProgressLogger(StreamingQueryListener):
    # Query dimulai, tidak ada yang dicatat
    def onQueryStarted(self, event):
        pass

    # Cetak satu baris progres untuk batch yang berisi data, kegagalan logging tidak boleh mengganggu query
    def onQueryProgress(self, event):
        try:
            progress = event.progress
            if progress.numInputRows > 0:
                trigger_ms = progress.durationMs.get("triggerExecution")
                rate = progress.processedRowsPerSecond or 0
                print(
                    f"progress query={progress.name} batch={progress.batchId} "
                    f"rows={progress.numInputRows} proc_rows_per_sec={rate:.0f} trigger_ms={trigger_ms}",
                    flush=True,
                )
        except Exception:
            pass

    # Query idle, tidak ada yang dicatat
    def onQueryIdle(self, event):
        pass

    # Query berhenti, tidak ada yang dicatat
    def onQueryTerminated(self, event):
        pass


# Alur utama: bangun stream, nyalakan empat query, lalu tunggu sampai ada yang berhenti
def main():
    config = load_config()
    spark = build_spark(config)
    spark.streams.addListener(ProgressLogger())
    clean, rejected = build_streams(spark, config)
    base = lake_base_path(config)
    print(f"lokasi data lake: {base}", flush=True)

    start_parquet(clean, "lake_events", f"{base}/events", ["event_type", "publish_date"], config)
    start_parquet(rejected, "lake_rejected", f"{base}/rejected", ["error_reason"], config)
    start_foreach(clean, "mongo_serving", sinks.write_serving_batch, config)
    start_foreach(
        clean, "mongo_metrics", sinks.make_metrics_writer(config["window_duration"]), config
    )

    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
