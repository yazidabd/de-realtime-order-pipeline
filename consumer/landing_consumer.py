# Consumer RabbitMQ: baca event, tulis micro-batch JSON lines ke landing zone untuk Spark, kirim pesan rusak ke DLQ
import json
import logging
import os
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pika
from dotenv import load_dotenv
from pika.exceptions import AMQPError
from prometheus_client import Counter, Gauge, Histogram, start_http_server

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Daftar tipe event yang dikenal, sama dengan EVENT_TYPES di producer/event_schema.py
KNOWN_EVENT_TYPES = (
    "order_created",
    "order_item",
    "payment",
    "order_status_changed",
    "stock_updated",
)

POLL_TIMEOUT_SECONDS = 1.0
DEPTH_CHECK_SECONDS = 5.0
RECONNECT_DELAY_SECONDS = 5

EVENTS_CONSUMED = Counter(
    "consumer_events_consumed_total", "Event yang ditulis ke landing zone", ["event_type"]
)
EVENTS_DEAD_LETTERED = Counter(
    "consumer_events_dead_lettered_total", "Pesan yang dikirim ke DLQ", ["reason"]
)
BATCHES_WRITTEN = Counter("consumer_batches_written_total", "Jumlah file batch yang ditulis")
LAST_BATCH_SIZE = Gauge("consumer_last_batch_size", "Jumlah event di batch terakhir")
QUEUE_DEPTH = Gauge(
    "consumer_queue_depth", "Jumlah pesan yang menunggu di queue utama (lag consumer)"
)
LANDING_LATENCY = Histogram(
    "consumer_landing_latency_seconds",
    "Selisih publish_time sampai event ditulis ke landing zone",
    buckets=(0.1, 0.25, 0.5, 1, 2, 3, 4, 5, 6, 7, 8, 10, 15, 20, 30, 60, 120, 300),
)

logger = logging.getLogger("landing_consumer")


# Baca env bertipe string, nilai kosong dianggap tidak diisi lalu pakai default
def env_str(name, default):
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip()


# Baca env bertipe integer dengan default
def env_int(name, default):
    return int(env_str(name, str(default)))


# Baca env bertipe float dengan default
def env_float(name, default):
    return float(env_str(name, str(default)))


# Kumpulkan konfigurasi consumer dari .env, path relatif dihitung dari root project
def load_config():
    landing_dir = Path(env_str("LANDING_DIR", "data/landing"))
    if not landing_dir.is_absolute():
        landing_dir = PROJECT_ROOT / landing_dir

    config = {
        "landing_dir": landing_dir,
        "batch_size": env_int("BATCH_SIZE", 500),
        "flush_seconds": env_float("BATCH_FLUSH_SECONDS", 5),
        "metrics_port": env_int("CONSUMER_METRICS_PORT", 8000),
        "rabbit_host": env_str("RABBITMQ_HOST", "127.0.0.1"),
        "rabbit_port": env_int("RABBITMQ_PORT", 5672),
        "rabbit_user": env_str("RABBITMQ_USER", "guest"),
        "rabbit_password": env_str("RABBITMQ_PASSWORD", "guest"),
        "queue": env_str("RABBITMQ_QUEUE", "orders.events.queue"),
    }
    if config["batch_size"] <= 0 or config["flush_seconds"] <= 0:
        raise ValueError("BATCH_SIZE dan BATCH_FLUSH_SECONDS harus lebih besar dari 0")
    return config


# Cek envelope satu pesan, kembalikan (event, None) kalau layak atau (None, alasan) kalau harus ke DLQ
def inspect_message(body):
    try:
        event = json.loads(body)
    except ValueError:
        return None, "invalid_json"
    if not isinstance(event, dict):
        return None, "not_object"
    if event.get("event_type") not in KNOWN_EVENT_TYPES:
        return None, "unknown_event_type"
    for field in ("event_id", "event_time"):
        if event.get(field) in (None, ""):
            return None, f"missing_{field}"
    return event, None


# Hitung detik sejak waktu ISO 8601 tertentu, kembalikan None kalau nilainya kosong atau rusak
def seconds_since(iso_value):
    if not iso_value:
        return None
    try:
        moment = datetime.fromisoformat(iso_value.replace("Z", "+00:00"))
    except (TypeError, ValueError, AttributeError):
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - moment).total_seconds()


# Tulis satu batch sebagai JSON lines: tulis ke file temp tersembunyi, fsync, lalu rename atomik supaya Spark tidak baca file setengah jadi
def write_batch(landing_dir, events):
    landing_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    name = f"orders-{stamp}-{uuid.uuid4().hex[:8]}.json"
    temp_path = landing_dir / f".tmp-{name}"
    final_path = landing_dir / name

    with open(temp_path, "w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp_path, final_path)
    return final_path


# Tulis buffer ke landing zone, baru ack ke RabbitMQ (at-least-once), lalu update metrik
def flush_buffer(channel, buffer, landing_dir):
    events = [event for _, event in buffer]
    path = write_batch(landing_dir, events)
    channel.basic_ack(delivery_tag=buffer[-1][0], multiple=True)

    for event in events:
        EVENTS_CONSUMED.labels(event["event_type"]).inc()
        latency = seconds_since(event.get("publish_time"))
        if latency is not None:
            LANDING_LATENCY.observe(max(latency, 0.0))
    BATCHES_WRITTEN.inc()
    LAST_BATCH_SIZE.set(len(events))
    logger.info("batch ditulis: %s (%d event)", path.name, len(events))
    buffer.clear()


# Baca jumlah pesan di queue utama lewat channel terpisah dan simpan ke gauge lag
def update_queue_depth(depth_channel, queue_name):
    try:
        result = depth_channel.queue_declare(queue=queue_name, passive=True)
        QUEUE_DEPTH.set(result.method.message_count)
    except AMQPError as error:
        logger.warning("gagal baca queue depth: %s", error)


# Tutup koneksi tanpa melempar error kalau sudah terputus
def close_quietly(connection):
    try:
        if connection.is_open:
            connection.close()
    except (AMQPError, OSError):
        pass


# Satu sesi koneksi: consume pesan, kumpulkan jadi batch, flush kalau penuh atau waktunya habis
def consume_loop(config, stop):
    credentials = pika.PlainCredentials(config["rabbit_user"], config["rabbit_password"])
    parameters = pika.ConnectionParameters(
        host=config["rabbit_host"],
        port=config["rabbit_port"],
        credentials=credentials,
        heartbeat=60,
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    depth_channel = connection.channel()
    channel.basic_qos(prefetch_count=config["batch_size"])
    logger.info("terhubung ke RabbitMQ, consume dari %s", config["queue"])

    buffer = []
    buffer_started = None
    last_depth_check = 0.0

    try:
        for method, _properties, body in channel.consume(
            queue=config["queue"], inactivity_timeout=POLL_TIMEOUT_SECONDS
        ):
            if method is not None:
                event, reason = inspect_message(body)
                if reason:
                    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    EVENTS_DEAD_LETTERED.labels(reason).inc()
                else:
                    if not buffer:
                        buffer_started = time.monotonic()
                    buffer.append((method.delivery_tag, event))

            now = time.monotonic()
            full = len(buffer) >= config["batch_size"]
            expired = bool(buffer) and now - buffer_started >= config["flush_seconds"]
            if full or expired:
                flush_buffer(channel, buffer, config["landing_dir"])
                buffer_started = None

            if now - last_depth_check >= DEPTH_CHECK_SECONDS:
                update_queue_depth(depth_channel, config["queue"])
                last_depth_check = now

            if stop.is_set():
                break

        if buffer:
            flush_buffer(channel, buffer, config["landing_dir"])
        channel.cancel()
    finally:
        close_quietly(connection)


# Jalankan consume_loop terus, kalau koneksi putus tunggu sebentar lalu sambung lagi
def run(config, stop):
    while not stop.is_set():
        try:
            consume_loop(config, stop)
        except (AMQPError, OSError) as error:
            logger.warning(
                "koneksi RabbitMQ gagal (%s: %s), coba lagi dalam %d detik",
                type(error).__name__,
                error,
                RECONNECT_DELAY_SECONDS,
            )
            stop.wait(RECONNECT_DELAY_SECONDS)


# Alur utama: baca config, nyalakan endpoint metrik, pasang handler shutdown, lalu jalankan consumer
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pika").setLevel(logging.CRITICAL)
    load_dotenv(PROJECT_ROOT / ".env")

    try:
        config = load_config()
    except ValueError as error:
        logger.error("%s", error)
        return 1

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda signum, frame: stop.set())
    signal.signal(signal.SIGTERM, lambda signum, frame: stop.set())

    start_http_server(config["metrics_port"])
    logger.info("metrik di port %d, landing zone %s", config["metrics_port"], config["landing_dir"])

    run(config, stop)
    logger.info("consumer berhenti")
    return 0


if __name__ == "__main__":
    sys.exit(main())
