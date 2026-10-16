# Entry point producer: muat event Olist, tambah event Faker, urutkan, sisipkan event rusak, lalu replay ke RabbitMQ
import argparse
import json
import logging
import os
import sys
import time
from collections import Counter
from pathlib import Path

import pika
from dotenv import load_dotenv
from pika.exceptions import AMQPError

from event_schema import validate_event
from faker_events import corrupt_events, generate_stock_events
from olist_loader import load_olist_events
from replay import REPLAY_MODES, replay_events

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOG_EVERY = 500

logger = logging.getLogger("order_producer")


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


# Baca env berupa rate (0 sampai 1) dan tolak nilai di luar rentang
def env_rate(name, default):
    value = env_float(name, default)
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"{name} harus di antara 0 dan 1, bukan {value}")
    return value


# Kumpulkan semua konfigurasi producer dari .env
def load_config():
    data_dir = Path(env_str("OLIST_DATA_DIR", "data/raw"))
    if not data_dir.is_absolute():
        data_dir = PROJECT_ROOT / data_dir

    mode = env_str("REPLAY_MODE", "time_compression")
    if mode not in REPLAY_MODES:
        raise ValueError(f"REPLAY_MODE harus salah satu dari {REPLAY_MODES}, bukan {mode}")

    return {
        "data_dir": data_dir,
        "max_orders": env_int("MAX_ORDERS", 0),
        "seed": env_int("FAKER_SEED", 42),
        "mode": mode,
        "speed_factor": env_float("SPEED_FACTOR", 3600),
        "max_sleep": env_float("MAX_SLEEP_SECONDS", 1),
        "events_per_second": env_float("EVENTS_PER_SECOND", 50),
        "duplicate_rate": env_rate("DUPLICATE_RATE", 0.02),
        "missing_field_rate": env_rate("MISSING_FIELD_RATE", 0.02),
        "negative_price_rate": env_rate("NEGATIVE_PRICE_RATE", 0.01),
        "stock_per_order": env_float("STOCK_EVENTS_PER_ORDER", 0.3),
        "rabbit_host": env_str("RABBITMQ_HOST", "localhost"),
        "rabbit_port": env_int("RABBITMQ_PORT", 5672),
        "rabbit_user": env_str("RABBITMQ_USER", "guest"),
        "rabbit_password": env_str("RABBITMQ_PASSWORD", "guest"),
        "exchange": env_str("RABBITMQ_EXCHANGE", "orders.events"),
    }


# Bangun stream event final: Olist plus stok Faker, diurutkan berdasarkan event_time, lalu disisipi event rusak
def build_event_stream(config):
    olist_events = load_olist_events(
        config["data_dir"], config["max_orders"], config.get("orders_offset", 0)
    )
    stock_events = generate_stock_events(olist_events, config["stock_per_order"], config["seed"])
    merged = sorted(olist_events + stock_events, key=lambda event: event["event_time"])
    return corrupt_events(
        merged,
        config["duplicate_rate"],
        config["missing_field_rate"],
        config["negative_price_rate"],
        config["seed"],
    )


# Buka koneksi dan channel RabbitMQ dengan publisher confirm aktif
def connect_rabbitmq(config):
    credentials = pika.PlainCredentials(config["rabbit_user"], config["rabbit_password"])
    parameters = pika.ConnectionParameters(
        host=config["rabbit_host"],
        port=config["rabbit_port"],
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=300,
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.confirm_delivery()
    return connection, channel


# Buka koneksi RabbitMQ dengan retry dan jeda bertambah, kembalikan None kalau semua percobaan gagal
def connect_with_retry(config, attempts=5, base_delay=2.0):
    for attempt in range(1, attempts + 1):
        try:
            return connect_rabbitmq(config)
        except (AMQPError, OSError) as error:
            logger.warning(
                "koneksi RabbitMQ gagal, percobaan %d dari %d: %s",
                attempt,
                attempts,
                type(error).__name__,
            )
            if attempt < attempts:
                time.sleep(base_delay * attempt)
    return None


# Buat fungsi publish: routing key = event_type, pesan persistent, log progres tiap LOG_EVERY event
def make_publisher(channel, exchange):
    state = {"sent": 0}

    def publish(event):
        channel.basic_publish(
            exchange=exchange,
            routing_key=event["event_type"],
            body=json.dumps(event, separators=(",", ":")).encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                message_id=str(event.get("event_id") or ""),
            ),
            mandatory=True,
        )
        state["sent"] += 1
        if state["sent"] % LOG_EVERY == 0:
            logger.info("terkirim %d event", state["sent"])

    publish.state = state
    return publish


# Tampilkan ringkasan stream tanpa mengirim apa pun, untuk cek hasil sebelum publish
def print_dry_run_summary(events, stats):
    by_type = Counter(event.get("event_type") for event in events)
    invalid = sum(1 for event in events if validate_event(event))
    logger.info("total event: %d", len(events))
    for event_type, count in sorted(by_type.items()):
        logger.info("  %s: %d", event_type, count)
    logger.info("event rusak yang disisipkan: %s", stats)
    logger.info("event gagal validasi: %d", invalid)
    for event in events[:3]:
        logger.info("contoh: %s", json.dumps(event))


# Baca argumen command line
def parse_args():
    parser = argparse.ArgumentParser(description="Producer event order Olist ke RabbitMQ")
    parser.add_argument("--dry-run", action="store_true", help="hanya tampilkan ringkasan, tidak kirim")
    parser.add_argument("--max-events", type=int, default=0, help="batasi jumlah event yang dikirim (0 = semua)")
    parser.add_argument("--skip-events", type=int, default=0, help="lewati N event pertama sebelum mengirim")
    parser.add_argument("--orders-offset", type=int, default=0, help="lewati N order pertama, untuk memakai order yang belum pernah dikirim")
    return parser.parse_args()


# Alur utama producer
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pika").setLevel(logging.CRITICAL)
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()

    try:
        config = load_config()
        config["orders_offset"] = args.orders_offset
        events, stats = build_event_stream(config)
    except (ValueError, FileNotFoundError) as error:
        logger.error("%s", error)
        return 1

    if args.skip_events > 0:
        events = events[args.skip_events :]
    if args.max_events > 0:
        events = events[: args.max_events]

    if args.dry_run:
        print_dry_run_summary(events, stats)
        return 0

    logger.info("mulai replay %d event, mode %s", len(events), config["mode"])
    connected = connect_with_retry(config)
    if connected is None:
        logger.error(
            "tidak bisa terhubung ke RabbitMQ di %s:%d, pastikan broker berjalan dan kredensial di .env benar",
            config["rabbit_host"],
            config["rabbit_port"],
        )
        return 1
    connection, channel = connected
    publisher = make_publisher(channel, config["exchange"])
    try:
        sent = replay_events(
            events,
            publisher,
            config["mode"],
            config["speed_factor"],
            config["max_sleep"],
            config["events_per_second"],
            sleep_fn=connection.sleep,
        )
        logger.info("selesai, total terkirim %d event", sent)
    except KeyboardInterrupt:
        logger.info("dihentikan manual")
    except (AMQPError, OSError) as error:
        sent = publisher.state["sent"]
        logger.error("replay terhenti setelah %d event terkirim: %s", sent, type(error).__name__)
        logger.error(
            "untuk melanjutkan, jalankan ulang dengan konfigurasi yang sama dan --skip-events %d",
            args.skip_events + sent,
        )
        return 1
    finally:
        try:
            connection.close()
        except (AMQPError, OSError):
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
