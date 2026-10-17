# de-realtime-order-pipeline

[![ci](https://github.com/yazidabd/de-realtime-order-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/yazidabd/de-realtime-order-pipeline/actions/workflows/ci.yml)

Pipeline streaming untuk event e-commerce. Dataset Olist diputar ulang sebagai stream, diproses lewat RabbitMQ dan Spark Structured Streaming, lalu disimpan ke data lake Parquet di S3 dan serving layer MongoDB, dengan monitoring Prometheus dan Grafana.

## Hasil terukur

Semua angka berasal dari run nyata di satu mesin (Spark local[2]), bukan klaim kapasitas produksi. Detail di docs.

| Ukuran | Hasil | Rincian |
|--------|-------|---------|
| Uji beban lima tahap | 560000 event terkirim, 555705 masuk landing dan 4295 masuk DLQ, tidak ada yang hilang | docs/load-test.md |
| Konsistensi | 536341 event unik, sama di ledger MongoDB dan data lake S3 | docs/load-test.md |
| Kapasitas Spark | Sekitar 1800 event per detik, diukur dari pengurasan backlog | docs/load-test.md |
| Ingress teramati | 352 event per detik dengan 4 producer, batas atas broker belum ditemukan | docs/load-test.md |
| Latency sampai landing zone | 99 persen di bawah 2 detik pada beban sekitar 290 per detik | docs/load-test.md |
| Uji kegagalan | 10 skenario (consumer, Spark, RabbitMQ, MongoDB dimatikan paksa dan lainnya), semuanya pulih dengan data konsisten | docs/failure-drills.md |
| Uji clone baru | Mengikuti README dari nol menghasilkan angka yang identik, 105 test lulus | docs/architecture.md |

## Arsitektur

```
producer -> RabbitMQ -> consumer -> landing zone -> Spark -> S3 Parquet dan MongoDB
                                                      Prometheus -> Grafana
```

Detail desain, hasil verifikasi, dan keterbatasan ada di docs/architecture.md. Kamus data ada di docs/data-dictionary.md. Hasil uji kegagalan ada di docs/failure-drills.md dan hasil load test ada di docs/load-test.md.

## Stack

| Lapisan | Teknologi |
|---------|-----------|
| Sumber data | Olist Brazilian E-Commerce, Faker |
| Messaging | RabbitMQ (exchange, queue, dead letter queue) |
| Pemrosesan | Spark Structured Streaming, Spark SQL |
| Data lake | Parquet di AWS S3 |
| Serving | MongoDB (single node replica set) |
| Monitoring | Prometheus, Grafana |
| Runtime | Docker Compose, Python |

## Prasyarat

Docker dengan Docker Compose, Python 3.10 atau lebih baru, dan AWS CLI (hanya bila memakai S3).

## Menjalankan

1. Buat .env dan folder data lewat skrip berikut. Skrip mengisi nilai bawaan, membuat password acak, dan menolak menimpa .env yang sudah ada.

```bash
bash infra/init_env.sh
```
2. Unduh dataset Olist dari Kaggle (Brazilian E-Commerce Public Dataset by Olist) dan taruh file CSV di data/raw/. Yang dipakai: orders, order_items, order_payments, dan products.
3. Jalankan broker dan database, lalu pastikan keduanya healthy:

```bash
docker compose up -d rabbitmq mongodb
docker compose ps
```

4. Buat user aplikasi RabbitMQ (diperlukan karena load_definitions mengabaikan RABBITMQ_DEFAULT_USER):

```bash
bash infra/setup_rabbitmq_user.sh
```

5. Opsional, data lake di S3. Isi S3_BUCKET_NAME dan credential AWS di .env, siapkan profile AWS CLI bernama de-dev, lalu:

```bash
bash infra/create_s3_bucket.sh
```

Tanpa S3_BUCKET_NAME, Parquet ditulis lokal ke data/checkpoints/local_lake. Bila Spark sudah pernah berjalan tanpa S3 lalu S3 diaktifkan, hapus data/checkpoints/lake_events dan data/checkpoints/lake_rejected (butuh sudo) lalu restart Spark, supaya data di landing zone ditulis ulang ke S3.

6. Jalankan consumer, Spark, dan monitoring:

```bash
docker compose up -d consumer spark prometheus grafana
```

7. Jalankan producer:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r producer/requirements.txt
python producer/order_producer.py --dry-run
python producer/order_producer.py
```

Gunakan REPLAY_MODE=fixed_rate untuk uji cepat, contoh: REPLAY_MODE=fixed_rate EVENTS_PER_SECOND=500 python producer/order_producer.py --max-events 3000

## Antarmuka lokal

| Layanan | Alamat |
|---------|--------|
| RabbitMQ management | http://localhost:15672 |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Spark UI | http://localhost:4040 |

Dashboard Grafana bernama Pipeline Overview di folder Pipeline.

## Konfigurasi

Variabel non-secret dan nilai yang disarankan:

| Variabel | Nilai |
|----------|-------|
| OLIST_DATA_DIR | data/raw |
| MAX_ORDERS | 2000 (0 untuk semua order) |
| FAKER_SEED | 42 |
| REPLAY_MODE | time_compression atau fixed_rate |
| SPEED_FACTOR | 3600 |
| MAX_SLEEP_SECONDS | 1 |
| EVENTS_PER_SECOND | 50 |
| DUPLICATE_RATE, MISSING_FIELD_RATE, NEGATIVE_PRICE_RATE | 0.02, 0.02, 0.01 |
| STOCK_EVENTS_PER_ORDER | 0.3 |
| RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_MANAGEMENT_PORT | 127.0.0.1, 5672, 15672 |
| RABBITMQ_USER | de_app |
| RABBITMQ_EXCHANGE, RABBITMQ_QUEUE, RABBITMQ_DLQ | orders.events, orders.events.queue, orders.events.dlq |
| LANDING_DIR, CHECKPOINT_DIR | data/landing, data/checkpoints |
| BATCH_SIZE, BATCH_FLUSH_SECONDS, CONSUMER_METRICS_PORT | 500, 5, 8000 |
| TRIGGER_INTERVAL_SECONDS, WINDOW_DURATION, WATERMARK_DELAY | 10, "1 minute", "30 minutes" |
| MAX_FILES_PER_TRIGGER | 60 (jumlah file landing per micro-batch Spark) |
| MONGO_HOST, MONGO_PORT, MONGO_USER, MONGO_DB | localhost, 27017, mongo_admin, orders_pipeline |
| AWS_REGION, S3_PREFIX | ap-southeast-1, lake |

Secret (RABBITMQ_PASSWORD, MONGO_PASSWORD, MONGO_REPLICA_KEY, GRAFANA_ADMIN_PASSWORD) dibuat acak oleh infra/init_env.sh. Hanya AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, dan S3_BUCKET_NAME yang dikosongkan dan diisi sendiri bila memakai S3. File .env tidak masuk repo.

## Pola replay

| Mode | Perilaku |
|------|----------|
| time_compression | Delay antar event = min(selisih event_time dibagi SPEED_FACTOR, MAX_SLEEP_SECONDS) |
| fixed_rate | Kirim EVENTS_PER_SECOND event per detik tanpa melihat selisih timestamp |

## Compact (layer silver)

Bronze (lake/events) bisa berisi duplikat, misalnya bila event yang sama diputar ulang melewati watermark dedup. Job compact membaca bronze, menyisakan satu baris per event_id, dan menulis lake/silver/events. Aman dijalankan berulang kali.

```bash
docker compose run --rm compact
```

## Tests

```bash
pip install -r producer/requirements.txt -r consumer/requirements.txt -r requirements-dev.txt
ruff check .
python -m pytest tests -q
```

## Menghentikan

```bash
docker compose down
docker compose down -v
```

Perintah pertama menghentikan semua service dan mempertahankan data. Perintah kedua juga menghapus semua volume. Untuk mengosongkan landing zone, checkpoint, MongoDB, dan prefix lake di S3 sekaligus, jalankan bash infra/reset_state.sh --yes (menghapus data, pakai hati-hati).

## Kredit dan lisensi data

Dataset: Brazilian E-Commerce Public Dataset by Olist, dipublikasikan di Kaggle dengan lisensi CC BY-NC-SA 4.0. Dataset tidak disertakan di repo ini dan hanya dipakai untuk tujuan non-komersial.
