# Data Dictionary

Semua timestamp berformat ISO 8601 UTC dengan milidetik, contoh 2017-10-02T10:56:33.000Z.

## Event

Field umum di semua tipe event:

| Field | Tipe | Keterangan |
|-------|------|------------|
| event_id | string | uuid5 deterministik dari tipe event dan identitasnya |
| event_type | string | order_created, order_item, payment, order_status_changed, atau stock_updated |
| event_time | string | Waktu event menurut data sumber atau aturan offset |
| publish_time | string | Waktu event dikirim producer, ditambahkan saat replay |

### order_created

| Field | Tipe | Keterangan |
|-------|------|------------|
| order_id | string | Kunci order dari Olist |
| customer_id | string | Kunci customer dari Olist |
| order_status | string | Selalu created, status lanjutan datang dari order_status_changed |

### order_item

| Field | Tipe | Keterangan |
|-------|------|------------|
| order_id | string | Kunci order |
| order_item_id | long | Urutan item dalam order, mulai dari 1 |
| product_id | string | Kunci produk |
| seller_id | string | Kunci seller |
| product_category_name | string | Kategori dari tabel products, boleh kosong, bukan field wajib |
| price | double | Tidak boleh negatif |
| freight_value | double | Tidak boleh negatif |
| shipping_limit_date | string | Atribut biasa, bukan event_time |

### payment

| Field | Tipe | Keterangan |
|-------|------|------------|
| order_id | string | Kunci order |
| payment_sequential | long | Urutan pembayaran dalam order |
| payment_type | string | Contoh credit_card, boleto, voucher |
| payment_installments | long | Jumlah cicilan |
| payment_value | double | Tidak boleh negatif |

### order_status_changed

| Field | Tipe | Keterangan |
|-------|------|------------|
| order_id | string | Kunci order |
| new_status | string | approved, shipped, atau delivered |

### stock_updated

Dibuat Faker karena Olist tidak punya data inventori.

| Field | Tipe | Keterangan |
|-------|------|------------|
| product_id | string | Dipilih dari produk yang muncul di event order_item |
| warehouse_id | string | Salah satu dari WH-SP-01, WH-RJ-01, WH-MG-01, WH-RS-01 |
| change_type | string | restock, sale_adjustment, atau correction |
| quantity_change | long | Positif atau negatif sesuai change_type |
| stock_level | long | Level stok setelah perubahan, tidak pernah di bawah 0 |

## RabbitMQ

| Objek | Nama | Keterangan |
|-------|------|------------|
| Exchange | orders.events | Tipe topic, routing key sama dengan event_type |
| Queue | orders.events.queue | Queue utama, di-bind dengan routing key # |
| Dead letter exchange | orders.dlx | Tipe direct, routing key dead |
| Dead letter queue | orders.events.dlq | Menampung pesan yang di-nack consumer |

Alasan masuk DLQ: invalid_json, not_object, unknown_event_type, missing_event_id, missing_event_time.

## Landing zone

File JSON lines berawalan orders-, satu event per baris, maksimal 500 event per file atau di-flush tiap 5 detik. File sementara berawalan .tmp- diabaikan Spark.

## Data lake (Parquet di S3)

| Lokasi | Partisi | Isi |
|--------|---------|-----|
| lake/events | event_type, publish_date | Event valid hasil dedup, semua field event ditambah event_ts, publish_ts, order_status_value |
| lake/rejected | error_reason | Event gagal validasi, semua field event ditambah event_ts, publish_ts, error_reason |
| lake/silver/events | event_type, event_month | Hasil compact: satu baris per event_id (publish_ts paling awal), kolom sama dengan lake/events tanpa publish_date, ditambah event_month |

Kolom turunan:

| Kolom | Keterangan |
|-------|------------|
| event_ts | event_time sebagai timestamp |
| publish_ts | publish_time sebagai timestamp, dasar window dan watermark |
| order_status_value | order_status untuk order_created, new_status untuk order_status_changed |
| publish_date | Tanggal dari publish_ts, dipakai sebagai partisi lake/events |
| event_month | Bulan dari event_ts (yyyy-MM), dipakai sebagai partisi layer silver |
| error_reason | unknown_event_type, missing_field, bad_timestamp, atau negative_value |

## MongoDB (database orders_pipeline)

### orders_current

| Field | Keterangan |
|-------|------------|
| _id | order_id |
| status | Status terbaru, aturan latest wins berdasarkan event_time |
| customer_id | Diisi sekali dari event order_created |
| updated_at | event_time dari event yang menetapkan status terakhir |

### stock_current

| Field | Keterangan |
|-------|------------|
| _id | product_id |
| stock_level | Level stok terbaru |
| warehouse_id | Gudang pada event terbaru |
| updated_at | event_time dari event terbaru |

### metrics_window

Satu dokumen per window 1 menit (berdasarkan publish_time) dan tipe event. Index unik pada window_start dan event_type.

| Field | Keterangan |
|-------|------------|
| window_start, window_end | Batas window |
| event_type | Tipe event |
| event_count | Jumlah event |
| payment_total | Total payment_value |
| item_total | Total price |

### processed_events

Ledger idempotency untuk metrics_window.

| Field | Keterangan |
|-------|------------|
| _id | event_id |
| processed_at | Waktu dicatat, dasar TTL 30 hari |
| batch_id | Nomor micro-batch Spark |
