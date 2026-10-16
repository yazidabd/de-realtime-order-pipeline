# Load Test

Dokumen ini mencatat uji beban pipeline, hasil ukur, dan batas yang ditemukan. Semua angka berasal dari run nyata di satu mesin lokal. Dokumen ini tidak mengklaim kapasitas produksi.

## Lingkungan

| Komponen | Konfigurasi |
|----------|-------------|
| Mesin | 16 CPU, 15 GB RAM, semua service di Docker pada satu mesin |
| RabbitMQ | Satu node, pesan persistent, publisher confirm per pesan |
| consumer | Satu proses, BATCH_SIZE 500, BATCH_FLUSH_SECONDS 5 |
| Spark | local[2], memori driver default 1 GB, trigger 10 detik, empat query |
| MongoDB | Single node replica set |
| S3 | Region ap-southeast-1, Parquet |
| Data | Olist penuh lewat MAX_ORDERS=0, producer mode fixed_rate dengan target 1000 per detik per producer |

## Alat ukur

| Alat | Fungsi |
|------|--------|
| infra/reset_state.sh | Mengosongkan queue, landing, checkpoint, MongoDB, dan prefix lake di S3 sebelum uji (butuh flag --yes) |
| infra/loadtest_sampler.sh | Mencatat queue, jumlah file landing, dan ukuran ledger tiap 10 detik ke CSV |
| infra/loadtest_report.sh | Ringkasan laju producer, statistik batch Spark per query, dan laju puncak dari Prometheus |
| infra/loadtest_drain.sh | Menunggu sampai keempat query Spark memproses seluruh baris target, lalu mencetak waktu selesai tiap query |
| infra/drill_snapshot.sh | Memeriksa konsistensi landing dan MongoDB |
| infra/check_lake.py | Menghitung event dan event_id unik di lake/events pada S3 |
| Listener progres Spark | Satu baris log per micro-batch per query: baris, durasi, laju |

## Hasil per tahap

Tahap A sampai E dijalankan berurutan pada kondisi bersih setelah reset. Tiap tahap memakai event baru yang belum pernah dikirim.

| Tahap | Beban | maxFilesPerTrigger |
|-------|-------|--------------------|
| A | Satu producer, 60000 event, Spark hidup | 20 |
| B | Tiga producer paralel, 3 kali 60000 event, Spark hidup | 20 |
| C | Spark dimatikan, tiga producer mengisi backlog 3 kali 40000 event, lalu Spark dinyalakan dan menguras | 20 |
| D | Sama seperti C | 60 |
| E | Empat producer paralel, 4 kali 20000 event, Spark hidup | 60 |

### Rekonsiliasi

| Tahap | Terkirim | Masuk landing | Masuk DLQ | Valid unik | consistent |
|-------|----------|---------------|-----------|------------|------------|
| A | 60000 | 59535 | 465 | 57435 | YES |
| B | 180000 | 178640 | 1360 | 172371 | YES |
| C | 120000 | 119064 | 936 | 114991 | YES |
| D | 120000 | 119099 | 901 | 114992 | YES |
| E | 80000 | 79367 | 633 | 76552 | YES |
| Total | 560000 | 555705 | 4295 | 536341 | YES |

Terkirim sama dengan landing ditambah DLQ pada setiap tahap, jadi tidak ada pesan yang hilang. Ledger MongoDB, total metrics_window, dan event_id unik di lake/events semuanya 536341. Sisa 19364 baris landing adalah duplikat dan event yang ditolak validasi Spark, belum dirinci satu per satu.

### Ingress (producer dan broker)

| Tahap | Producer | Laju per producer | Total | Puncak broker menerima |
|-------|----------|-------------------|-------|------------------------|
| B | 3 | 95 per detik | sekitar 285 | 290 per detik |
| C | 3 | 96 per detik | sekitar 288 | 298 per detik |
| D | 3 | 92 per detik | sekitar 276 | 285 per detik |
| E | 4 | 88 per detik | sekitar 352 | 364 per detik |

Laju total naik dari 3 ke 4 producer tetapi tidak linear, jadi broker belum jenuh pada uji ini, namun ada pembagian sumber daya bersama. Penyebab pastinya tidak diisolasi dan batas atas broker belum ditemukan. Consumer menulis ke landing dengan laju yang sama dengan broker menerima (367 per detik pada puncak), dan queue utama tidak pernah melebihi 500 pesan.

### Spark per query

Rata-rata durasi satu micro-batch dalam detik.

| Tahap | Rata-rata baris lake_events | lake_events | lake_rejected | mongo_serving | mongo_metrics |
|-------|-----------------------------|-------------|---------------|---------------|---------------|
| A | 1567 | 10.6 | 10.0 | 3.1 | 5.0 |
| B | 3134 | 11.3 | 11.1 | 4.7 | 5.5 |
| C | 10000 | 15.0 | 13.3 | 6.9 | 9.5 |
| D | 29775 | 16.1 | 13.3 | 12.5 | 15.5 |
| E | 4177 | 12.8 | 10.8 | 4.6 | 6.1 |

Kapasitas diukur dari pengurasan backlog, saat batch sudah penuh. Satuan baris per detik sibuk.

| Query | Tahap C (batch sekitar 10 ribu baris) | Tahap D (batch sekitar 30 ribu baris) |
|-------|---------------------------------------|----------------------------------------|
| lake_events | 668 | 1845 |
| lake_rejected | 751 | 2246 |
| mongo_serving | 1432 | 2373 |
| mongo_metrics | 1044 | 1923 |

Query paling lambat (lake_events) menentukan kapasitas: sekitar 650 sampai 700 event per detik pada 20 file per batch, dan sekitar 1800 pada 60 file per batch. Tahap D menguras backlog sekitar 119 ribu baris dalam 57 sampai 71 detik sejak Spark dinyalakan, termasuk waktu start Spark. Default maxFilesPerTrigger dinaikkan menjadi 60.

Biaya sink S3 hampir seluruhnya biaya tetap per batch. lake_rejected butuh 13.3 detik pada batch 10 ribu dan 30 ribu baris, dan menulis hanya puluhan baris per batch saja sudah menghabiskan sekitar 10 detik. Menaikkan ukuran batch hampir tanpa biaya tambahan. Persen waktu sibuk bukan tanda jenuh: micro-batch berjalan beruntun dan ukuran batch menyesuaikan laju masuk. Pada tahap E, lake_events sibuk sekitar 96 persen padahal kapasitasnya sekitar 1800 per detik.

Setelah producer berhenti pada tahap E, keempat query selesai 5 sampai 18 detik kemudian.

### Latency

Diukur dari publish_time sampai file landing ditulis consumer, dari histogram Prometheus untuk jendela tahap B dan C.

| Beban | p50 | p95 | p99 |
|-------|-----|-----|-----|
| Ringan (sekitar 50 per detik, dari Grafana) | sekitar 2.5 detik | sekitar 4.8 detik | tidak dicatat |
| Tinggi (sekitar 290 per detik) | 0.89 detik | 1.89 detik | 1.98 detik |

Pada beban ringan latency didominasi BATCH_FLUSH_SECONDS=5. Pada beban tinggi batch 500 event penuh dalam sekitar 1.7 detik, jadi latency turun. Resolusi bucket histogram 1 detik di rentang ini, jadi klaim yang aman adalah 99 persen di bawah 2 detik.

Latency ujung ke ujung (sampai muncul di MongoDB dan S3) tidak diukur langsung. Estimasinya 25 sampai 30 detik pada beban ini, dari tambahan waktu tunggu trigger Spark dan durasi batch 10 sampai 15 detik.

### Layer silver pada data besar

Compact pada tahap A (57435 baris bronze, tanpa duplikat) butuh 170 detik untuk menulis silver. Pada data 10 kali lebih besar dari uji awal (5752 baris, 84 detik), waktu tulis hanya sekitar 2 kali lebih lama, sejalan dengan biaya per file di S3.

### Ukuran file di S3

lake/events setelah tahap E berisi 2676 objek dengan total 88.446.804 byte, rata-rata sekitar 33 KB per objek. Ini masalah file kecil. Layer silver menulis satu file per partisi, jadi analisis lewat silver tidak terdampak, tetapi bronze tetap terfragmentasi.

## Prediksi yang meleset

Tiap tahap diawali prediksi tertulis. Yang salah dicatat karena memperbaiki cara membaca sistem.

| Prediksi | Hasil |
|----------|-------|
| Producer 1 sampai 3 ribu event per detik | 88 sampai 96 per detik per producer, penyebab batas tidak diisolasi |
| Bottleneck ada di sink MongoDB | Salah, MongoDB paling santai. Yang paling mahal adalah dua sink S3 |
| Transaksi MongoDB mendekati batas 60 detik | Durasi terpanjang mongo_metrics 22.8 detik |
| Batch 30 ribu baris butuh 26 detik untuk lake_events | 16.1 detik, biaya tetap lebih dominan dari model |
| Persen sibuk tinggi berarti jenuh | Salah, lihat bagian Spark per query |

## Keterbatasan

| Batas | Keterangan |
|-------|------------|
| Satu mesin, Spark local[2] | Angka tidak bisa diekstrapolasi ke cluster |
| Batas atas broker belum ditemukan | Puncak yang teramati 364 per detik tanpa tanda jenuh |
| Producer memakai confirm per pesan | Confirm asinkron atau per batch kemungkinan menaikkan laju, belum dicoba |
| Kapasitas Spark diukur dari pengurasan backlog | Belum diuji sebagai laju masuk berkelanjutan di atas 364 per detik |
| Latency ujung ke ujung berupa estimasi | Yang diukur hanya sampai landing zone |
| Memori driver 1 GB | Batch 30 ribu baris aman (tanpa restart), batas di atasnya belum dicari |
| Silver hanya diukur dua kali pada ukuran berbeda | Waktu tulis pada 345 ribu baris atau lebih belum diukur |
| File kecil di bronze | Rata-rata 33 KB per objek |

## Cara mengulang

Reset menghapus semua data pipeline, termasuk isi prefix lake di S3. Jalankan hanya pada lingkungan uji.

```bash
bash infra/reset_state.sh --yes
bash infra/loadtest_sampler.sh /tmp/loadtest.csv 10 < /dev/null > /dev/null 2>&1 &
BEFORE_LINES=$(cat data/landing/orders-*.json | wc -l)
START_ISO=$(date -u +%Y-%m-%dT%H:%M:%SZ)
MAX_ORDERS=0 REPLAY_MODE=fixed_rate EVENTS_PER_SECOND=1000 python producer/order_producer.py --skip-events N --max-events 20000
AFTER_LINES=$(cat data/landing/orders-*.json | wc -l)
bash infra/loadtest_drain.sh "$START_ISO" $((AFTER_LINES - BEFORE_LINES)) 1200
bash infra/drill_snapshot.sh --wait 600
bash infra/loadtest_report.sh "$START_ISO" /tmp/loadtest.csv "/tmp/producer_*.log"
```

Untuk beberapa producer paralel, jalankan beberapa perintah producer dengan --skip-events yang tidak tumpang tindih dan arahkan log masing-masing ke /tmp/producer_N.log. Untuk uji kapasitas Spark, hentikan Spark dulu, isi backlog, lalu nyalakan lagi.
