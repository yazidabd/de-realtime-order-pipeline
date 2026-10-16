# Failure Drills

Dokumen ini mencatat uji kegagalan pada pipeline: apa yang dijalankan, apa yang terbukti, apa yang ditemukan, dan apa yang belum diuji. Semua angka berasal dari run nyata di mesin lokal.

## Metode

Alat ukur ada di infra/drill_check.py dan infra/drill_snapshot.sh. drill_check membaca seluruh file landing, menghitung sendiri berapa event_id valid unik menurut aturan validasi yang sama dengan Spark, lalu membandingkannya dengan MongoDB.

| Indikator | Arti |
|-----------|------|
| expected_valid_ids | Jumlah event_id valid unik di landing zone |
| mongo_ledger | Jumlah event_id di ledger processed_events |
| mongo_metrics_total | Total event_count di metrics_window |
| consistent=YES | mongo_ledger sama dengan expected_valid_ids, dan mongo_metrics_total sama dengan mongo_ledger |

Tiap drill memakai pola yang sama: snapshot sebelum, gangguan dijalankan sambil mengirim event baru, tunggu sistem tenang, snapshot sesudah, lalu cocokkan selisihnya. Dua persamaan harus berlaku.

| Persamaan | Arti |
|-----------|------|
| Selisih landing_lines ditambah selisih queue_dlq sama dengan jumlah event yang dikirim | Tidak ada pesan yang hilang di jalur RabbitMQ ke landing |
| Selisih expected_valid_ids sama dengan selisih mongo_ledger dan selisih mongo_metrics_total | Setiap event valid tercatat tepat satu kali di MongoDB |

Event yang dipakai adalah event baru (id belum pernah masuk), dipilih lewat opsi producer --skip-events dan --orders-offset. Nilai waited_seconds di snapshot adalah waktu sampai antrean kosong dan data konsisten, termasuk waktu start container. Angka itu bukan SLA.

## Ringkasan hasil

| No | Gangguan | Beban | Hasil |
|----|----------|-------|-------|
| 1 | Consumer dimatikan paksa (SIGKILL) saat replay | 1500 event, 50 per detik | 1489 landing ditambah 11 DLQ sama dengan 1500, consistent=YES, waited 30 detik |
| 2 | Spark dimatikan paksa saat replay | 1500 event | 1491 landing ditambah 9 DLQ sama dengan 1500, id valid +1452 sama di ledger dan metrics, consistent=YES, waited 30 detik |
| 3 | RabbitMQ di-restart saat 1500 pesan menunggu di queue (consumer berhenti) | 1500 event | 1490 landing ditambah 10 DLQ sama dengan 1500, id valid +1439 sama di ledger dan metrics, consistent=YES, waited 20 detik |
| 4 | Spark dimatikan paksa tepat di tengah batch (mongo_metrics offsets 38, commits 37) | 2300 event, 100 per detik | 2276 landing ditambah 24 DLQ sama dengan 2300, id valid +2212 sama di ledger dan metrics, consistent=YES, waited 20 detik |
| 5 | MongoDB dimatikan 40 detik saat replay | 1500 event | Spark crash dan direstart Docker, 1494 landing ditambah 6 DLQ sama dengan 1500, id valid +1448 sama di ledger dan metrics, consistent=YES, waited 10 detik |
| 6 | Spark dimatikan tepat setelah transaksi ledger commit dan sebelum checkpoint (fault injection) | 1500 event | Batch yang sama diulang dan semua eventnya dilewati ledger, 1492 landing ditambah 8 DLQ sama dengan 1500, id valid +1443 sama di ledger dan metrics, consistent=YES, waited 20 detik |
| 7 | Pengiriman ulang penuh 1500 event yang sama dengan drill 6 | 1500 event lama | Ledger dan metrics tidak berubah (15173), landing +1492 dan DLQ +8, consistent=YES |
| 8 | Producer saat broker mati dari awal | 5 event | Lima percobaan dengan jeda 2, 4, 6, 8 detik, exit code 1, tanpa traceback |
| 9 | Producer saat broker mati di tengah replay | 400 event, 20 per detik | Melaporkan 110 event terkirim, queue berisi 110 setelah broker pulih, exit code 1 dengan petunjuk --skip-events 110 |
| 10 | Consumer saat broker mati lalu hidup | Tanpa beban | RestartCount 0, warning tiap 5 detik, tersambung kembali sekitar 5 detik setelah broker hidup |

## Bukti drill 6

Drill 6 membuktikan kasus yang paling sulit: transaksi MongoDB sudah commit, tetapi checkpoint Spark belum tercatat. Hook di spark/sinks.py mematikan proses satu kali tepat di titik itu, aktif hanya bila FAULT_CRASH_AFTER_COMMIT=1 dan hanya sekali berkat file penanda di data/checkpoints.

| Log Spark | Arti |
|-----------|------|
| metrics batch 44: 244 event, 244 baru, 0 dilewati ledger | Batch 44 diproses dan transaksi commit |
| fault injection: crash setelah commit batch 44 | Proses dimatikan sebelum checkpoint |
| metrics batch 44: 244 event, 0 baru, 244 dilewati ledger | Batch yang sama diulang setelah restart, ledger menolak menghitung dua kali |
| metrics batch 45: 1199 event, 1199 baru, 0 dilewati ledger | Pemrosesan lanjut normal |

## Data lake setelah semua drill

Job compact dijalankan setelah drill 6.

| Ukuran | Nilai |
|--------|-------|
| bronze rows | 18051 |
| bronze distinct event_id | 15173, sama dengan expected_valid_ids |
| duplikat dibuang | 2878, sama dengan sebelum drill 5 dan 6 |
| silver rows | 15173, verifikasi OK |

Crash di tengah batch, crash setelah commit, dan pengiriman ulang penuh tidak menambah baris duplikat baru ke bronze. Duplikat 2878 itu berasal dari replay lama yang melewati watermark dedup.

## Temuan dan perbaikan

| No | Temuan | Perbaikan |
|----|--------|-----------|
| 1 | Consumer crash saat broker mati karena error DNS (gaierror) tidak tertangkap loop reconnect, RestartCount 2 dan pulih sekitar 35 detik | Loop menangkap AMQPError dan OSError, RestartCount 0 dan pulih sekitar 5 detik |
| 2 | pip install dijalankan tiap container consumer start | Dependency di-bake lewat consumer/Dockerfile |
| 3 | Shutdown consumer 10.4 detik karena SIGTERM tidak sampai ke Python (sh sebagai PID 1) | Command bentuk list sehingga Python menjadi PID 1, shutdown 0.4 detik |
| 4 | Producer mati dengan traceback mentah saat broker mati | Retry dengan jeda bertambah, pesan error jelas, petunjuk --skip-events untuk melanjutkan |
| 5 | User aplikasi RabbitMQ tidak pernah dibuat karena load_definitions mengabaikan RABBITMQ_DEFAULT_USER | Script infra/setup_rabbitmq_user.sh yang idempotent |
| 6 | metrics_window memakai $inc dan bisa menghitung dua kali saat retry atau replay | Ledger processed_events, insert ledger dan $inc dalam satu transaksi MongoDB |
| 7 | Dedup streaming dibatasi watermark 30 menit sehingga bronze bisa berisi duplikat | Job compact membentuk layer silver dengan dedup penuh |

## Yang belum terbukti atau belum diuji

| Batas | Keterangan |
|-------|------------|
| Outage MongoDB panjang | Hanya diuji 40 detik. Spark tidak menunggu MongoDB di dalam proses, dia gagal cepat lalu Docker yang memulihkan. Outage lebih panjang berarti crash berulang tiap sekitar 45 detik sampai MongoDB kembali. |
| Pengiriman ulang pesan oleh RabbitMQ | Pada drill 1 total tepat 1500 tanpa kelebihan, jadi pesan unacked yang dikirim ulang tidak teramati. Yang terbukti hanya tidak ada pesan hilang. |
| Drill 4 dan batch ganda | Log tidak memperlihatkan nomor batch yang muncul dua kali, jadi crash jatuh sebelum log batch tercetak. Kasus commit sebelum checkpoint dibuktikan terpisah lewat drill 6. |
| Drill 3 dan jumlah queue setelah restart | Baris hitungan queue sesudah restart tidak tercetak. Buktinya tidak langsung: 1500 pesan ada sebelum restart, dan sesudahnya 1490 masuk landing ditambah 10 ke DLQ. Uji B producer menutup celah ini dengan hitungan 110 sama dengan 110. |
| File Parquet yatim di S3 | Belum diperiksa apakah kill saat penulisan Parquet meninggalkan file yang tidak tercatat di _spark_metadata |
| Kegagalan lain | Disk penuh, partisi jaringan, S3 tidak tersedia, dan failover MongoDB (single node) tidak diuji |
| Beban | Drill dijalankan pada 20 sampai 100 event per detik, belum pada beban penuh |

## Cara mengulang

Snapshot, gangguan, snapshot:

```bash
bash infra/drill_snapshot.sh --wait 300 | tee /tmp/before.txt
REPLAY_MODE=fixed_rate EVENTS_PER_SECOND=50 python producer/order_producer.py --orders-offset 2000 --skip-events N --max-events 1500 &
sleep 15
docker compose kill spark
sleep 15
docker compose up -d spark
wait
bash infra/drill_snapshot.sh --wait 300 | tee /tmp/after.txt
```

Ganti N dengan awal rentang event yang belum pernah dikirim. Gangguan lain mengikuti pola yang sama: ganti baris kill dan up dengan stop atau kill pada consumer, rabbitmq, atau mongodb.

Fault injection untuk drill 6:

```bash
sudo rm -f data/checkpoints/.fault_after_commit_done
FAULT_CRASH_AFTER_COMMIT=1 docker compose up -d --force-recreate spark
```

Setelah selesai, jalankan docker compose up -d --force-recreate spark tanpa variabel itu supaya hook kembali mati.
