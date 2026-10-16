#!/usr/bin/env bash
# Cetak ringkasan load test: laju producer, statistik batch Spark per query, ringkasan sampler, dan laju puncak dari Prometheus
set -uo pipefail

START_ISO="${1:?isi waktu mulai ISO sebagai argumen pertama}"
SAMPLER_CSV="${2:-/tmp/loadtest.csv}"
PRODUCER_LOGS="${3:-/tmp/producer_B_*.log}"

# Hitung laju tiap producer dari log: event terkirim dibagi selisih waktu mulai dan selesai
producer_rates() {
  for file in ${PRODUCER_LOGS}; do
    awk '
      /mulai replay/ { split($2, a, /[:,]/); t0 = a[1]*3600 + a[2]*60 + a[3] + a[4]/1000 }
      /selesai, total terkirim/ { split($2, a, /[:,]/); t1 = a[1]*3600 + a[2]*60 + a[3] + a[4]/1000; n = $7 }
      END { if (t1 > t0) printf "%s: %d event dalam %.0f detik = %.0f per detik\n", FILENAME, n, t1 - t0, n / (t1 - t0); else print FILENAME ": belum selesai" }
    ' "${file}"
  done
}

# Ringkas progres batch Spark per query sejak waktu mulai
spark_summary() {
  docker compose logs --since "${START_ISO}" spark 2>&1 | grep "progress query=" | awk '{
    split($4,q,"="); split($6,r,"="); split($8,t,"=");
    n[q[2]]++; rows[q[2]]+=r[2]; ms[q[2]]+=t[2]; if (t[2]+0>mx[q[2]]+0) mx[q[2]]=t[2]
  } END { for (k in n) printf "%s batches=%d avg_rows=%.0f avg_trigger_ms=%.0f max_trigger_ms=%d rows_per_busy_sec=%.0f\n", k, n[k], rows[k]/n[k], ms[k]/n[k], mx[k], rows[k]/(ms[k]/1000) }'
}

# Ringkas file CSV sampler: queue utama tertinggi dan baris terakhir
sampler_summary() {
  awk -F, 'NR>1 && $2 ~ /^[0-9]+$/ && $2+0>m {m=$2+0} END{print "queue_main max=" m+0}' "${SAMPLER_CSV}"
  echo "baris data sampler: $(( $(wc -l < "${SAMPLER_CSV}") - 1 ))"
  tail -n 2 "${SAMPLER_CSV}"
}

# Ambil laju puncak per detik selama 3 jam terakhir dari Prometheus
prom_peak() {
  curl -s localhost:9090/api/v1/query --data-urlencode "query=max_over_time(sum(rate($2[1m]))[3h:15s])" | python3 -c "
import sys, json
result = json.load(sys.stdin)['data']['result']
print('$1:', round(float(result[0]['value'][1])) if result else 'kosong', 'per detik')"
}

echo "--- laju producer ---"
producer_rates
echo "--- batch Spark ---"
spark_summary
echo "--- sampler ---"
sampler_summary
echo "--- laju puncak (Prometheus, 3 jam) ---"
prom_peak "diterima broker" rabbitmq_global_messages_received_total
prom_peak "ditulis consumer ke landing" consumer_events_consumed_total
