#!/usr/bin/env bash
# Catat kondisi pipeline tiap beberapa detik ke file CSV selama load test, hentikan dengan Ctrl+C
set -uo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-/tmp/loadtest.csv}"
INTERVAL="${2:-10}"

# Ambil jumlah pesan di satu queue RabbitMQ dari hasil list_queues
queue_messages() {
  echo "$1" | awk -v q="$2" '$1 == q {print $2}'
}

# Hitung dokumen ledger di MongoDB lewat estimasi cepat supaya tidak membebani database
ledger_count() {
  docker compose exec -T mongodb sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin --quiet --eval "db.getSiblingDB(process.env.MONGO_INITDB_DATABASE).processed_events.estimatedDocumentCount()"' 2>/dev/null | tail -n 1
}

# Alur utama: tulis header lalu ambil satu baris data tiap INTERVAL detik
main() {
  cd "${PROJECT_ROOT}"
  echo "epoch,queue_main,queue_dlq,landing_files,ledger" > "${OUT}"
  while true; do
    local queues main dlq files ledger
    queues="$(docker compose exec -T rabbitmq rabbitmqctl list_queues name messages 2>/dev/null)"
    main="$(queue_messages "${queues}" orders.events.queue)"
    dlq="$(queue_messages "${queues}" orders.events.dlq)"
    files="$(ls "${PROJECT_ROOT}/data/landing" 2>/dev/null | grep -c '^orders-')"
    ledger="$(ledger_count)"
    echo "$(date +%s),${main:-NA},${dlq:-NA},${files},${ledger:-NA}" | tee -a "${OUT}"
    sleep "${INTERVAL}"
  done
}

main
