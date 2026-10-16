#!/usr/bin/env bash
# Buat file .env dari .env.example dengan nilai bawaan dan password acak, lalu siapkan folder data
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXAMPLE="${PROJECT_ROOT}/.env.example"
TARGET="${1:-${PROJECT_ROOT}/.env}"
# Variabel yang sengaja dikosongkan karena hanya dipakai bila S3 diaktifkan
OPTIONAL_EMPTY="AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY S3_BUCKET_NAME"

# Isi satu variabel di file target dengan nilai apa adanya
set_var() {
  sed -i "s|^$1=.*|$1=$2|" "${TARGET}"
}

# Isi semua variabel dengan nilai bawaan dan buat secret acak
fill_defaults() {
  set_var OLIST_DATA_DIR data/raw
  set_var MAX_ORDERS 2000
  set_var FAKER_SEED 42
  set_var REPLAY_MODE time_compression
  set_var SPEED_FACTOR 3600
  set_var MAX_SLEEP_SECONDS 1
  set_var EVENTS_PER_SECOND 50
  set_var DUPLICATE_RATE 0.02
  set_var MISSING_FIELD_RATE 0.02
  set_var NEGATIVE_PRICE_RATE 0.01
  set_var STOCK_EVENTS_PER_ORDER 0.3
  set_var RABBITMQ_HOST 127.0.0.1
  set_var RABBITMQ_PORT 5672
  set_var RABBITMQ_MANAGEMENT_PORT 15672
  set_var RABBITMQ_USER de_app
  set_var RABBITMQ_PASSWORD "$(openssl rand -hex 16)"
  set_var RABBITMQ_EXCHANGE orders.events
  set_var RABBITMQ_QUEUE orders.events.queue
  set_var RABBITMQ_DLQ orders.events.dlq
  set_var LANDING_DIR data/landing
  set_var BATCH_SIZE 500
  set_var BATCH_FLUSH_SECONDS 5
  set_var CONSUMER_METRICS_PORT 8000
  set_var CHECKPOINT_DIR data/checkpoints
  set_var TRIGGER_INTERVAL_SECONDS 10
  set_var MAX_FILES_PER_TRIGGER 60
  set_var WINDOW_DURATION '"1 minute"'
  set_var WATERMARK_DELAY '"30 minutes"'
  set_var MONGO_HOST localhost
  set_var MONGO_PORT 27017
  set_var MONGO_USER mongo_admin
  set_var MONGO_PASSWORD "$(openssl rand -hex 16)"
  set_var MONGO_DB orders_pipeline
  set_var MONGO_REPLICA_KEY "$(openssl rand -hex 32)"
  set_var AWS_REGION ap-southeast-1
  set_var S3_PREFIX lake
  set_var GRAFANA_ADMIN_USER admin
  set_var GRAFANA_ADMIN_PASSWORD "$(openssl rand -hex 16)"
}

# Cetak variabel yang masih kosong dan tidak termasuk daftar boleh kosong, kembalikan 1 bila ada
check_empty() {
  local missing=0
  local key value
  while IFS='=' read -r key value; do
    case "${key}" in
      ''|'#'*) continue ;;
    esac
    if [ -z "${value}" ] && [[ " ${OPTIONAL_EMPTY} " != *" ${key} "* ]]; then
      echo "variabel kosong: ${key}" >&2
      missing=1
    fi
  done < "${TARGET}"
  return "${missing}"
}

# Alur utama: tolak menimpa file yang sudah ada kecuali --force, lalu buat file dan folder data
main() {
  if [ ! -f "${EXAMPLE}" ]; then
    echo ".env.example tidak ditemukan di ${EXAMPLE}" >&2
    exit 1
  fi
  if [ -e "${TARGET}" ] && [ "${2:-}" != "--force" ]; then
    echo "${TARGET} sudah ada dan tidak ditimpa (tambahkan --force sebagai argumen kedua untuk menimpa)" >&2
    exit 1
  fi
  cp "${EXAMPLE}" "${TARGET}"
  chmod 600 "${TARGET}"
  fill_defaults
  mkdir -p "${PROJECT_ROOT}/data/raw" "${PROJECT_ROOT}/data/landing" "${PROJECT_ROOT}/data/checkpoints"
  local status=0
  check_empty || status=1
  echo "file env dibuat di ${TARGET}, folder data siap"
  exit "${status}"
}

main "$@"
