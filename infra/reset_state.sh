#!/usr/bin/env bash
# Reset pipeline ke kondisi bersih untuk load test: kosongkan queue, landing, checkpoint, MongoDB, dan prefix lake di S3
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${PROJECT_ROOT}/.env"
AWS_PROFILE_NAME="${AWS_PROFILE_NAME:-de-dev}"

# Baca satu variabel dari .env tanpa meng-export seluruh isi file
read_env() {
  grep -E "^$1=" "${ENV_FILE}" | head -n 1 | cut -d= -f2-
}

# Jalankan mongosh di dalam container mongodb dengan kredensial dari environment container
mongo_eval() {
  docker compose exec -T mongodb sh -c "mongosh -u \"\$MONGO_INITDB_ROOT_USERNAME\" -p \"\$MONGO_INITDB_ROOT_PASSWORD\" --authenticationDatabase admin --quiet $1"
}

# Tampilkan apa yang akan dihapus, lalu berhenti kecuali flag --yes diberikan
confirm_or_exit() {
  local bucket="$1"
  local prefix="$2"
  echo "reset ini akan MENGHAPUS:"
  echo "  1. semua pesan di queue orders.events.queue dan orders.events.dlq"
  echo "  2. isi data/landing dan data/checkpoints"
  echo "  3. semua data MongoDB (collection dibuat ulang kosong beserta index)"
  if [ -n "${bucket}" ]; then
    echo "  4. semua objek di s3://${bucket}/${prefix}/"
  fi
  if [ "${3:-}" != "--yes" ]; then
    echo "jalankan ulang dengan --yes untuk melanjutkan"
    exit 1
  fi
}

# Hentikan consumer dan Spark supaya tidak menulis saat data dihapus
stop_services() {
  docker compose stop consumer spark
}

# Kosongkan dua queue RabbitMQ
purge_queues() {
  docker compose exec -T rabbitmq rabbitmqctl purge_queue orders.events.queue > /dev/null
  docker compose exec -T rabbitmq rabbitmqctl purge_queue orders.events.dlq > /dev/null
  echo "queue dikosongkan"
}

# Hapus isi landing dan checkpoint (file dimiliki root dari container, jadi butuh sudo)
clear_local_files() {
  sudo find "${PROJECT_ROOT}/data/landing" -mindepth 1 -delete
  sudo find "${PROJECT_ROOT}/data/checkpoints" -mindepth 1 -delete
  echo "landing dan checkpoint dikosongkan"
}

# Hapus database MongoDB lalu buat ulang collection dan index lewat init_indexes.js
reset_mongo() {
  mongo_eval '--eval "db.getSiblingDB(process.env.MONGO_INITDB_DATABASE).dropDatabase()"' > /dev/null
  mongo_eval '--file /docker-entrypoint-initdb.d/init_indexes.js'
}

# Hapus semua objek di bawah prefix lake pada bucket S3
clear_s3() {
  local bucket="$1"
  local prefix="$2"
  if [ -z "${bucket}" ]; then
    return
  fi
  aws --profile "${AWS_PROFILE_NAME}" s3 rm "s3://${bucket}/${prefix}/" --recursive --only-show-errors
  echo "prefix s3://${bucket}/${prefix}/ dikosongkan"
}

# Nyalakan lagi consumer dan Spark dari kondisi bersih
start_services() {
  docker compose up -d consumer spark
}

# Alur utama: validasi pengaman, konfirmasi, lalu reset berurutan
main() {
  local bucket prefix
  bucket="$(read_env S3_BUCKET_NAME)"
  prefix="$(read_env S3_PREFIX)"
  prefix="${prefix:-lake}"
  prefix="${prefix#/}"
  prefix="${prefix%/}"
  if [ -z "${prefix}" ]; then
    echo "prefix S3 kosong, dibatalkan demi keamanan" >&2
    exit 1
  fi
  confirm_or_exit "${bucket}" "${prefix}" "${1:-}"
  stop_services
  purge_queues
  clear_local_files
  reset_mongo
  clear_s3 "${bucket}" "${prefix}"
  start_services
  echo "reset selesai"
}

main "$@"
