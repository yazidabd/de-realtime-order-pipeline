#!/usr/bin/env bash
# Pastikan user aplikasi RabbitMQ ada dan sesuai .env, aman dijalankan berulang kali
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${PROJECT_ROOT}/.env"

# Baca satu variabel dari .env tanpa meng-export seluruh isi file
read_env() {
  grep -E "^$1=" "${ENV_FILE}" | head -n 1 | cut -d= -f2-
}

# Jalankan rabbitmqctl di dalam container rabbitmq
rmq() {
  docker compose exec -T rabbitmq rabbitmqctl "$@"
}

# Cek apakah user sudah terdaftar di broker
user_exists() {
  rmq list_users 2>/dev/null | awk '{print $1}' | grep -qxF "$1"
}

# Buat user kalau belum ada, kalau sudah ada samakan passwordnya dengan .env
ensure_user() {
  local user="$1"
  local password="$2"
  if user_exists "${user}"; then
    rmq change_password "${user}" "${password}" > /dev/null
    echo "user ${user} sudah ada, password disamakan dengan .env"
  else
    rmq add_user "${user}" "${password}" > /dev/null
    echo "user ${user} dibuat"
  fi
}

# Beri tag administrator dan permission penuh di vhost default
grant_access() {
  rmq set_user_tags "$1" administrator > /dev/null
  rmq set_permissions -p / "$1" ".*" ".*" ".*" > /dev/null
  echo "tag dan permission diset untuk $1"
}

# Alur utama: baca kredensial dari .env lalu pastikan user siap
main() {
  local user password
  user="$(read_env RABBITMQ_USER)"
  password="$(read_env RABBITMQ_PASSWORD)"
  if [ -z "${user}" ] || [ -z "${password}" ]; then
    echo "RABBITMQ_USER dan RABBITMQ_PASSWORD harus terisi di .env" >&2
    exit 1
  fi
  cd "${PROJECT_ROOT}"
  ensure_user "${user}" "${password}"
  grant_access "${user}"
  echo "selesai"
}

main "$@"
