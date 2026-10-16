#!/usr/bin/env bash
# Tunggu sampai keempat query Spark selesai memproses TARGET baris, lalu cetak waktu selesai tiap query sejak START_ISO
set -uo pipefail

START_ISO="${1:?isi waktu mulai ISO sebagai argumen pertama}"
TARGET="${2:?isi jumlah baris target sebagai argumen kedua}"
MAX_WAIT="${3:-2400}"
QUERIES=(lake_events lake_rejected mongo_serving mongo_metrics)

# Hitung total baris input yang sudah diproses satu query sejak START_ISO
rows_done() {
  docker compose logs --since "${START_ISO}" spark 2>&1 | grep "progress query=$1 " | awk '{split($6,r,"="); s+=r[2]} END{print s+0}'
}

# Ambil timestamp log terakhir untuk satu query
last_time() {
  docker compose logs -t --since "${START_ISO}" spark 2>&1 | grep "progress query=$1 " | tail -n 1 | awk '{print $3}'
}

# Alur utama: ulangi pengecekan tiap 15 detik sampai semua query mencapai target atau waktu habis, lalu cetak ringkasan
main() {
  local waited=0
  local all_done=no
  local query
  while [ "${waited}" -lt "${MAX_WAIT}" ]; do
    all_done=yes
    for query in "${QUERIES[@]}"; do
      if [ "$(rows_done "${query}")" -lt "${TARGET}" ]; then
        all_done=no
      fi
    done
    if [ "${all_done}" = "yes" ]; then
      break
    fi
    sleep 15
    waited=$((waited + 15))
  done
  echo "semua_query_selesai=${all_done}"
  local start_epoch
  start_epoch="$(date -d "${START_ISO}" +%s)"
  for query in "${QUERIES[@]}"; do
    local stamp
    stamp="$(last_time "${query}")"
    if [ -n "${stamp}" ]; then
      echo "${query} baris=$(rows_done "${query}") selesai_detik=$(( $(date -d "${stamp}" +%s) - start_epoch ))"
    fi
  done
}

main
