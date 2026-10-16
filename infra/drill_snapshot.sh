#!/usr/bin/env bash
# Cetak snapshot kondisi pipeline untuk failure drill, opsional menunggu sampai kondisi stabil
set -uo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WAIT_SECONDS=0
POLL_SECONDS=10

# Hitung pesan (ready dan unacked) di satu queue RabbitMQ, unknown kalau broker tidak menjawab
queue_count() {
  local count
  count="$(docker compose exec -T rabbitmq rabbitmqctl list_queues name messages 2>/dev/null | awk -v q="$1" '$1 == q {print $2}')"
  echo "${count:-unknown}"
}

# Jalankan drill_check.py di dalam container spark
run_check() {
  docker compose exec -T spark sh -c 'export PYTHONPATH="/opt/spark/python:$(ls /opt/spark/python/lib/py4j-*-src.zip):/app"; python3 -' < "${PROJECT_ROOT}/infra/drill_check.py"
}

# Kumpulkan satu snapshot lengkap: dua queue RabbitMQ plus hasil drill_check
take_snapshot() {
  echo "queue_main=$(queue_count orders.events.queue)"
  echo "queue_dlq=$(queue_count orders.events.dlq)"
  run_check || echo "consistent=NO"
}

# Alur utama: ambil snapshot, kalau --wait diberikan ulangi sampai queue kosong dan data konsisten atau waktu habis
main() {
  if [ "${1:-}" = "--wait" ]; then
    WAIT_SECONDS="${2:-300}"
  fi
  cd "${PROJECT_ROOT}"
  local output waited=0
  output="$(take_snapshot 2>&1)"
  while [ "${WAIT_SECONDS}" -gt 0 ] && [ "${waited}" -lt "${WAIT_SECONDS}" ]; do
    if echo "${output}" | grep -q "^consistent=YES$" && echo "${output}" | grep -q "^queue_main=0$"; then
      break
    fi
    sleep "${POLL_SECONDS}"
    waited=$((waited + POLL_SECONDS))
    output="$(take_snapshot 2>&1)"
  done
  echo "${output}"
  echo "waited_seconds=${waited}"
}

main "$@"
