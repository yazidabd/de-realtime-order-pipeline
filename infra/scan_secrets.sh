#!/usr/bin/env bash
# Pastikan tidak ada nilai rahasia dari .env dan tidak ada pola kunci di file yang akan masuk ke git
set -uo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"
ENV_FILE=".env"
FOUND=0

# Daftar file yang akan masuk ke git: sudah dilacak, atau belum dilacak tetapi tidak diabaikan
candidate_files() {
  git ls-files --cached --others --exclude-standard -z
}

# Cek tiap nilai rahasia di .env tidak muncul di file kandidat, hanya nama variabel dan nama file yang dicetak
scan_env_values() {
  local line key value hits
  if [ ! -f "${ENV_FILE}" ]; then
    echo "peringatan: .env tidak ada, pemeriksaan nilai rahasia dilewati"
    return
  fi
  while IFS= read -r line; do
    case "${line}" in
      ''|'#'*) continue ;;
    esac
    key="${line%%=*}"
    value="${line#*=}"
    case "${key}" in
      *PASSWORD*|*SECRET*|*KEY*|*TOKEN*) ;;
      *) continue ;;
    esac
    if [ "${#value}" -lt 6 ]; then
      continue
    fi
    hits="$(candidate_files | xargs -0 grep -lIF -- "${value}" 2>/dev/null || true)"
    if [ -n "${hits}" ]; then
      echo "BOCOR: nilai ${key} ditemukan di:"
      echo "${hits}" | sed 's/^/  /'
      FOUND=1
    fi
  done < "${ENV_FILE}"
}

# Cari pola kunci akses AWS dan kunci privat di file kandidat
scan_patterns() {
  local hits
  hits="$(candidate_files | xargs -0 grep -InE 'AKIA[0-9A-Z]{16}|BEGIN (RSA|OPENSSH|EC) PRIVATE KEY' 2>/dev/null || true)"
  if [ -n "${hits}" ]; then
    echo "BOCOR: pola kunci ditemukan:"
    echo "${hits}" | sed 's/^/  /'
    FOUND=1
  fi
}

# Alur utama: jalankan kedua pemeriksaan dan laporkan hasilnya
main() {
  local total
  total="$(candidate_files | tr -cd '\0' | wc -c)"
  scan_env_values
  scan_patterns
  if [ "${FOUND}" -eq 0 ]; then
    echo "bersih: ${total} file diperiksa, tidak ada nilai rahasia atau pola kunci"
  else
    echo "GAGAL: perbaiki temuan di atas sebelum commit" >&2
    exit 1
  fi
}

main
