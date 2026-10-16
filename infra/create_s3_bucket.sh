#!/usr/bin/env bash
# Buat bucket S3 data lake dengan pengaturan aman, aman dijalankan berulang kali
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${PROJECT_ROOT}/.env"
AWS_PROFILE_NAME="${AWS_PROFILE_NAME:-de-dev}"

# Baca satu variabel dari .env tanpa meng-export seluruh isi file
read_env() {
  grep -E "^$1=" "${ENV_FILE}" | head -n 1 | cut -d= -f2-
}

# Jalankan perintah aws dengan profile de-dev
aws_cli() {
  aws --profile "${AWS_PROFILE_NAME}" "$@"
}

# Buat bucket di region target kalau belum ada
create_bucket() {
  local bucket="$1"
  local region="$2"
  if aws_cli s3api head-bucket --bucket "${bucket}" 2>/dev/null; then
    echo "bucket ${bucket} sudah ada, lewati pembuatan"
    return
  fi
  aws_cli s3api create-bucket \
    --bucket "${bucket}" \
    --region "${region}" \
    --create-bucket-configuration "LocationConstraint=${region}" > /dev/null
  echo "bucket ${bucket} dibuat di ${region}"
}

# Blokir semua jalur akses publik ke bucket
block_public_access() {
  aws_cli s3api put-public-access-block \
    --bucket "$1" \
    --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
  echo "akses publik diblokir"
}

# Aktifkan enkripsi default SSE-S3 (AES256) untuk semua objek baru
enable_encryption() {
  aws_cli s3api put-bucket-encryption \
    --bucket "$1" \
    --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
  echo "enkripsi default AES256 aktif"
}

# Bersihkan multipart upload yang gagal setelah 7 hari supaya tidak menumpuk biaya
set_lifecycle() {
  aws_cli s3api put-bucket-lifecycle-configuration \
    --bucket "$1" \
    --lifecycle-configuration \
    '{"Rules":[{"ID":"abort-incomplete-multipart","Status":"Enabled","Filter":{"Prefix":""},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":7}}]}'
  echo "lifecycle multipart cleanup aktif"
}

# Alur utama: validasi env, lalu buat dan amankan bucket
main() {
  local bucket region
  bucket="$(read_env S3_BUCKET_NAME)"
  region="$(read_env AWS_REGION)"
  if [ -z "${bucket}" ] || [ -z "${region}" ]; then
    echo "S3_BUCKET_NAME dan AWS_REGION harus terisi di .env" >&2
    exit 1
  fi
  create_bucket "${bucket}" "${region}"
  block_public_access "${bucket}"
  enable_encryption "${bucket}"
  set_lifecycle "${bucket}"
  echo "selesai, bucket siap: s3://${bucket}"
}

main "$@"
