#!/bin/sh
set -e

DB_PATH="/app/data/storage.sqlite"
CONFIG_PATH="/etc/litestream.yml"

echo "[startup] =============================================="
echo "[startup] Litestream Startup Script"
echo "[startup] DB Path     : ${DB_PATH}"
echo "[startup] Bucket      : ${LITESTREAM_BUCKET}"
echo "[startup] Supabase Ref: ${SUPABASE_PROJECT_REF}"
echo "[startup] =============================================="

# Kiểm tra config file tồn tại
if [ ! -f "${CONFIG_PATH}" ]; then
  echo "[startup] ERROR: Config file not found at ${CONFIG_PATH}"
  exit 1
fi

# Tạo thư mục data nếu chưa có
mkdir -p "$(dirname "${DB_PATH}")"

if [ ! -f "${DB_PATH}" ]; then
  echo "[startup] No local DB found. Attempting restore from Supabase S3..."

  # ✅ PHẢI dùng -config flag để litestream đọc credentials từ litestream.yml
  # KHÔNG dùng URL trực tiếp vì sẽ thiếu: credentials, region, force-path-style, sign-payload
  litestream restore \
    -config "${CONFIG_PATH}" \
    -if-replica-exists \
    -o "${DB_PATH}" \
    "${DB_PATH}"

  EXIT_CODE=$?
  if [ ${EXIT_CODE} -eq 0 ]; then
    echo "[startup] ✅ Restore successful from S3."
  else
    echo "[startup] ⚠️  No remote backup found or restore failed (exit ${EXIT_CODE})."
    echo "[startup] Starting fresh with empty database."
  fi
else
  echo "[startup] ✅ Local DB already exists at ${DB_PATH}. Skipping restore."
  # In ra size để debug
  echo "[startup] DB Size: $(du -sh "${DB_PATH}" | cut -f1)"
fi

echo "[startup] Starting Litestream replication..."
exec litestream replicate -config "${CONFIG_PATH}"