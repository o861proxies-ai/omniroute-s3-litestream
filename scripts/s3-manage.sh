#!/bin/sh
##############################################################
# scripts/s3-manage.sh
# Quản lý S3 backup cho Litestream / Supabase Storage
#
# Không cần cài aws CLI — chạy qua Docker tự động.
# Yêu cầu: Docker Desktop đang chạy.
#
# Usage:
#   ./scripts/s3-manage.sh info       — Xem thông tin cấu hình
#   ./scripts/s3-manage.sh check      — Kiểm tra kết nối S3
#   ./scripts/s3-manage.sh snapshots  — Liệt kê snapshots
#   ./scripts/s3-manage.sh clear      — Xóa toàn bộ backup S3 (có xác nhận)
#   ./scripts/s3-manage.sh clear --force
##############################################################

set -e

# ─── Load .env ───────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../.env"

if [ -f "${ENV_FILE}" ]; then
  set -a
  # shellcheck disable=SC1090
  . "${ENV_FILE}"
  set +a
  echo "✅ Loaded .env"
else
  echo "⚠️  Không tìm thấy .env tại ${ENV_FILE}"
fi

# ─── Validate Required Vars ──────────────────────────────────
check_required_vars() {
  MISSING=""
  for var in LITESTREAM_BUCKET LITESTREAM_ACCESS_KEY_ID LITESTREAM_SECRET_ACCESS_KEY SUPABASE_PROJECT_REF; do
    eval val="\$$var"
    if [ -z "$val" ]; then MISSING="${MISSING} ${var}"; fi
  done
  if [ -n "${MISSING}" ]; then
    echo "❌ Thiếu biến môi trường:${MISSING}"
    exit 1
  fi
}

# ─── Check Docker available ──────────────────────────────────
check_docker() {
  if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker không chạy hoặc chưa cài. Vui lòng mở Docker Desktop."
    exit 1
  fi
}

# ─── AWS CLI via Docker ───────────────────────────────────────
# Dùng image amazon/aws-cli — pull tự động lần đầu (~50MB)
aws_cli() {
  docker run --rm \
    -e AWS_ACCESS_KEY_ID="${LITESTREAM_ACCESS_KEY_ID}" \
    -e AWS_SECRET_ACCESS_KEY="${LITESTREAM_SECRET_ACCESS_KEY}" \
    -e AWS_DEFAULT_REGION="ap-southeast-1" \
    amazon/aws-cli \
    --endpoint-url "https://${SUPABASE_PROJECT_REF}.supabase.co/storage/v1/s3" \
    "$@"
}

# ─── Commands ────────────────────────────────────────────────

cmd_info() {
  echo ""
  echo "══════════════════════════════════════════"
  echo "  📋 Cấu Hình Hiện Tại"
  echo "══════════════════════════════════════════"
  echo "  Bucket        : ${LITESTREAM_BUCKET}"
  echo "  Supabase Ref  : ${SUPABASE_PROJECT_REF}"
  echo "  S3 Endpoint   : https://${SUPABASE_PROJECT_REF}.supabase.co/storage/v1/s3"
  echo "  S3 Path       : s3://${LITESTREAM_BUCKET}/storage"
  echo "  Access Key    : ${LITESTREAM_ACCESS_KEY_ID:0:8}... (truncated)"
  echo "  Secret Key    : ******* (hidden)"
  echo ""
  echo "  Local DB path : ./data/storage.sqlite"

  DB_CHECK="${SCRIPT_DIR}/../data/storage.sqlite"
  if [ -f "${DB_CHECK}" ]; then
    echo "  Local DB      : tồn tai"
    SIZE=$(wc -c < "${DB_CHECK}" 2>/dev/null || echo "?")
    echo "  Local DB size : ${SIZE} bytes"
  else
    echo "  Local DB      : (không tồn tại)"
  fi

  echo "══════════════════════════════════════════"
  echo ""
  echo "  Docker        : $(docker --version 2>/dev/null || echo 'không tìm thấy')"
  echo ""
}

cmd_check() {
  check_required_vars
  check_docker

  echo ""
  echo "Kiem tra ket noi S3 (via Docker aws CLI)..."
  echo "(Lan dau se pull image amazon/aws-cli ~50MB)"
  echo ""

  if aws_cli s3 ls "s3://${LITESTREAM_BUCKET}/" > /dev/null 2>&1; then
    echo "Ket noi S3 thanh cong!"
  else
    echo "Ket noi that bai. Chi tiet loi:"
    aws_cli s3 ls "s3://${LITESTREAM_BUCKET}/" || true
    echo ""
    echo "   Kiem tra lai:"
    echo "   1. SUPABASE_PROJECT_REF dung chua?"
    echo "   2. LITESTREAM_ACCESS_KEY_ID / SECRET dung chua?"
    echo "   3. Bucket '${LITESTREAM_BUCKET}' da tao trong Supabase Storage chua?"
    exit 1
  fi

  echo ""
  echo "Noi dung bucket root:"
  aws_cli s3 ls "s3://${LITESTREAM_BUCKET}/" || echo "   (trong)"

  echo ""
  echo "Thu muc storage/ (Litestream backups):"
  aws_cli s3 ls "s3://${LITESTREAM_BUCKET}/storage/" --recursive 2>/dev/null \
    | head -20 \
    || echo "   (chua co backup nao - chay docker compose up de bat dau replicate)"
  echo ""
}

cmd_snapshots() {
  check_required_vars
  check_docker

  echo ""
  echo "Snapshots:"
  aws_cli s3 ls "s3://${LITESTREAM_BUCKET}/storage/snapshots/" --recursive 2>/dev/null \
    | sort -r \
    || echo "   (chua co snapshot nao)"

  echo ""
  echo "WAL Segments (10 gan nhat):"
  aws_cli s3 ls "s3://${LITESTREAM_BUCKET}/storage/wal/" --recursive 2>/dev/null \
    | sort -r | head -10 \
    || echo "   (chua co WAL nao)"
  echo ""
}

cmd_clear() {
  check_required_vars
  check_docker

  FORCE=0
  if [ "$1" = "--force" ]; then FORCE=1; fi

  echo ""
  echo "CANH BAO: Se XOA TOAN BO backup tren S3!"
  echo "Path: s3://${LITESTREAM_BUCKET}/storage"
  echo ""

  if [ ${FORCE} -eq 0 ]; then
    printf "Nhap 'DELETE' de xac nhan: "
    read -r CONFIRM
    if [ "${CONFIRM}" != "DELETE" ]; then
      echo "Da huy."
      exit 0
    fi
  fi

  echo ""
  echo "Dang xoa..."
  aws_cli s3 rm "s3://${LITESTREAM_BUCKET}/storage/" --recursive
  echo "Da xoa xong."
  echo ""
  echo "   Local DB (./data/storage.sqlite) van con nguyen."
  echo "   Litestream se bat dau replicate lai tu dau khi restart."
  echo ""
}

# ─── Router ──────────────────────────────────────────────────
CMD="${1:-help}"
case "${CMD}" in
  info)      cmd_info ;;
  check)     cmd_check ;;
  snapshots) cmd_snapshots ;;
  clear)     cmd_clear "$2" ;;
  help|*)
    echo ""
    echo "Usage: $0 <command>"
    echo ""
    echo "  info        Xem cau hinh + trang thai local DB"
    echo "  check       Test ket noi S3 + liet ke files"
    echo "  snapshots   Liet ke snapshots va WAL tren S3"
    echo "  clear       Xoa toan bo backup S3 (co xac nhan)"
    echo "  clear --force  Xoa khong hoi"
    echo ""
    echo "Yeu cau: Docker Desktop dang chay (aws CLI chay qua Docker)"
    echo ""
    ;;
esac