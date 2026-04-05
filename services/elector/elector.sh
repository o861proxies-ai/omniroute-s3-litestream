#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════════════
# services/elector/elector.sh
#
# Leader Election Daemon — Firebase RTDB Distributed Lock
#
# Giải quyết vấn đề multi-instance SQLite/Litestream:
#   • Chỉ 1 instance là LEADER tại 1 thời điểm
#   • LEADER: chạy litestream(replicate) + omniroute + cloudflared
#   • FOLLOWER: tất cả managed services bị dừng → Cloudflare tự skip
#   • Failover: lock TTL=30s, heartbeat=10s → chuyển leader < 60s
#
# Cơ chế atomic election: Firebase RTDB conditional PUT với If-Match ETag
# ══════════════════════════════════════════════════════════════════════
set -uo pipefail

# ──────────────────────────────────────────────────────────────────────
# 1. Config
# ──────────────────────────────────────────────────────────────────────
RTDB_URL="${RTDB_URL:?RTDB_URL is required}"

# Tách base URL và query params (auth token nếu có)
# Hỗ trợ cả 2 dạng:
#   1) https://xxx.firebasedatabase.app?auth=TOKEN
#   2) https://xxx.firebasedatabase.app/env-prod.json?auth=TOKEN
# Sau normalize:
#   - bỏ trailing slash
#   - bỏ ".json" nếu URL đầu vào đã có sẵn
RTDB_BASE="${RTDB_URL%%\?*}"
RTDB_QUERY="${RTDB_URL#*\?}"
[ "$RTDB_QUERY" = "$RTDB_URL" ] && RTDB_QUERY=""
RTDB_BASE="${RTDB_BASE%/}"
RTDB_BASE="${RTDB_BASE%.json}"

# Unique instance ID — tạo 1 lần per container lifecycle
_ID_FILE="/tmp/elector-instance-id"
if [ ! -f "$_ID_FILE" ]; then
  _raw_id="${INSTANCE_ID:-}"
  if [ -z "$_raw_id" ]; then
    # /proc/sys/kernel/random/uuid có trong Linux, Alpine, WSL2
    _raw_id=$(cat /proc/sys/kernel/random/uuid 2>/dev/null \
              | tr -d '-' | head -c16 \
              || date +%s | md5sum 2>/dev/null | head -c16 \
              || date +%s%N)
  fi
  echo "$_raw_id" > "$_ID_FILE"
fi
INSTANCE_ID=$(cat "$_ID_FILE")

# Docker Compose lowercases project name → match chính xác
PROJECT=$(echo "${COMPOSE_PROJECT_NAME:-omniroute-s3-litestream}" \
          | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9-' '-' \
          | sed 's/^-*//; s/-*$//')

LOCK_KEY="${LEADER_LOCK_KEY:-leader-lock-${PROJECT}}"
LOCK_TTL="${LEADER_LOCK_TTL:-30}"        # giây — TTL của distributed lock
HEARTBEAT="${HEARTBEAT_INTERVAL:-10}"    # giây — interval gia hạn lock

# Thứ tự khởi động khi trở thành leader (QUAN TRỌNG: litestream trước)
LEADER_START_ORDER="litestream omniroute cloudflared"
# Thứ tự dừng khi trở thành follower (tunnel tắt trước để Cloudflare skip)
FOLLOWER_STOP_ORDER="cloudflared omniroute litestream"

# ──────────────────────────────────────────────────────────────────────
# 2. Logging
# ──────────────────────────────────────────────────────────────────────
log()  { echo "[elector $(date '+%H:%M:%S')] $*"; }
info() { log "ℹ  $*"; }
warn() { log "⚠  $*" >&2; }

# Cảnh báo khi CI inject placeholder literal thay vì giá trị thực
[ "${COMPOSE_PROJECT_NAME:-}" = "COMPOSE_PROJECT_NAME" ] \
  && warn "COMPOSE_PROJECT_NAME đang là placeholder literal"
[ "${INSTANCE_ID:-}" = "INSTANCE_ID" ] \
  && warn "INSTANCE_ID đang là placeholder literal"

# ──────────────────────────────────────────────────────────────────────
# 3. Docker helpers
# ──────────────────────────────────────────────────────────────────────

# Tìm container name qua compose labels (đúng hơn là dùng container_name cứng)
get_cname() {
  docker ps -a \
    --filter "label=com.docker.compose.service=${1}" \
    --filter "label=com.docker.compose.project=${PROJECT}" \
    --format "{{.Names}}" | head -1
}

is_running() {
  local c
  c=$(get_cname "$1") || return 1
  [ -z "$c" ] && return 1
  local state
  state=$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null || echo "false")
  [ "$state" = "true" ]
}

get_health() {
  local c
  c=$(get_cname "$1")
  [ -z "$c" ] && echo "missing" && return
  docker inspect -f \
    '{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' \
    "$c" 2>/dev/null || echo "unknown"
}

svc_start() {
  local svc c
  svc="$1"
  c=$(get_cname "$svc")
  if [ -z "$c" ]; then
    warn "Container không tìm thấy cho service: $svc"
    return 1
  fi
  if is_running "$svc"; then
    info "$svc đã đang chạy"
    return 0
  fi
  log "▶ Starting $svc ($c)..."
  docker start "$c" \
    && log "  ✅ $svc started" \
    || warn "  ✖ Failed to start $svc"
}

svc_stop() {
  local svc timeout c
  svc="$1"
  timeout="${2:-10}"
  c=$(get_cname "$svc")
  [ -z "$c" ] && return 0
  is_running "$svc" || return 0
  log "■ Stopping $svc (grace=${timeout}s)..."
  docker stop -t "$timeout" "$c" \
    && log "  ✅ $svc stopped" \
    || warn "  ✖ Failed to stop $svc"
}

svc_ensure_running() {
  # Dùng trong heartbeat — restart nếu crash trong khi đang là leader
  if ! is_running "$1"; then
    warn "$1 crashed, restarting..."
    svc_start "$1" || true
  fi
}

wait_healthy() {
  local svc="${1}"
  local timeout="${2:-180}"
  local interval=5
  local waited=0
  local h

  log "⏳ Chờ $svc healthy (tối đa ${timeout}s)..."
  while [ "$waited" -lt "$timeout" ]; do
    h=$(get_health "$svc")
    case "$h" in
      healthy|no-healthcheck)
        log "  $svc: $h ✅"
        return 0
        ;;
      unhealthy)
        warn "  $svc: unhealthy!"
        return 1
        ;;
      missing)
        warn "  $svc: container không tồn tại!"
        return 1
        ;;
    esac
    sleep "$interval"
    waited=$((waited + interval))
    log "  $svc: $h — ${waited}/${timeout}s"
  done

  warn "$svc không healthy sau ${timeout}s, tiếp tục..."
  return 1
}

# ──────────────────────────────────────────────────────────────────────
# 4. Firebase RTDB helpers
#
# Atomic election dựa trên:
#   GET /lock.json         → trả về body + ETag header
#   PUT /lock.json
#       If-Match: "<etag>" → 200 nếu ETag khớp (win)
#                          → 412 nếu ETag không khớp (lost race)
# ──────────────────────────────────────────────────────────────────────
_build_url() {
  local path="$1"
  if [ -n "$RTDB_QUERY" ]; then
    echo "${RTDB_BASE}/${path}.json?${RTDB_QUERY}"
  else
    echo "${RTDB_BASE}/${path}.json"
  fi
}

LAST_ETAG=""

rtdb_get() {
  # GET lock node, lưu ETag vào LAST_ETAG
  local url body
  url=$(_build_url "$LOCK_KEY")
  body=$(curl -sS --max-time 10 \
    -H "X-Firebase-ETag: true" \
    -D /tmp/rtdb-resp-hdr \
    "$url" 2>/dev/null || echo "null")
  LAST_ETAG=$(grep -i '^etag:' /tmp/rtdb-resp-hdr 2>/dev/null \
    | tr -d '\r\n' | sed "s/.*\"\\([^\"]*\\)\".*/\\1/" || echo "")
  echo "$body"
}

rtdb_put() {
  # PUT không điều kiện (dùng khi đã chắc chắn là leader)
  local url
  url=$(_build_url "$LOCK_KEY")
  curl -sf --max-time 10 -X PUT \
    -H "Content-Type: application/json" \
    -d "$1" \
    "$url" > /dev/null 2>&1 || true
}

rtdb_conditional_put() {
  # PUT với If-Match — atomic compare-and-swap
  # Return: HTTP status code (200=win, 412=lost, 000=error)
  local url
  url=$(_build_url "$LOCK_KEY")
  curl -sS --max-time 10 \
    -o /dev/null -w "%{http_code}" \
    -X PUT \
    -H "If-Match: \"${LAST_ETAG}\"" \
    -H "Content-Type: application/json" \
    -d "$1" \
    "$url" 2>/dev/null \
    || echo "000"
}

rtdb_delete() {
  local url
  url=$(_build_url "$LOCK_KEY")
  curl -sf --max-time 10 -X DELETE "$url" > /dev/null 2>&1 || true
}

_make_lock_payload() {
  local now exp
  now=$(date +%s)
  exp=$((now + LOCK_TTL))
  jq -n \
    --arg  id  "$INSTANCE_ID" \
    --argjson exp "$exp" \
    --argjson now "$now" \
    '{instance_id: $id, expires_at: $exp, acquired_at: $now}'
}

# ──────────────────────────────────────────────────────────────────────
# 5. Leader Election Logic
# ──────────────────────────────────────────────────────────────────────
_RTDB_ERR=0  # đếm consecutive RTDB failures

try_acquire_lock() {
  local body now expires holder code

  now=$(date +%s)
  body=$(rtdb_get)

  # RTDB không trả lời được
  if [ -z "$body" ]; then
    _RTDB_ERR=$((_RTDB_ERR + 1))
    warn "RTDB không phản hồi (lần $_RTDB_ERR)"
    return 1
  fi
  _RTDB_ERR=0

  expires=$(echo "$body" | jq -r '.expires_at // 0' 2>/dev/null || echo "0")
  holder=$(echo "$body"  | jq -r '.instance_id // ""' 2>/dev/null || echo "")

  # Đang giữ lock → gia hạn
  if [ "$holder" = "$INSTANCE_ID" ]; then
    rtdb_put "$(_make_lock_payload)"
    return 0
  fi

  # Lock trống hoặc expired → tranh giành
  if [ "$body" = "null" ] || [ "$expires" -lt "$now" ]; then
    local payload
    payload=$(_make_lock_payload)
    code=$(rtdb_conditional_put "$payload")

    if [ "$code" = "200" ]; then
      info "🏆 Thắng election (HTTP $code)"
      return 0
    fi
    info "Thua election race (HTTP $code) — instance khác nhanh hơn"
  fi

  return 1  # ai đó đang giữ lock hợp lệ
}

renew_lock() {
  rtdb_put "$(_make_lock_payload)"
}

check_still_leader() {
  local body now expires holder

  now=$(date +%s)
  body=$(curl -sf --max-time 10 "$(_build_url "$LOCK_KEY")" 2>/dev/null || echo "ERROR")

  if [ "$body" = "ERROR" ]; then
    _RTDB_ERR=$((_RTDB_ERR + 1))
    warn "RTDB unreachable trong heartbeat ($_RTDB_ERR/3)"
    # Chịu đựng tối đa 3 lần RTDB down liên tiếp (~30s) trước khi demote
    # Tránh false demotion khi RTDB tạm thời flaky
    [ "$_RTDB_ERR" -lt 3 ] && return 0
    return 1
  fi
  _RTDB_ERR=0

  expires=$(echo "$body" | jq -r '.expires_at // 0' 2>/dev/null || echo "0")
  holder=$(echo "$body"  | jq -r '.instance_id // ""' 2>/dev/null || echo "")

  [ "$holder" = "$INSTANCE_ID" ] && [ "$expires" -gt "$now" ]
}

release_lock() {
  local body holder
  body=$(curl -sf --max-time 10 "$(_build_url "$LOCK_KEY")" 2>/dev/null || echo "null")
  holder=$(echo "$body" | jq -r '.instance_id // ""' 2>/dev/null || echo "")
  if [ "$holder" = "$INSTANCE_ID" ]; then
    rtdb_delete
    log "🔓 Lock released"
  fi
}

# ──────────────────────────────────────────────────────────────────────
# 6. Role Transitions
# ──────────────────────────────────────────────────────────────────────
IS_LEADER=false

on_become_leader() {
  log "══════════════════════════════════════"
  log "🎉 LEADER — $INSTANCE_ID"
  log "══════════════════════════════════════"
  IS_LEADER=true

  # Bước 1: Start litestream TRƯỚC
  # startup.sh sẽ: check S3 → restore nếu cần → exec litestream replicate
  svc_start "litestream"

  # Bước 2: Chờ litestream healthy (restore xong, replicate đang chạy)
  # Nếu timeout vẫn tiếp tục — omniroute có thể chạy dù litestream chưa sync
  wait_healthy "litestream" 180 \
    && log "Litestream ready ✅" \
    || warn "Litestream chưa healthy — omniroute sẽ start nhưng backup có thể bị lag"

  # Bước 3: Start app + tunnel
  svc_start "omniroute"
  svc_start "cloudflared"

  log "✅ LEADER mode active — traffic sẽ được route đến instance này"
}

on_become_follower() {
  log "══════════════════════════════════════"
  log "📡 FOLLOWER — $INSTANCE_ID"
  log "══════════════════════════════════════"
  IS_LEADER=false

  # Tắt theo thứ tự: tunnel trước để Cloudflare ngừng route
  # → omniroute sau (xử lý nốt in-flight requests)
  # → litestream cuối (đảm bảo WAL đã được flush)
  svc_stop "cloudflared" 10   # Cloudflare sẽ route sang instance khác ngay
  svc_stop "omniroute"   35   # Grace period cho in-flight requests
  svc_stop "litestream"  15   # Flush WAL trước khi dừng

  log "✅ FOLLOWER mode — hot standby (sẵn sàng lên leader)"
}

# ──────────────────────────────────────────────────────────────────────
# 7. Graceful Shutdown
# ──────────────────────────────────────────────────────────────────────
_cleanup() {
  log "🛑 Elector shutting down (signal received)..."
  if $IS_LEADER; then
    on_become_follower
    release_lock
  fi
  log "Goodbye from $INSTANCE_ID"
}
trap _cleanup EXIT INT TERM

# ──────────────────────────────────────────────────────────────────────
# 8. Main Loop
# ──────────────────────────────────────────────────────────────────────
log "╔══════════════════════════════════════╗"
log "║  Leader Elector starting             ║"
log "╠══════════════════════════════════════╣"
log "║ Instance  : $INSTANCE_ID"
log "║ Project   : $PROJECT"
log "║ Lock key  : $LOCK_KEY"
log "║ TTL       : ${LOCK_TTL}s"
log "║ Heartbeat : ${HEARTBEAT}s"
log "╚══════════════════════════════════════╝"

# ── Init: dừng TẤT CẢ managed services ───────────────────────────────
# Elector là người duy nhất quyết định khi nào services được start.
# Tránh: litestream replicate chạy trên follower → S3 corruption
log "Init: stopping all managed services..."
for _svc in $FOLLOWER_STOP_ORDER; do
  svc_stop "$_svc" 5 2>/dev/null || true
done
log "Init complete — bắt đầu election loop"

# ── Election loop ─────────────────────────────────────────────────────
while true; do

  if $IS_LEADER; then
    # ── Leader heartbeat ────────────────────────────────────────────
    if check_still_leader; then
      renew_lock

      # Health monitor — restart services nếu crash trong khi là leader
      for _svc in litestream omniroute cloudflared; do
        svc_ensure_running "$_svc"
      done

      log "💚 Heartbeat OK — leader=$INSTANCE_ID"
    else
      warn "❌ Mất leader lock!"
      on_become_follower
    fi

  else
    # ── Follower: thử giành lock ────────────────────────────────────
    if try_acquire_lock; then
      on_become_leader
    else
      _cur_leader=$(curl -sf --max-time 5 "$(_build_url "$LOCK_KEY")" 2>/dev/null \
        | jq -r '.instance_id // "unknown"' 2>/dev/null || echo "unknown")
      log "👥 Follower — leader hiện tại: $_cur_leader"
    fi
  fi

  sleep "$HEARTBEAT"
done
