#!/usr/bin/env bash
# scripts/start-console-dev.sh — 웹 콘솔 개발/테스트용 환경 기동
#
# 실행: bash scripts/start-console-dev.sh
#
# 기동 구성:
#   PM   gRPC :8000  HTTP :8080
#   PS-1 :8001
#   PS-2 :8002
#
# 종료: Ctrl-C (모든 자식 프로세스 자동 정리)

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BIN_DIR="$ROOT_DIR/bin"
LOG_DIR="/tmp/ab-console"
WAL_DIR="$LOG_DIR/wal"
CKPT_DIR="$LOG_DIR/ckpt"
ETCD="localhost:2379"
PM_ADDR="localhost:8000"
HTTP_ADDR=":8080"

PID_FILE="$LOG_DIR/pids"

# ── 색상 출력 ─────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log()  { echo -e "${CYAN}[$(date '+%H:%M:%S')]${NC} $*"; }
ok()   { echo -e "${GREEN}[$(date '+%H:%M:%S')] ✓${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')] !${NC} $*"; }
die()  { echo -e "${RED}[$(date '+%H:%M:%S')] ✗ $*${NC}" >&2; exit 1; }

PM_PID=""; PS1_PID=""; PS2_PID=""

# ── Ctrl-C 시 모든 자식 프로세스 정리 ────────────────────────────────────────
cleanup() {
    echo ""
    log "Shutting down..."
    for pid in "$PM_PID" "$PS1_PID" "$PS2_PID"; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null && wait "$pid" 2>/dev/null || true
    done
    rm -f "$PID_FILE"
    ok "All processes stopped."
}
trap cleanup EXIT INT TERM

# ── 사전 확인 ─────────────────────────────────────────────────────────────────
log "Checking prerequisites..."

etcdctl endpoint health --endpoints="$ETCD" &>/dev/null \
    || die "etcd is not running at $ETCD. Start it first."
ok "etcd OK"

# ── 빌드 ─────────────────────────────────────────────────────────────────────
log "Building binaries..."
cd "$ROOT_DIR"
go build -o bin/pm        ./cmd/pm
go build -o bin/abctl     ./cmd/abctl
go build -o bin/kv_server ./examples/kv_server
go build -o bin/kv_client ./examples/kv_client
ok "Build OK"

# ── 환경 초기화 ───────────────────────────────────────────────────────────────
log "Resetting state..."
"$ROOT_DIR/scripts/stop-console-dev.sh" 2>/dev/null || true
etcdctl del --prefix /actorbase &>/dev/null || true
rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR" "$WAL_DIR" "$CKPT_DIR"
ok "State reset"

# ── PM 기동 ───────────────────────────────────────────────────────────────────
log "Starting PM (gRPC $PM_ADDR, HTTP $HTTP_ADDR)..."
"$BIN_DIR/pm" \
    -addr  ":8000" \
    -http  "$HTTP_ADDR" \
    -etcd  "$ETCD" \
    -actor-types kv \
    > "$LOG_DIR/pm.log" 2>&1 &
PM_PID=$!

for i in $(seq 10); do
    sleep 0.5
    grep -q "elected as leader" "$LOG_DIR/pm.log" 2>/dev/null && break
    [ "$i" -eq 10 ] && die "PM failed to start. Check $LOG_DIR/pm.log"
done
ok "PM started (PID $PM_PID)"

# ── PS-1 기동 ─────────────────────────────────────────────────────────────────
log "Starting PS-1 (:8001)..."
"$BIN_DIR/kv_server" \
    -node-id ps-1 \
    -addr   localhost:8001 \
    -etcd   "$ETCD" \
    -wal-dir        "$WAL_DIR" \
    -checkpoint-dir "$CKPT_DIR" \
    > "$LOG_DIR/ps1.log" 2>&1 &
PS1_PID=$!
sleep 1
ok "PS-1 started (PID $PS1_PID)"

# ── PS-2 기동 ─────────────────────────────────────────────────────────────────
log "Starting PS-2 (:8002)..."
"$BIN_DIR/kv_server" \
    -node-id ps-2 \
    -addr   localhost:8002 \
    -etcd   "$ETCD" \
    -wal-dir        "$WAL_DIR" \
    -checkpoint-dir "$CKPT_DIR" \
    > "$LOG_DIR/ps2.log" 2>&1 &
PS2_PID=$!
sleep 2
ok "PS-2 started (PID $PS2_PID)"

# PID 파일 저장 (stop 스크립트에서 사용)
printf "PM=%s\nPS1=%s\nPS2=%s\n" "$PM_PID" "$PS1_PID" "$PS2_PID" > "$PID_FILE"

# ── 클러스터 상태 확인 ────────────────────────────────────────────────────────
log "Cluster members:"
"$BIN_DIR/abctl" -pm "$PM_ADDR" members

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  Web Console : http://localhost:8080${NC}"
echo -e "${GREEN}  PM gRPC     : localhost:8000${NC}"
echo -e "${GREEN}  Logs        : $LOG_DIR/${NC}"
echo -e "${GREEN}  Stop        : bash scripts/stop-console-dev.sh${NC}"
echo -e "${GREEN}  Press Ctrl-C to stop all processes${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# ── 프로세스 감시 ─────────────────────────────────────────────────────────────
while true; do
    sleep 3
    for entry in "PM:$PM_PID" "PS-1:$PS1_PID" "PS-2:$PS2_PID"; do
        name="${entry%%:*}"
        pid="${entry##*:}"
        if ! kill -0 "$pid" 2>/dev/null; then
            warn "$name (PID $pid) exited unexpectedly. Check $LOG_DIR/"
        fi
    done
done
