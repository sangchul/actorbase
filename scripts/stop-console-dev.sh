#!/usr/bin/env bash
# scripts/stop-console-dev.sh — 웹 콘솔 개발 환경 종료
#
# start-console-dev.sh가 저장한 PID 파일을 읽어 종료.
# PID 파일이 없으면 바이너리 이름으로 pkill.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="/tmp/ab-console/pids"

GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'
log() { echo -e "${CYAN}[$(date '+%H:%M:%S')]${NC} $*"; }
ok()  { echo -e "${GREEN}[$(date '+%H:%M:%S')] ✓${NC} $*"; }

stop_pid() {
    local name="$1" pid="$2"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" && wait "$pid" 2>/dev/null || true
        ok "$name (PID $pid) stopped"
    fi
}

if [ -f "$PID_FILE" ]; then
    log "Stopping via PID file..."
    PM_PID="";  PS1_PID=""; PS2_PID=""
    while IFS='=' read -r key val; do
        case "$key" in
            PM)  PM_PID="$val"  ;;
            PS1) PS1_PID="$val" ;;
            PS2) PS2_PID="$val" ;;
        esac
    done < "$PID_FILE"
    stop_pid "PM"   "$PM_PID"
    stop_pid "PS-1" "$PS1_PID"
    stop_pid "PS-2" "$PS2_PID"
    rm -f "$PID_FILE"
else
    log "PID file not found, using pkill..."
    pkill -f "$ROOT_DIR/bin/pm"        2>/dev/null && ok "PM stopped"        || true
    pkill -f "$ROOT_DIR/bin/kv_server" 2>/dev/null && ok "kv_server stopped" || true
fi

ok "Done."
