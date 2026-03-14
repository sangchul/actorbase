#!/usr/bin/env bash
# run_autobalance.sh — auto balancer 통합 longrun 테스트
#
# 실행 방법 (프로젝트 루트에서):
#   bash test/longrun/run_autobalance.sh
#
# 기존 run.sh와의 차이점:
#   - 클러스터 기동 후 abctl policy apply로 ThresholdPolicy 활성화
#   - chaos는 SIGKILL/SIGTERM + 재기동만 수행 (split/migrate는 auto balancer가 담당)
#
# 클러스터 구성:
#   PM    : localhost:8000
#   PS-1  : localhost:8001
#   PS-2  : localhost:8002
#   PS-3  : localhost:8003

set -euo pipefail

# ── 경로/설정 ─────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN_DIR="$ROOT_DIR/bin"

PM_ADDR="localhost:8000"
ETCD_ADDR="localhost:2379"
WAL_DIR="/tmp/actorbase/wal"
CKPT_DIR="/tmp/actorbase/checkpoint"
LEDGER="/tmp/ab_autobalance_ledger.json"
DURATION="8m"

RUN_ID="$(date '+%Y%m%d_%H%M%S')"
LOG_DIR="/tmp/actorbase_runs/autobalance_$RUN_ID"
mkdir -p "$LOG_DIR"

PM_LOG="$LOG_DIR/pm.log"
PS1_LOG="$LOG_DIR/ps1.log"
PS2_LOG="$LOG_DIR/ps2.log"
PS3_LOG="$LOG_DIR/ps3.log"
LONGRUN_LOG="$LOG_DIR/longrun.log"

ln -sf "$PM_LOG"      /tmp/ab_pm.log
ln -sf "$PS1_LOG"     /tmp/ab_ps1.log
ln -sf "$PS2_LOG"     /tmp/ab_ps2.log
ln -sf "$PS3_LOG"     /tmp/ab_ps3.log
ln -sf "$LONGRUN_LOG" /tmp/ab_longrun.log

PS1_PID_FILE="/tmp/ab_ps1.pid"
PS2_PID_FILE="/tmp/ab_ps2.pid"
PS3_PID_FILE="/tmp/ab_ps3.pid"

PM_PID=""
LONGRUN_PID=""
CHAOS_PID=""

# ── auto balancer에 적용할 ThresholdPolicy YAML ───────────────────────────────
#
# key_threshold: 2000  → 파티션당 키가 2000 초과 시 split 유도
#                         (10,000키 부하에서 여러 번 split 발생 예상)
# check_interval: 20s  → 8분 테스트 내 충분히 발동
# max_partition_diff: 1 → 노드 간 파티션 수 1개 이상 차이나면 migrate
# cooldown: split/migrate 연쇄 방지

POLICY_YAML='
algorithm: threshold
check_interval: 20s
cooldown:
  global: 30s
  partition: 60s
split:
  rps_threshold: 0
  key_threshold: 2000
balance:
  max_partition_diff: 1
  rps_imbalance_pct: 0
'

# ── 유틸리티 ──────────────────────────────────────────────────────────────────

log() { echo "[$(date '+%H:%M:%S')] [run] $*"; }

cleanup() {
  log "Cleaning up... (logs: $LOG_DIR)"
  [[ -n "$CHAOS_PID" ]]   && kill "$CHAOS_PID"   2>/dev/null || true
  [[ -n "$LONGRUN_PID" ]] && kill "$LONGRUN_PID" 2>/dev/null || true
  for pf in "$PS1_PID_FILE" "$PS2_PID_FILE" "$PS3_PID_FILE"; do
    if [[ -f "$pf" ]]; then
      pid=$(cat "$pf"); kill "$pid" 2>/dev/null || true; rm -f "$pf"
    fi
  done
  [[ -n "$PM_PID" ]] && kill "$PM_PID" 2>/dev/null || true
  log "Cleanup done."
}
trap cleanup EXIT

# ── 바이너리 경로 확인 ────────────────────────────────────────────────────────

for bin in pm kv_server kv_longrun abctl; do
  if [[ ! -x "$BIN_DIR/$bin" ]]; then
    log "ERROR: $BIN_DIR/$bin not found. Run: go build -o bin/$bin ./..."
    exit 1
  fi
done

# ── 데이터 초기화 ─────────────────────────────────────────────────────────────

log "Cleaning old data (etcd, WAL, checkpoint, ledger)..."
etcdctl --endpoints="$ETCD_ADDR" del /actorbase/ --prefix >/dev/null 2>&1 || true
rm -rf "$WAL_DIR" "$CKPT_DIR" "$LEDGER"
mkdir -p "$WAL_DIR" "$CKPT_DIR"
for log_file in "$PM_LOG" "$PS1_LOG" "$PS2_LOG" "$PS3_LOG" "$LONGRUN_LOG"; do
  > "$log_file"
done

# ── PM 기동 ──────────────────────────────────────────────────────────────────

log "Starting PM ($PM_ADDR)..."
"$BIN_DIR/pm" -addr "$PM_ADDR" -actor-types kv >> "$PM_LOG" 2>&1 &
PM_PID=$!
sleep 1
if ! kill -0 "$PM_PID" 2>/dev/null; then
  log "ERROR: PM failed to start. See $PM_LOG"
  exit 1
fi
log "PM started (pid=$PM_PID)"

# ── PS 기동 함수 ──────────────────────────────────────────────────────────────

start_ps() {
  local node_id="$1" addr="$2" pid_file="$3" log_file="$4"
  "$BIN_DIR/kv_server" \
    -node-id "$node_id" \
    -addr "$addr" \
    -etcd "$ETCD_ADDR" \
    -wal-dir "$WAL_DIR" \
    -checkpoint-dir "$CKPT_DIR" \
    >> "$log_file" 2>&1 &
  echo $! > "$pid_file"
  log "$node_id started (pid=$(cat "$pid_file"), addr=$addr)"
}

# ── PS-1,2,3 기동 ────────────────────────────────────────────────────────────

start_ps "ps-1" "localhost:8001" "$PS1_PID_FILE" "$PS1_LOG"
start_ps "ps-2" "localhost:8002" "$PS2_PID_FILE" "$PS2_LOG"
start_ps "ps-3" "localhost:8003" "$PS3_PID_FILE" "$PS3_LOG"
sleep 2

log "Log dir: $LOG_DIR"
log "Cluster up. Members:"
"$BIN_DIR/abctl" -pm "$PM_ADDR" members || true

# ── auto balancer policy 적용 ─────────────────────────────────────────────────

log "Applying ThresholdPolicy (key_threshold=2000, check_interval=20s)..."
POLICY_FILE=$(mktemp /tmp/ab_policy_XXXXXX.yaml)
echo "$POLICY_YAML" > "$POLICY_FILE"
if "$BIN_DIR/abctl" -pm "$PM_ADDR" policy apply "$POLICY_FILE"; then
  log "Policy applied. Auto balancer active."
else
  log "ERROR: policy apply failed"
  rm -f "$POLICY_FILE"
  exit 1
fi
rm -f "$POLICY_FILE"

# ── kv_longrun 부하 시작 ──────────────────────────────────────────────────────

log "Starting kv_longrun (duration=$DURATION)..."
"$BIN_DIR/kv_longrun" \
  -pm "$PM_ADDR" \
  -duration "$DURATION" \
  -workers 20 \
  -keys 10000 \
  -ledger "$LEDGER" \
  -flush-interval 10s \
  -set-ratio 0.6 \
  -del-ratio 0.2 \
  >> "$LONGRUN_LOG" 2>&1 &
LONGRUN_PID=$!

# ── chaos 이벤트 주입 (장애/재기동만) ────────────────────────────────────────

log "Starting chaos events (failover + rejoin only, auto balancer handles split/migrate)..."
bash "$SCRIPT_DIR/chaos_autobalance.sh" "$BIN_DIR" "$PM_ADDR" "$WAL_DIR" "$CKPT_DIR" "$ETCD_ADDR" \
  >> /tmp/ab_chaos_autobalance.log 2>&1 &
CHAOS_PID=$!

# ── 부하 종료 대기 ────────────────────────────────────────────────────────────

log "Waiting for kv_longrun to finish (duration=$DURATION + buffer)..."
wait "$LONGRUN_PID" || true
LONGRUN_PID=""
log "kv_longrun finished."

if [[ -n "$CHAOS_PID" ]] && kill -0 "$CHAOS_PID" 2>/dev/null; then
  kill "$CHAOS_PID" 2>/dev/null || true
  wait "$CHAOS_PID" 2>/dev/null || true
fi
CHAOS_PID=""

# ── 안정화 대기 ───────────────────────────────────────────────────────────────

log "Waiting 30s for cluster to stabilize..."
sleep 30

log "Current routing table:"
"$BIN_DIR/abctl" -pm "$PM_ADDR" routing || true

log "Current policy:"
"$BIN_DIR/abctl" -pm "$PM_ADDR" policy get || true

# ── 검증 ─────────────────────────────────────────────────────────────────────

log "Running verification..."
if "$BIN_DIR/kv_longrun" -pm "$PM_ADDR" -verify -ledger "$LEDGER"; then
  log "===== PASS ====="
else
  log "===== FAIL ====="
  log "Longrun log:  $LONGRUN_LOG"
  log "Chaos log:    /tmp/ab_chaos_autobalance.log"
  log "PM log:       $PM_LOG"
  exit 1
fi
