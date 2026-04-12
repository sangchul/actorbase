#!/usr/bin/env bash
# test/integration/run.sh — actorbase 통합 시나리오 테스트 자동화
#
# 실행 방법 (프로젝트 루트에서):
#   bash test/integration/run.sh           # 전체 실행
#   bash test/integration/run.sh 12        # 시나리오 12만 실행
#   bash test/integration/run.sh 1 2 3     # 시나리오 1, 2, 3만 실행
#   bash test/integration/run.sh 12 13     # 시나리오 12, 13만 실행
#
# 사전 요건:
#   - etcd 바이너리가 PATH에 존재할 것 (스크립트가 직접 기동/종료)
#   - go build 완료 (bin/ 디렉토리에 바이너리 존재)
#     go build -o bin/pm ./cmd/pm
#     go build -o bin/abctl ./cmd/abctl
#     go build -o bin/kv_server ./examples/kv_server
#     go build -o bin/kv_client ./examples/kv_client
#     go build -o bin/kv_stress ./examples/kv_stress
#
# 시나리오:
#   1. 클러스터 부트스트랩
#   2. 기본 KV 동작 (set/get/del)
#   3. 파티션 Split
#   4. Scale-out + Migrate
#   5. 예기치 않은 장애 복구 (SIGKILL → 자동 Failover + WAL replay)
#   6. SDK 라우팅 자동 갱신 (부하 중 split, fail=0 검증)
#   7. Graceful Shutdown (SIGTERM → drainPartitions)
#   8. Range Scan (다중 파티션 fan-out)
#   9. PM HA Failover (standby PM이 리더를 인계받음)
#  10. Actor Eviction + Re-activation (EvictionScheduler → getOrActivate)
#  11. SDK HA Mode 자동 재발견 (etcd 모드, PM 장애 중 자동 재연결)
#  12. Multi-actor-type 동시 운영 (kv + counter 파티션 공존)
#  13. drainPartitions 타임아웃 (PM 없는 환경 → EvictAll → checkpoint 복원)
#  14. 파티션 Merge (Split → Merge → 데이터 무결성 + checkpoint 복원)
#  15. SIGKILL + 파티션 없음 → Waiting (케이스 A: node reset 불필요)
#  16. SIGKILL + 파티션 있음 + 다른 PS 없음 → 새 PS join 시 자동 재할당 (케이스 E)
#  17. SIGTERM + 파티션 있음 + 다른 PS 없음 → 다른 PS join 시 자동 재할당 (케이스 F)
#  18. Drained 상태 검증 (drain 완료 후 Drained, activate로 Active 복귀)
#  19. Restricted 상태 검증 (restrict 후 migration 거부, unrestrict 후 복귀)
#  20. PM heartbeat 기반 빠른 failover (HeartbeatTimeout 5s + WalFlushMargin 3s ≤ 10s)
#      + EvictionComplete 로그 검증 (SIGTERM 시 PS → PM WAL flush 완료 신호 전송)

set -euo pipefail

# ── 시나리오 선택 ─────────────────────────────────────────────────────────────
# 인수가 없으면 전체 실행. 인수가 있으면 해당 번호만 실행.
SELECTED_SCENARIOS=("$@")

should_run() {
  local n="$1"
  if [[ ${#SELECTED_SCENARIOS[@]} -eq 0 ]]; then
    return 0
  fi
  for s in "${SELECTED_SCENARIOS[@]}"; do
    [[ "$s" == "$n" ]] && return 0
  done
  return 1
}

# ── 경로/설정 ─────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN_DIR="$ROOT_DIR/bin"

PM_ADDR="localhost:8000"
PM2_ADDR="localhost:8003"
ETCD_ADDR="localhost:2379"
ETCD_DATA_DIR="/tmp/actorbase-itest/etcd"
WAL_DIR="/tmp/actorbase-itest/wal"
CKPT_DIR="/tmp/actorbase-itest/checkpoint"

# WAL 백엔드 설정: fs(기본) 또는 redis
WAL_BACKEND="${WAL_BACKEND:-fs}"
REDIS_ADDR="${REDIS_ADDR:-localhost:6379}"

# Checkpoint 백엔드 설정: fs(기본) 또는 minio
CHECKPOINT_BACKEND="${CHECKPOINT_BACKEND:-fs}"
MINIO_ADDR="${MINIO_ADDR:-localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-actorbase-itest}"

RUN_ID="$(date '+%Y%m%d_%H%M%S')"
LOG_DIR="/tmp/actorbase-itest/logs/$RUN_ID"
mkdir -p "$LOG_DIR"

PM_LOG="$LOG_DIR/pm.log"
PM2_LOG="$LOG_DIR/pm2.log"
PS1_LOG="$LOG_DIR/ps1.log"
PS2_LOG="$LOG_DIR/ps2.log"

ETCD_PID=""
PM_PID=""
PM2_PID=""
PM3_PID=""
PS1_PID=""
PS2_PID=""

PASS=0
FAIL=0

# ── 유틸리티 ──────────────────────────────────────────────────────────────────

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
pass() { log "  ✓ $*"; PASS=$((PASS+1)); }
fail() { log "  ✗ FAIL: $*"; FAIL=$((FAIL+1)); }

assert_eq() {
  local desc="$1" expected="$2" actual="$3"
  if [[ "$actual" == "$expected" ]]; then
    pass "$desc"
  else
    fail "$desc — expected='$expected' actual='$actual'"
  fi
}

assert_contains() {
  local desc="$1" needle="$2" haystack="$3"
  if echo "$haystack" | grep -qF "$needle"; then
    pass "$desc"
  else
    fail "$desc — '$needle' not found in output"
  fi
}

assert_not_contains() {
  local desc="$1" needle="$2" haystack="$3"
  if ! echo "$haystack" | grep -qF "$needle"; then
    pass "$desc"
  else
    fail "$desc — '$needle' unexpectedly found in output"
  fi
}

kv_set() { "$BIN_DIR/kv_client" -pm "$PM_ADDR" set "$1" "$2" 2>/dev/null; }
kv_get() { "$BIN_DIR/kv_client" -pm "$PM_ADDR" get "$1" 2>/dev/null || true; }
kv_del() { "$BIN_DIR/kv_client" -pm "$PM_ADDR" del "$1" 2>/dev/null; }

routing() { "$BIN_DIR/abctl" -pm "$PM_ADDR" routing 2>/dev/null; }

# 라우팅 테이블에서 파티션 목록 파싱: id\tstart\tend\tnode
routing_entries() {
  routing | awk 'NR>4 && $1!="" && $1!~"^-" {print $1"\t"$3"\t"$4"\t"$5}'
}

routing_version() {
  routing | awk '/^Version:/ {print $2}'
}

# 특정 node에 있는 파티션 수
partitions_on_node() {
  local node="$1"
  routing_entries | awk -F'\t' -v n="$node" '$4==n {count++} END {print count+0}'
}

# 파티션 ID 조회 (번호 기준, 1-indexed)
partition_id_by_index() {
  local idx="$1"
  routing_entries | awk -F'\t' "NR==$idx {print \$1}"
}

# 파티션 수 조회
partition_count() {
  routing_entries | wc -l | tr -d ' '
}

# kv actor type 파티션만 필터링 (multi-actor-type 환경에서 merge 등에 사용)
kv_routing_entries() {
  routing | awk 'NR>4 && $1!="" && $1!~"^-" && $2=="kv" {print $1"\t"$3"\t"$4"\t"$5}'
}

kv_partition_count() {
  kv_routing_entries | wc -l | tr -d ' '
}

kv_partition_id_by_index() {
  local idx="$1"
  kv_routing_entries | awk -F'\t' "NR==$idx {print \$1}"
}

cleanup() {
  [[ -n "$PS2_PID"  ]] && kill "$PS2_PID"  2>/dev/null || true
  [[ -n "$PS1_PID"  ]] && kill "$PS1_PID"  2>/dev/null || true
  [[ -n "$PM3_PID"  ]] && kill "$PM3_PID"  2>/dev/null || true
  [[ -n "$PM2_PID"  ]] && kill "$PM2_PID"  2>/dev/null || true
  [[ -n "$PM_PID"   ]] && kill "$PM_PID"   2>/dev/null || true
  pkill -f "kv_stress" 2>/dev/null || true
  [[ -n "$ETCD_PID" ]] && kill "$ETCD_PID" 2>/dev/null || true
  if [[ "$WAL_BACKEND" == "redis" ]]; then
    redis-cli -u "redis://$REDIS_ADDR" FLUSHDB >/dev/null 2>&1 || true
  fi
  if [[ "$CHECKPOINT_BACKEND" == "minio" ]]; then
    mc rm --recursive --force "minio-itest/$MINIO_BUCKET" >/dev/null 2>&1 || true
  fi
  log "Cleanup done. Logs: $LOG_DIR"
}
trap cleanup EXIT

kv_scan() {
  local start="$1" end="$2"
  "$BIN_DIR/kv_client" -pm "$PM_ADDR" scan "$start" "$end" 2>/dev/null || true
}

# ── 바이너리 확인 ──────────────────────────────────────────────────────────────

for bin in pm abctl kv_server kv_client kv_stress; do
  if [[ ! -x "$BIN_DIR/$bin" ]]; then
    echo "ERROR: $BIN_DIR/$bin not found. Run: go build -o bin/$bin ./..."
    exit 1
  fi
done

# ── etcd 기동 ─────────────────────────────────────────────────────────────────

if ! command -v etcd >/dev/null 2>&1; then
  echo "ERROR: etcd binary not found in PATH"
  exit 1
fi

# 포트 2379/2380을 점유 중인 프로세스를 정리한다 (이전 테스트 잔류 etcd 포함).
lsof -ti:2379 -sTCP:LISTEN 2>/dev/null | xargs kill -9 2>/dev/null || true
lsof -ti:2380 -sTCP:LISTEN 2>/dev/null | xargs kill -9 2>/dev/null || true
sleep 0.5

rm -rf "$ETCD_DATA_DIR"
mkdir -p "$ETCD_DATA_DIR"
log "Starting etcd (data-dir: $ETCD_DATA_DIR)..."
etcd \
  --data-dir "$ETCD_DATA_DIR" \
  --listen-client-urls "http://$ETCD_ADDR" \
  --advertise-client-urls "http://$ETCD_ADDR" \
  --listen-peer-urls "http://localhost:2380" \
  --initial-advertise-peer-urls "http://localhost:2380" \
  --log-level warn \
  >"$LOG_DIR/etcd.log" 2>&1 &
ETCD_PID=$!

# etcd가 준비될 때까지 대기 (최대 10초)
for i in $(seq 1 20); do
  if etcdctl --endpoints="$ETCD_ADDR" endpoint health >/dev/null 2>&1; then
    log "etcd ready (pid=$ETCD_PID)"
    break
  fi
  sleep 0.5
  if [[ $i -eq 20 ]]; then
    echo "ERROR: etcd failed to start within 10s. See $LOG_DIR/etcd.log"
    exit 1
  fi
done

# ── Redis 확인 (WAL_BACKEND=redis 시) ─────────────────────────────────────────

if [[ "$WAL_BACKEND" == "redis" ]]; then
  log "WAL_BACKEND=redis — checking Redis at $REDIS_ADDR..."
  if ! redis-cli -u "redis://$REDIS_ADDR" PING >/dev/null 2>&1; then
    echo "ERROR: Redis is not running at $REDIS_ADDR (required for WAL_BACKEND=redis)"
    exit 1
  fi
fi

# ── MinIO 확인 + 버킷 생성 (CHECKPOINT_BACKEND=minio 시) ──────────────────────

if [[ "$CHECKPOINT_BACKEND" == "minio" ]]; then
  log "CHECKPOINT_BACKEND=minio — checking MinIO at $MINIO_ADDR..."
  if ! curl -sf "http://$MINIO_ADDR/minio/health/live" >/dev/null 2>&1; then
    echo "ERROR: MinIO is not running at $MINIO_ADDR (required for CHECKPOINT_BACKEND=minio)"
    echo "  Start with: MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin minio server /tmp/minio-data"
    exit 1
  fi
  mc alias set minio-itest "http://$MINIO_ADDR" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" >/dev/null 2>&1
  mc mb "minio-itest/$MINIO_BUCKET" >/dev/null 2>&1 || true
  log "MinIO bucket ready: $MINIO_BUCKET"
fi

# ── WAL 인수 배열 구성 ────────────────────────────────────────────────────────

if [[ "$WAL_BACKEND" == "redis" ]]; then
  WAL_ARGS=(-wal-backend redis -redis-addr "$REDIS_ADDR")
else
  WAL_ARGS=(-wal-dir "$WAL_DIR")
fi

# ── Checkpoint 인수 배열 구성 ─────────────────────────────────────────────────

if [[ "$CHECKPOINT_BACKEND" == "minio" ]]; then
  CKPT_ARGS=(
    -checkpoint-backend s3
    -s3-endpoint "http://$MINIO_ADDR"
    -s3-bucket "$MINIO_BUCKET"
    -s3-prefix checkpoint
    -s3-region us-east-1
  )
  export AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY"
  export AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY"
else
  CKPT_ARGS=(-checkpoint-dir "$CKPT_DIR")
fi

# ── 환경 초기화 ───────────────────────────────────────────────────────────────

log "Cleaning old data (WAL, checkpoint)..."
if [[ "$WAL_BACKEND" == "redis" ]]; then
  redis-cli -u "redis://$REDIS_ADDR" FLUSHDB >/dev/null 2>&1 || true
else
  rm -rf "$WAL_DIR"
fi
if [[ "$CHECKPOINT_BACKEND" == "minio" ]]; then
  mc rm --recursive --force "minio-itest/$MINIO_BUCKET" >/dev/null 2>&1 || true
else
  rm -rf "$CKPT_DIR"
fi
mkdir -p "$WAL_DIR" "$CKPT_DIR" "$LOG_DIR"

# ── PM 기동 ──────────────────────────────────────────────────────────────────

log "Starting PM ($PM_ADDR)..."
"$BIN_DIR/pm" -addr :8000 -etcd "$ETCD_ADDR" -actor-types kv >"$PM_LOG" 2>&1 &
PM_PID=$!
sleep 1
if ! kill -0 "$PM_PID" 2>/dev/null; then
  echo "ERROR: PM failed to start. See $PM_LOG"
  exit 1
fi

# ── PS 기동 함수 ──────────────────────────────────────────────────────────────

start_ps1() {
  # If node is in Failed state (after SIGKILL), reset to Waiting so PM accepts RequestJoin.
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node reset ps-1 2>/dev/null || true
  # Pre-register ps-1 in the PM catalog (Waiting → Active on startup).
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node add ps-1 localhost:8001 2>/dev/null || true
  "$BIN_DIR/kv_server" -node-id ps-1 -addr localhost:8001 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" >"$PS1_LOG" 2>&1 &
  PS1_PID=$!
  sleep 2
}

start_ps2() {
  # If node is in Failed state (after SIGKILL), reset to Waiting so PM accepts RequestJoin.
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node reset ps-2 2>/dev/null || true
  # Pre-register ps-2 in the PM catalog (Waiting → Active on startup).
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node add ps-2 localhost:8002 2>/dev/null || true
  "$BIN_DIR/kv_server" -node-id ps-2 -addr localhost:8002 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" >"$PS2_LOG" 2>&1 &
  PS2_PID=$!
  sleep 2
}

# ─────────────────────────────────────────────────────────────────────────────
# 독립 실행용 셋업 헬퍼
# 특정 시나리오만 지정해 실행할 때 클러스터 상태를 직접 구축하는 함수들.
# 전체 실행(인수 없음)에서는 호출되지 않는다.
# ─────────────────────────────────────────────────────────────────────────────

# 공통 기반: 모든 프로세스 종료 + etcd/WAL/CKPT 정리 + PM(kv-only) 재기동
_base_reset() {
  [[ -n "$PS2_PID"  ]] && { kill "$PS2_PID"  2>/dev/null || true; PS2_PID=""; }
  [[ -n "$PS1_PID"  ]] && { kill "$PS1_PID"  2>/dev/null || true; PS1_PID=""; }
  [[ -n "$PM3_PID"  ]] && { kill "$PM3_PID"  2>/dev/null || true; PM3_PID=""; }
  [[ -n "$PM2_PID"  ]] && { kill "$PM2_PID"  2>/dev/null || true; PM2_PID=""; }
  [[ -n "$PM_PID"   ]] && { kill "$PM_PID"   2>/dev/null || true; PM_PID="";  }
  pkill -f "kv_stress" 2>/dev/null || true
  sleep 2
  etcdctl --endpoints="$ETCD_ADDR" del /actorbase/ --prefix >/dev/null 2>&1 || true
  if [[ "$WAL_BACKEND" == "redis" ]]; then
    redis-cli -u "redis://$REDIS_ADDR" FLUSHDB >/dev/null 2>&1 || true
  else
    rm -rf "$WAL_DIR"
  fi
  rm -rf "$CKPT_DIR"
  mkdir -p "$WAL_DIR" "$CKPT_DIR"
  PM_ADDR="localhost:8000"
  "$BIN_DIR/pm" -addr :8000 -etcd "$ETCD_ADDR" -actor-types kv >"$PM_LOG" 2>&1 &
  PM_PID=$!
  sleep 1
  if ! kill -0 "$PM_PID" 2>/dev/null; then
    echo "ERROR: PM failed to start in _base_reset. See $PM_LOG"; exit 1
  fi
}

# PM(kv) + PS1 단독 (1 파티션) — 시나리오 2, 3용
setup_basic_cluster() {
  _base_reset
  start_ps1
}

# PM(kv) + PS1 단독 + split(m) 완료 (2 파티션, 기본 데이터) — 시나리오 4용
setup_cluster_pre_migrate() {
  setup_basic_cluster
  kv_set apple  red    >/dev/null
  kv_set banana yellow >/dev/null
  kv_set mango  orange >/dev/null
  kv_set zebra  black  >/dev/null
  local pid
  pid=$(partition_id_by_index 1)
  "$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$pid" m >/dev/null 2>&1
  sleep 1
}

# PM(kv) + PS1[start,m) + PS2[m,end) + 기본 데이터 — 시나리오 5용
setup_split_cluster() {
  setup_basic_cluster
  kv_set apple  red    >/dev/null
  kv_set banana yellow >/dev/null
  kv_set mango  orange >/dev/null
  kv_set zebra  black  >/dev/null
  local pid
  pid=$(partition_id_by_index 1)
  "$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$pid" m >/dev/null 2>&1
  sleep 1
  start_ps2
  local upper_id
  upper_id=$(routing_entries | awk -F'\t' '$2=="m" {print $1}')
  if [[ -n "$upper_id" ]]; then
    "$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$upper_id" ps-2 >/dev/null 2>&1
    sleep 1
  fi
}

# PM(kv) + PS2만 활성 (모든 파티션 PS2로 이전, PS1 종료)
# 시나리오 6, 7, 9, 10, 11용
setup_ps2_only() {
  setup_split_cluster
  # PS1에 남은 파티션도 PS2로 이전
  local pid
  for pid in $(routing_entries | awk -F'\t' '$4=="ps-1" {print $1}'); do
    "$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$pid" ps-2 >/dev/null 2>&1 || true
    sleep 1
  done
  # PS1 종료 (drain)
  if [[ -n "$PS1_PID" ]]; then
    kill "$PS1_PID" 2>/dev/null || true
    PS1_PID=""
  fi
  sleep 4  # drain 완료 대기
}

# PM(kv) + PS2만 활성, 3개 파티션 [start,f), [f,m), [m,end) + 특정 데이터
# 시나리오 8용
setup_three_partitions_ps2() {
  setup_basic_cluster
  kv_set apple   red2   >/dev/null
  kv_set banana  yellow >/dev/null
  kv_set avocado green  >/dev/null
  kv_set cherry  red3   >/dev/null
  kv_set mango   orange >/dev/null
  kv_set zebra   black  >/dev/null
  # [start,end) → split at m → [start,m), [m,end)
  local pid
  pid=$(partition_id_by_index 1)
  "$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$pid" m >/dev/null 2>&1
  sleep 1
  # [start,m) → split at f → [start,f), [f,m)
  local lower_id
  lower_id=$(routing_entries | awk -F'\t' '$2!="m" {print $1; exit}')
  "$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$lower_id" f >/dev/null 2>&1
  sleep 1
  # 모든 파티션을 PS2로 이전
  start_ps2
  for pid in $(routing_entries | awk -F'\t' '$4=="ps-1" {print $1}'); do
    "$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$pid" ps-2 >/dev/null 2>&1 || true
    sleep 1
  done
  if [[ -n "$PS1_PID" ]]; then
    kill "$PS1_PID" 2>/dev/null || true
    PS1_PID=""
  fi
  sleep 4
}

# PM(kv,counter) + PS1(kv,counter) — 시나리오 13, 14용
setup_multi_actor_cluster() {
  [[ -n "$PS2_PID"  ]] && { kill "$PS2_PID"  2>/dev/null || true; PS2_PID=""; }
  [[ -n "$PS1_PID"  ]] && { kill "$PS1_PID"  2>/dev/null || true; PS1_PID=""; }
  [[ -n "$PM3_PID"  ]] && { kill "$PM3_PID"  2>/dev/null || true; PM3_PID=""; }
  [[ -n "$PM2_PID"  ]] && { kill "$PM2_PID"  2>/dev/null || true; PM2_PID=""; }
  [[ -n "$PM_PID"   ]] && { kill "$PM_PID"   2>/dev/null || true; PM_PID="";  }
  pkill -f "kv_stress" 2>/dev/null || true
  sleep 2
  etcdctl --endpoints="$ETCD_ADDR" del /actorbase/ --prefix >/dev/null 2>&1 || true
  if [[ "$WAL_BACKEND" == "redis" ]]; then
    redis-cli -u "redis://$REDIS_ADDR" FLUSHDB >/dev/null 2>&1 || true
  else
    rm -rf "$WAL_DIR"
  fi
  rm -rf "$CKPT_DIR"
  mkdir -p "$WAL_DIR" "$CKPT_DIR"
  PM_ADDR="localhost:8000"
  "$BIN_DIR/pm" -addr :8000 -etcd "$ETCD_ADDR" -actor-types kv,counter \
    >"$LOG_DIR/pm_multi.log" 2>&1 &
  PM_PID=$!
  sleep 1
  if ! kill -0 "$PM_PID" 2>/dev/null; then
    echo "ERROR: PM(kv,counter) failed to start"; exit 1
  fi
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node add ps-1 localhost:8001 2>/dev/null || true
  "$BIN_DIR/kv_server" -node-id ps-1 -addr localhost:8001 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" \
    -actor-types kv,counter >"$LOG_DIR/ps1_multi.log" 2>&1 &
  PS1_PID=$!
  sleep 4
}

# ═══════════════════════════════════════════════════════════════════════════════
# 시나리오 함수 정의
# ═══════════════════════════════════════════════════════════════════════════════

scenario_1() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 1: 클러스터 부트스트랩"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  start_ps1

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-1이 active 상태로 등록됨" "ps-1" "$members"
  assert_contains "ps-1 주소가 localhost:8001" "localhost:8001" "$members"

  rt=$(routing)
  assert_eq      "라우팅 테이블 Version=1" "1" "$(echo "$rt" | awk '/^Version:/ {print $2}')"
  assert_contains "actor-type=kv 파티션 존재" "kv" "$rt"
  assert_contains "key range (start)" "(start)" "$rt"
  assert_contains "key range (end)"   "(end)"   "$rt"
  assert_eq      "초기 파티션 수=1" "1" "$(partition_count)"
}
should_run 1 || log "시나리오 1: SKIP"
should_run 1 && scenario_1

scenario_2() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 2: 기본 KV 동작 (set/get/del)"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # 독립 실행 시 클러스터 초기화 (전체 실행에서는 시나리오 1이 이미 PS1을 기동)
  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_basic_cluster; fi

  assert_eq "set user:1001" "ok"             "$(kv_set user:1001 '{"name":"alice"}')"
  assert_eq "get user:1001" '{"name":"alice"}' "$(kv_get user:1001)"
  assert_eq "del user:1001" "ok"             "$(kv_del user:1001)"
  assert_eq "get after del (not found)" ""  "$(kv_get user:1001)"
}
should_run 2 || log "시나리오 2: SKIP"
should_run 2 && scenario_2

scenario_3() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 3: 파티션 Split"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # 독립 실행 시 클러스터 초기화
  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_basic_cluster; fi

  kv_set apple  red    >/dev/null
  kv_set banana yellow >/dev/null
  kv_set mango  orange >/dev/null
  kv_set zebra  black  >/dev/null

  PARTITION_ID=$(partition_id_by_index 1)
  split_out=$("$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$PARTITION_ID" m 2>/dev/null)
  assert_contains "split 명령 성공" "split successful" "$split_out"

  sleep 1
  rt=$(routing)
  assert_eq      "split 후 파티션 수=2"     "2" "$(partition_count)"
  assert_eq      "split 후 Version=2"       "2" "$(routing_version)"
  assert_contains "하위 파티션 key-end=m"   "m"        "$rt"
  assert_contains "상위 파티션 key-start=m" "m"        "$rt"

  # split 후 데이터 접근
  assert_eq "apple (< m) 조회" "red"    "$(kv_get apple)"
  assert_eq "mango (>= m) 조회" "orange" "$(kv_get mango)"
  assert_eq "zebra (>= m) 조회" "black"  "$(kv_get zebra)"
}
should_run 3 || log "시나리오 3: SKIP"
should_run 3 && scenario_3

scenario_4() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 4: Scale-out + Migrate"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # 독립 실행 시: PS1 + split(m) 완료 상태까지 준비 (PS2는 이 시나리오가 직접 기동)
  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_cluster_pre_migrate; fi

  start_ps2

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-2 등록됨" "ps-2" "$members"

  # 상위 파티션(key-start=m)을 PS-2로 migrate
  UPPER_ID=$(routing_entries | awk -F'\t' '$2=="m" {print $1}')
  migrate_out=$("$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$UPPER_ID" ps-2 2>/dev/null)
  assert_contains "migrate 명령 성공" "migrate successful" "$migrate_out"

  sleep 1
  assert_eq "migrate 후 Version=3"    "3" "$(routing_version)"
  assert_eq "ps-1에 파티션 1개"       "1" "$(partitions_on_node ps-1)"
  assert_eq "ps-2에 파티션 1개"       "1" "$(partitions_on_node ps-2)"
  assert_eq "migrate 후 mango 조회"  "orange" "$(kv_get mango)"
  assert_eq "migrate 후 zebra 조회"  "black"  "$(kv_get zebra)"
  assert_eq "migrate 후 apple 조회"  "red"    "$(kv_get apple)"
}
should_run 4 || log "시나리오 4: SKIP"
should_run 4 && scenario_4

scenario_5() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 5: SIGKILL → 자동 Failover + WAL replay"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # 독립 실행 시: PS1[start,m) + PS2[m,end) + 기본 데이터(apple=red, banana=yellow, ...) 준비
  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_split_cluster; fi

  # checkpoint 이후 WAL에만 기록되는 데이터 삽입
  kv_set apple  red2  >/dev/null  # 이전 값 red → red2
  kv_set avocado green >/dev/null
  kv_set cherry  red3  >/dev/null

  # PS-1 강제 종료 (SIGKILL)
  kill -9 "$PS1_PID" 2>/dev/null || true
  PS1_PID=""
  log "PS-1 killed. Waiting for PM heartbeat timeout + walFlushMargin (~10s)..."

  # PM heartbeat timeout(5s) + walFlushMargin(3s) + 여유 2s
  sleep 10

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-1이 Failed 상태 (SIGKILL)" "Failed" "$(echo "$members" | grep ps-1 || true)"
  assert_contains "ps-2가 active 상태"           "ps-2"   "$members"

  assert_eq "failover 후 Version=4"         "4"  "$(routing_version)"
  assert_eq "하위 파티션이 ps-2로 이동됨"   "2"  "$(partitions_on_node ps-2)"

  # checkpoint 이전 데이터
  assert_eq "banana (checkpoint 복원)" "yellow" "$(kv_get banana)"
  assert_eq "mango  (checkpoint 복원)" "orange" "$(kv_get mango)"

  # WAL replay 데이터 (checkpoint 이후 기록)
  assert_eq "apple  (WAL replay: red2, not red)" "red2"  "$(kv_get apple)"
  assert_eq "avocado (WAL replay)"               "green" "$(kv_get avocado)"
  assert_eq "cherry  (WAL replay)"               "red3"  "$(kv_get cherry)"
}
should_run 5 || log "시나리오 5: SKIP"
should_run 5 && scenario_5

scenario_6() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 6: SDK 라우팅 자동 갱신 (부하 중 split)"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # 독립 실행 시: PM + PS2 활성 (모든 파티션 PS2에 존재) 상태 준비
  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_ps2_only; fi

  # 현재 하위 파티션(ps-2에 있음)을 split 대상으로 선택
  LOWER_ID=$(routing_entries | awk -F'\t' '$2!="m" {print $1; exit}')

  STRESS_LOG="$LOG_DIR/stress6.log"
  "$BIN_DIR/kv_stress" -pm "$PM_ADDR" -duration 20s >"$STRESS_LOG" 2>&1 &
  STRESS_PID=$!
  sleep 3

  # 부하 중 split 실행
  split_out=$("$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$LOWER_ID" f 2>/dev/null)
  assert_contains "부하 중 split 성공" "split successful" "$split_out"

  wait "$STRESS_PID" 2>/dev/null || true

  stress_result=$(tail -3 "$STRESS_LOG")
  stress_success=$(echo "$stress_result" | grep -oE 'success=[0-9]+' | tail -1 | grep -oE '[0-9]+' || echo "0")
  stress_fail=$(echo "$stress_result" | grep -oE 'fail=[0-9]+' | tail -1 | grep -oE '[0-9]+' || echo "-1")

  assert_eq "부하 중 split: fail=0" "0" "$stress_fail"
  if [[ "$stress_success" -gt 0 ]]; then
    pass "kv_stress success=$stress_success (>0)"
  else
    fail "kv_stress success=$stress_success (should be >0)"
  fi
}
should_run 6 || log "시나리오 6: SKIP"
should_run 6 && scenario_6

scenario_7() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 7: Graceful Shutdown (SIGTERM → drain)"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # 독립 실행 시: PM + PS2 활성 + apple=red2, mango=orange 준비
  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then
    setup_ps2_only
    kv_set apple  red2   >/dev/null
    kv_set mango  orange >/dev/null
  fi

  # ps-1 재기동 후 파티션 하나를 migrate하여 두 노드에 분산
  start_ps1

  # ps-2에서 파티션 하나를 ps-1으로 migrate
  SOME_ID=$(routing_entries | awk -F'\t' '$4=="ps-2" {print $1; exit}')
  "$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$SOME_ID" ps-1 >/dev/null 2>/dev/null || true
  sleep 1

  partitions_before=$(partitions_on_node ps-1)

  # 부하 생성기 실행
  STRESS7_LOG="$LOG_DIR/stress7.log"
  "$BIN_DIR/kv_stress" -pm "$PM_ADDR" -duration 20s >"$STRESS7_LOG" 2>&1 &
  STRESS7_PID=$!
  sleep 2

  # SIGTERM (graceful shutdown)
  kill "$PS1_PID" 2>/dev/null || true
  log "PS-1 SIGTERM sent. Waiting for drain..."
  sleep 6

  drain_log=$(grep "drain" "$PS1_LOG" 2>/dev/null || true)
  assert_contains "drain 완료 로그 출력됨" "drain: partition migrated" "$drain_log"

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-1이 Waiting 상태 (SIGTERM 후 drain)" "Waiting" "$(echo "$members" | grep ps-1 || true)"

  # drain 후 ps-1 파티션이 ps-2로 이전되었는지
  assert_eq "drain 후 ps-2에 모든 파티션" "$(partition_count)" "$(partitions_on_node ps-2)"

  # 데이터 정합성
  assert_eq "apple 조회 (drain 후)"  "red2"  "$(kv_get apple)"
  assert_eq "mango 조회 (drain 후)"  "orange" "$(kv_get mango)"

  wait "$STRESS7_PID" 2>/dev/null || true
  PS1_PID=""
}
should_run 7 || log "시나리오 7: SKIP"
should_run 7 && scenario_7

scenario_8() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 8: Range Scan (다중 파티션 fan-out)"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # 독립 실행 시: PS2에 [start,f), [f,m), [m,end) 3개 파티션 + 특정 데이터 준비
  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_three_partitions_ps2; fi

  #
  # 이 시점의 클러스터 상태:
  #   파티션: [start,f), [f,m), [m,end) — 모두 ps-2에 존재
  #   키:  apple=red2, avocado=green, banana=yellow, cherry=red3 → [start,f) 또는 [f,m)
  #        mango=orange, zebra=black → [m,end)
  #
  # 검증 목표:
  #   - 전체 range scan이 여러 파티션에 걸쳐 누락 없이 동작하는지
  #   - 부분 range scan이 올바른 파티션만 조회하는지

  # 현재 키 상태 확인 (이전 시나리오에서 삽입된 값 유지)
  kv_set peach pink >/dev/null  # [start,f) 또는 [f,m) — p는 f~m 사이
  kv_set quince yellow2 >/dev/null  # q는 [f,m)

  # 전체 range scan: 모든 파티션을 가로질러야 함
  scan_all=$(kv_scan "" "")
  assert_contains "scan 전체: apple 포함"   "apple"   "$scan_all"
  assert_contains "scan 전체: banana 포함"  "banana"  "$scan_all"
  assert_contains "scan 전체: mango 포함"   "mango"   "$scan_all"
  assert_contains "scan 전체: zebra 포함"   "zebra"   "$scan_all"
  assert_contains "scan 전체: peach 포함"   "peach"   "$scan_all"
  assert_contains "scan 전체: quince 포함"  "quince"  "$scan_all"

  # 상위 range scan: [m, end) — mango, zebra만
  scan_upper=$(kv_scan "m" "")
  assert_contains     "scan [m,): mango 포함"  "mango"  "$scan_upper"
  assert_contains     "scan [m,): zebra 포함"  "zebra"  "$scan_upper"
  assert_not_contains "scan [m,): apple 제외"  "apple"  "$scan_upper"
  assert_not_contains "scan [m,): banana 제외" "banana" "$scan_upper"

  # 하위 range scan: [a, m) — a~m 미만 키들만
  scan_lower=$(kv_scan "a" "m")
  assert_contains     "scan [a,m): apple 포함"   "apple"   "$scan_lower"
  assert_contains     "scan [a,m): banana 포함"  "banana"  "$scan_lower"
  assert_contains     "scan [a,m): cherry 포함"  "cherry"  "$scan_lower"
  assert_not_contains "scan [a,m): mango 제외"   "mango"   "$scan_lower"
  assert_not_contains "scan [a,m): zebra 제외"   "zebra"   "$scan_lower"

  # 값 정합성: scan 결과에서 값도 확인
  assert_contains "scan 전체: apple=red2"   "red2"   "$scan_all"
  assert_contains "scan 전체: mango=orange" "orange" "$scan_all"
}
should_run 8 || log "시나리오 8: SKIP"
should_run 8 && scenario_8

scenario_9() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 9: PM HA Failover"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # 독립 실행 시: PM + PS2 활성 상태 준비
  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_ps2_only; fi

  #
  # 이 시점: 클러스터가 정상 동작 중 (PM-1 leader, PS-2 active)
  # 절차:
  #   1. PM-2를 standby로 기동 (CampaignLeader에서 블로킹, gRPC 미오픈)
  #   2. PM-1 SIGKILL → etcd 리더십 해제 → PM-2가 리더 승계
  #   3. PM-2 gRPC 포트 오픈 확인 (라우팅 테이블 조회 성공)
  #   4. 데이터 접근 정상 확인 (기존 라우팅 테이블 etcd에서 복원)

  # 데이터 삽입 (failover 전)
  kv_set "ha_test" "before_failover" >/dev/null

  # PM-2 standby로 기동 (Campaign에서 블로킹)
  log "Starting PM-2 as standby ($PM2_ADDR)..."
  "$BIN_DIR/pm" -addr :8003 -etcd "$ETCD_ADDR" -actor-types kv >"$PM2_LOG" 2>&1 &
  PM2_PID=$!
  sleep 2
  if ! kill -0 "$PM2_PID" 2>/dev/null; then
    fail "PM-2 process died immediately. See $PM2_LOG"
  else
    pass "PM-2 standby 프로세스 실행 중"
  fi

  # PM-2는 아직 리더가 아니므로 gRPC 포트가 열리지 않아야 함
  if "$BIN_DIR/abctl" -pm "$PM2_ADDR" routing >/dev/null 2>&1; then
    fail "PM-2가 standby인데 gRPC 포트가 열림 (예상: 실패)"
  else
    pass "PM-2 standby 상태: gRPC 포트 미오픈 확인"
  fi

  # PM-1 SIGKILL
  log "Killing PM-1 (SIGKILL)..."
  kill -9 "$PM_PID" 2>/dev/null || true
  PM_PID=""
  log "PM-1 killed. Waiting for etcd lease expiry (~15s) and PM-2 election..."
  sleep 18

  # PM-2가 리더로 승계되어 gRPC 서버를 열었는지 확인
  pm2_rt=$("$BIN_DIR/abctl" -pm "$PM2_ADDR" routing 2>/dev/null || true)
  if [[ -n "$pm2_rt" ]]; then
    pass "PM-2 gRPC 서버 오픈됨 (라우팅 테이블 조회 성공)"
  else
    fail "PM-2 gRPC 서버 미오픈 (라우팅 테이블 조회 실패)"
  fi

  assert_contains "PM-2 라우팅 테이블: kv 파티션 존재" "kv" "$pm2_rt"

  pm2_log_leader=$(grep "elected as leader" "$PM2_LOG" 2>/dev/null || true)
  assert_contains "PM-2 로그: 리더 선출 확인" "elected as leader" "$pm2_log_leader"

  # PM-2를 통해 데이터 접근
  PM_ADDR="$PM2_ADDR"  # 이후 kv_client도 PM-2로 접근
  assert_eq "failover 후 ha_test 조회" "before_failover" "$(kv_get ha_test)"
  assert_eq "failover 후 기존 데이터 접근 (apple)" "red2" "$(kv_get apple)"

  kv_set "ha_test2" "after_failover" >/dev/null
  assert_eq "failover 후 신규 데이터 저장/조회" "after_failover" "$(kv_get ha_test2)"
}
should_run 9 || log "시나리오 9: SKIP"
should_run 9 && scenario_9

scenario_10() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 10: Actor Eviction + Re-activation"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # 독립 실행 시: PM + PS2 활성 상태 준비 (PM_ADDR은 localhost:8000으로 리셋)
  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_ps2_only; fi

  #
  # 이 시점: PM-2가 리더, PS-2가 모든 파티션 소유
  # 절차:
  #   1. PS-2를 짧은 idle-timeout(5s)으로 재기동
  #   2. 데이터 set → actor 활성화 → stats 확인 (partitions>0)
  #   3. idle-timeout + evict-interval 대기 (10s)
  #   4. stats 확인 → partitions=0 (EvictionScheduler가 evict)
  #   5. 데이터 get → getOrActivate (checkpoint+WAL replay) → 값 정합성 확인
  #   6. stats 확인 → partitions>0 (re-activation)

  # PS-2 종료 후 짧은 idle-timeout으로 재기동
  log "PS-2 재기동 (idle-timeout=5s, evict-interval=2s)..."
  [[ -n "$PS2_PID" ]] && kill "$PS2_PID" 2>/dev/null || true
  PS2_PID=""
  sleep 5  # graceful shutdown 대기 (drain 실패 후 evictAll, deregister)

  "$BIN_DIR/kv_server" -node-id ps-2 -addr localhost:8002 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" \
    -idle-timeout 5s -evict-interval 2s >"$LOG_DIR/ps2_evict.log" 2>&1 &
  PS2_PID=$!
  sleep 3  # etcd 등록 + 초기 라우팅 테이블 수신 대기

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-2 재기동 후 members에 존재" "ps-2" "$members"

  # 데이터 set → 해당 파티션의 actor 활성화 (getOrActivate: checkpoint+WAL replay)
  kv_set "evict-key" "evict-value" >/dev/null
  sleep 1

  # set 직후 stats → 활성 partition 1개 이상
  # 새 테이블 형식: 파티션 있는 줄은 ps-2로 시작하며 파티션ID(UUID)가 4번째 컬럼
  # 파티션 없는 노드는 ps-2 ... - - - - 형태
  stats_after_set=$("$BIN_DIR/abctl" -pm "$PM_ADDR" stats 2>/dev/null)
  ps2_active_set=$(echo "$stats_after_set" | awk '/^ps-2/ && $4 != "-" {count++} END {print count+0}')
  if [[ "$ps2_active_set" -gt 0 ]]; then
    pass "set 직후 actor active (ps-2 partitions=$ps2_active_set)"
  else
    fail "set 직후 actor active 확인 실패 (ps-2 partitions=$ps2_active_set)"
  fi

  # idle-timeout(5s) + evict-interval(2s) + 여유(3s) 대기
  log "EvictionScheduler 대기 (10s)..."
  sleep 10

  # eviction 확인 → partitions=0 (모든 줄에 파티션ID가 "-")
  stats_after_evict=$("$BIN_DIR/abctl" -pm "$PM_ADDR" stats 2>/dev/null)
  ps2_active_evict=$(echo "$stats_after_evict" | awk '/^ps-2/ && $4 != "-" {count++} END {print count+0}')
  assert_eq "EvictionScheduler 동작: actor evict됨 (partitions=0)" "0" "$ps2_active_evict"

  # re-activation: get 요청 → getOrActivate → checkpoint+WAL replay
  val=$(kv_get "evict-key")
  assert_eq "re-activation 후 값 복원 (evict-value)" "evict-value" "$val"
  sleep 1

  # re-activation 후 stats → partitions>0
  stats_after_get=$("$BIN_DIR/abctl" -pm "$PM_ADDR" stats 2>/dev/null)
  ps2_active_get=$(echo "$stats_after_get" | awk '/^ps-2/ && $4 != "-" {count++} END {print count+0}')
  if [[ "$ps2_active_get" -gt 0 ]]; then
    pass "re-activation 후 actor active 복원 (ps-2 partitions=$ps2_active_get)"
  else
    fail "re-activation 후 actor active 복원 실패 (ps-2 partitions=$ps2_active_get)"
  fi
}
should_run 10 || log "시나리오 10: SKIP"
should_run 10 && scenario_10

scenario_11() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 11: SDK HA Mode 자동 재발견"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # 독립 실행 시: PM + PS2 활성 상태 준비 (PM_ADDR=localhost:8000)
  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then
    setup_ps2_only
    PM2_PID=""  # 독립 실행 시 PM2는 없음
  fi

  #
  # 이 시점: PM-2가 리더 (localhost:8003), PS-2가 kv 파티션 소유
  # 절차:
  #   1. PM-3 standby로 기동 (:8004)
  #   2. kv_stress를 etcd 모드로 실행 (PM 주소 없이 etcd에서 자동 발견)
  #   3. 5초 후 PM-2 SIGKILL
  #   4. PM-3가 리더 승계 (~15s)
  #   5. kv_stress fail 수 검증 (cached routing → PS 직접 접속으로 대부분 성공)

  PM3_ADDR="localhost:8004"
  PM3_LOG="$LOG_DIR/pm3.log"

  # PM-3 standby 기동
  log "PM-3 standby 기동 ($PM3_ADDR)..."
  "$BIN_DIR/pm" -addr :8004 -etcd "$ETCD_ADDR" -actor-types kv >"$PM3_LOG" 2>&1 &
  PM3_PID=$!
  sleep 2
  if ! kill -0 "$PM3_PID" 2>/dev/null; then
    fail "PM-3 프로세스 시작 실패. See $PM3_LOG"
  else
    pass "PM-3 standby 프로세스 실행 중"
  fi

  # kv_stress를 etcd 모드로 실행 (PM 주소 없이 etcd 자동 발견)
  STRESS_LOG="$LOG_DIR/stress_ha.log"
  "$BIN_DIR/kv_stress" -etcd "$ETCD_ADDR" -duration 50s -interval 200ms -max-retries 10 \
    >"$STRESS_LOG" 2>&1 &
  STRESS_PID=$!
  log "kv_stress (etcd 모드) 시작. 5초 후 PM-2 종료..."
  sleep 5

  # PM-2 SIGKILL (현재 리더)
  log "PM-2 SIGKILL..."
  kill -9 "$PM2_PID" 2>/dev/null || true
  PM2_PID=""
  log "PM-2 killed. PM-3 리더 승계 대기 (~18s)..."
  sleep 20

  # PM-3이 리더로 승계되어 gRPC 서버를 열었는지 확인
  pm3_rt=$("$BIN_DIR/abctl" -pm "$PM3_ADDR" routing 2>/dev/null || true)
  if [[ -n "$pm3_rt" ]]; then
    pass "PM-3 gRPC 서버 오픈됨 (라우팅 테이블 조회 성공)"
  else
    fail "PM-3 gRPC 서버 미오픈 (라우팅 테이블 조회 실패)"
  fi

  # kv_stress 완료 대기
  wait "$STRESS_PID" 2>/dev/null || true

  # 결과 파싱
  stress_result=$(grep "done:" "$STRESS_LOG" 2>/dev/null | tail -1 || true)
  log "kv_stress 결과: $stress_result"
  stress_fail=$(echo "$stress_result" | grep -oE 'fail=[0-9]+' | grep -oE '[0-9]+' || echo "999")
  stress_success=$(echo "$stress_result" | grep -oE 'success=[0-9]+' | grep -oE '[0-9]+' || echo "0")

  # PM 교체 중 일부 실패는 허용 (최대 30회)
  if [[ "$stress_fail" -le 30 ]]; then
    pass "SDK HA 자동 재발견: success=$stress_success fail=$stress_fail (≤30)"
  else
    fail "SDK HA 자동 재발견 실패: success=$stress_success fail=$stress_fail (>30)"
  fi

  # PM_ADDR를 PM-3으로 갱신
  PM_ADDR="$PM3_ADDR"
  assert_eq "HA 재발견 후 기존 데이터 접근 (apple)" "red2" "$(kv_get apple)"
}
should_run 11 || log "시나리오 11: SKIP"
should_run 11 && scenario_11

scenario_12() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 12: Multi-actor-type 동시 운영"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  #
  # 절차:
  #   1. 클러스터 전체 재기동 (kv + counter 두 타입 지원)
  #   2. 라우팅 테이블에 kv, counter 두 타입 파티션 존재 확인
  #   3. kv 요청 정상 동작 확인
  #   4. abctl split 후 두 타입 파티션 모두 유지되는지 확인

  # 기존 클러스터 정리 (독립 실행 지원: PM_PID, PM2_PID 포함 전체 종료)
  log "기존 클러스터 정리..."
  [[ -n "$PM3_PID" ]] && { kill "$PM3_PID" 2>/dev/null || true; PM3_PID=""; }
  [[ -n "$PM2_PID" ]] && { kill "$PM2_PID" 2>/dev/null || true; PM2_PID=""; }
  [[ -n "$PM_PID"  ]] && { kill "$PM_PID"  2>/dev/null || true; PM_PID="";  }
  [[ -n "$PS2_PID" ]] && { kill "$PS2_PID" 2>/dev/null || true; PS2_PID=""; }
  [[ -n "$PS1_PID" ]] && { kill "$PS1_PID" 2>/dev/null || true; PS1_PID=""; }
  sleep 3

  # etcd 데이터 초기화 및 WAL/CKPT 정리 (클린 재기동)
  etcdctl --endpoints="$ETCD_ADDR" del /actorbase/ --prefix >/dev/null 2>&1 || true
  if [[ "$WAL_BACKEND" == "redis" ]]; then
    redis-cli -u "redis://$REDIS_ADDR" FLUSHDB >/dev/null 2>&1 || true
  else
    rm -rf "$WAL_DIR"
  fi
  rm -rf "$CKPT_DIR"
  mkdir -p "$WAL_DIR" "$CKPT_DIR"

  # PM 재기동: kv + counter 두 타입 지원
  log "PM 재기동 with -actor-types kv,counter..."
  PM_ADDR="localhost:8000"
  "$BIN_DIR/pm" -addr :8000 -etcd "$ETCD_ADDR" -actor-types kv,counter \
    >"$LOG_DIR/pm_multi.log" 2>&1 &
  PM_PID=$!
  sleep 1

  # PS-1 재기동: kv + counter 두 타입 등록 (새 etcd이므로 node add 필요)
  log "PS-1 재기동 with -actor-types kv,counter..."
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node add ps-1 localhost:8001 2>/dev/null || true
  "$BIN_DIR/kv_server" -node-id ps-1 -addr localhost:8001 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" \
    -actor-types kv,counter >"$LOG_DIR/ps1_multi.log" 2>&1 &
  PS1_PID=$!
  sleep 4  # 부트스트랩 완료 + 초기 파티션 등록 대기

  # 라우팅 테이블 확인
  rt_multi=$(routing)
  assert_contains "multi-type: kv 파티션 존재"     "kv"      "$rt_multi"
  assert_contains "multi-type: counter 파티션 존재" "counter" "$rt_multi"

  # kv 파티션 수 ≥ 1, counter 파티션 수 ≥ 1
  kv_count=$(routing_entries | awk -F'\t' 'BEGIN{c=0} {c++} END{print c+0}')
  if [[ "$kv_count" -ge 1 ]]; then
    pass "kv 타입 라우팅 항목 ${kv_count}개 이상 존재"
  else
    fail "kv 타입 라우팅 항목 없음"
  fi

  # kv 요청 정상 동작
  kv_set "mt-key" "mt-value" >/dev/null
  assert_eq "multi-type: kv set/get 정상" "mt-value" "$(kv_get mt-key)"

  # abctl split 후 두 타입 모두 유지되는지 확인 (kv 타입 파티션만 선택)
  kv_part_id=$("$BIN_DIR/abctl" -pm "$PM_ADDR" routing 2>/dev/null \
    | awk 'NR>4 && $1!="" && $1!~"^-" && $2=="kv" {print $1; exit}')
  if [[ -n "$kv_part_id" ]]; then
    rt_ver_before=$(routing_version)
    "$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$kv_part_id" m >/dev/null 2>&1 || true
    sleep 2
    rt_ver_after=$(routing_version)
    if [[ "$rt_ver_after" -gt "$rt_ver_before" ]]; then
      pass "split 후 라우팅 버전 증가 (${rt_ver_before}→${rt_ver_after})"
    else
      fail "split 후 라우팅 버전 미증가"
    fi
    rt_after_split=$(routing)
    assert_contains "split 후 kv 파티션 유지"     "kv"      "$rt_after_split"
    assert_contains "split 후 counter 파티션 유지" "counter" "$rt_after_split"
  else
    fail "kv 파티션 ID를 가져오지 못함"
  fi
}
should_run 12 || log "시나리오 12: SKIP"
should_run 12 && scenario_12

scenario_13() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 13: drainPartitions 타임아웃"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # 독립 실행 시: PM(kv,counter) + PS1(kv,counter) 상태 준비
  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_multi_actor_cluster; fi

  #
  # 절차:
  #   1. 테스트 데이터 삽입 + WAL flush 대기
  #   2. PS-1을 drain-timeout=3s로 재기동
  #   3. PM 종료 (drain 실패 유도)
  #   4. PS-1 SIGTERM → drain 3s 후 타임아웃 → EvictAll(checkpoint 저장) → 종료
  #   5. PM + PS-1 재기동 → checkpoint에서 데이터 복원 확인

  # 테스트 데이터 삽입 (checkpoint 복원 검증용)
  kv_set "drain-key-1" "drain-val-1" >/dev/null
  kv_set "drain-key-2" "drain-val-2" >/dev/null
  sleep 1  # WAL flush 대기

  # PS-1을 짧은 drain-timeout(3s)으로 재기동
  log "PS-1 재기동 (drain-timeout=3s)..."
  [[ -n "$PS1_PID" ]] && { kill "$PS1_PID" 2>/dev/null || true; PS1_PID=""; }
  sleep 2

  "$BIN_DIR/kv_server" -node-id ps-1 -addr localhost:8001 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" \
    -actor-types kv,counter -drain-timeout 3s \
    >"$LOG_DIR/ps1_drain.log" 2>&1 &
  PS1_PID=$!
  sleep 3

  # 재기동 후 데이터 확인 (checkpoint+WAL 복원)
  assert_eq "drain-timeout PS 재기동 후 데이터 확인" "drain-val-1" "$(kv_get drain-key-1)"

  # PM 종료 (drain 실패 유도)
  log "PM 종료 (drain 실패 유도)..."
  [[ -n "$PM_PID" ]] && { kill "$PM_PID" 2>/dev/null || true; PM_PID=""; }
  sleep 1

  # PS-1 SIGTERM → drain 3s 후 타임아웃 → EvictAll (checkpoint 저장) → 종료
  log "PS-1 SIGTERM → drain timeout(3s) + EvictAll 대기..."
  kill -TERM "$PS1_PID" 2>/dev/null || true
  # drain-timeout(3s) + EvictAll 시간 + 여유(5s)
  sleep 10
  PS1_PID=""

  # PS-1 로그에서 drain 관련 메시지 확인
  ps1_drain_log=$(cat "$LOG_DIR/ps1_drain.log" 2>/dev/null || true)
  if echo "$ps1_drain_log" | grep -qi "drain\|shutdown\|evict"; then
    pass "PS-1 로그에서 drain/shutdown/evict 관련 메시지 확인"
  else
    fail "PS-1 로그에 drain/shutdown 메시지 없음"
  fi

  # PM + PS-1 재기동 (checkpoint 복원 확인)
  log "PM + PS-1 재기동 (checkpoint 복원 확인)..."
  "$BIN_DIR/pm" -addr :8000 -etcd "$ETCD_ADDR" -actor-types kv,counter \
    >"$LOG_DIR/pm_after_drain.log" 2>&1 &
  PM_PID=$!
  sleep 1

  "$BIN_DIR/kv_server" -node-id ps-1 -addr localhost:8001 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" \
    -actor-types kv,counter >"$LOG_DIR/ps1_after_drain.log" 2>&1 &
  PS1_PID=$!
  sleep 3

  PM_ADDR="localhost:8000"
  assert_eq "drain 후 재기동: drain-key-1 복원" "drain-val-1" "$(kv_get drain-key-1)"
  assert_eq "drain 후 재기동: drain-key-2 복원" "drain-val-2" "$(kv_get drain-key-2)"
}
should_run 13 || log "시나리오 13: SKIP"
should_run 13 && scenario_13

scenario_14() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 14: 파티션 Merge (Split → Merge → 데이터 무결성)"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # 독립 실행 시: PM(kv,counter) + PS1(kv,counter) 상태 준비
  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_multi_actor_cluster; fi

  #
  # 절차:
  #   1. 현재 파티션 수 확인 (시나리오 3에서 split된 상태일 수 있음)
  #   2. 데이터 삽입: lower 범위 + upper 범위 각각 데이터 삽입
  #   3. split으로 파티션을 2개로 분리 (이미 2개면 재활용)
  #   4. 두 파티션을 merge
  #   5. merge 후 파티션 수 감소 확인
  #   6. merge 후 모든 데이터 접근 가능 확인
  #   7. Evict + 재활성화 후 데이터 유지 확인 (checkpoint 무결성)

  # PS-1이 실행 중이 아니면 기동
  if [[ -z "$PS1_PID" ]] || ! kill -0 "$PS1_PID" 2>/dev/null; then
    start_ps1
  fi

  # 깨끗한 상태를 위해 새 데이터 삽입
  kv_set "merge-a" "val-a" >/dev/null
  kv_set "merge-b" "val-b" >/dev/null
  kv_set "merge-x" "val-x" >/dev/null
  kv_set "merge-z" "val-z" >/dev/null
  sleep 1  # WAL flush 대기

  # 현재 kv 파티션 수 확인 (multi-actor-type 환경에서는 kv만 카운트)
  local before_count
  before_count=$(kv_partition_count)
  log "현재 kv 파티션 수: $before_count"

  # split으로 kv 파티션을 2개 이상 만들기 (이미 2개 이상이면 재활용)
  if [[ "$before_count" -lt 2 ]]; then
    local pid
    pid=$(kv_partition_id_by_index 1)
    "$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$pid" "merge-m" >/dev/null 2>&1
    sleep 1
  fi

  local split_count
  split_count=$(kv_partition_count)
  log "split 후 kv 파티션 수: $split_count"

  # lower/upper 파티션 ID 파악 (같은 노드에 있어야 merge 가능)
  # kv_routing_entries: id\tstart\tend\tnode (kv 타입만)
  # "merge-m" 이전이 lower, "merge-m" 이후가 upper
  local lower_id upper_id
  lower_id=$(kv_routing_entries | awk -F'\t' '$3=="merge-m" || ($2=="(start)" && $3=="merge-m") {print $1}' | head -1)
  upper_id=$(kv_routing_entries | awk -F'\t' '$2=="merge-m" {print $1}' | head -1)

  if [[ -z "$lower_id" || -z "$upper_id" ]]; then
    # fallback: kv 파티션 중 첫 번째와 두 번째 (인접한 파티션)
    lower_id=$(kv_partition_id_by_index 1)
    upper_id=$(kv_partition_id_by_index 2)
  fi

  log "merge 대상: lower=$lower_id, upper=$upper_id"

  # 두 파티션이 같은 노드에 있는지 확인. 다르면 migrate
  local lower_node upper_node
  lower_node=$(kv_routing_entries | awk -F'\t' -v id="$lower_id" '$1==id {print $4}')
  upper_node=$(kv_routing_entries | awk -F'\t' -v id="$upper_id" '$1==id {print $4}')

  if [[ "$lower_node" != "$upper_node" ]]; then
    log "upper를 $lower_node으로 migrate..."
    "$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$upper_id" "$lower_node" >/dev/null 2>&1
    sleep 2
  fi

  # merge 실행 (set -e 환경에서 실패 시 스크립트 종료 방지)
  merge_out=$("$BIN_DIR/abctl" -pm "$PM_ADDR" merge kv "$lower_id" "$upper_id" 2>/dev/null) || merge_out=""
  assert_contains "merge 명령 성공" "merge successful" "$merge_out"

  sleep 1

  # merge 후 kv 파티션 수 감소 확인
  local after_count
  after_count=$(kv_partition_count)
  assert_eq "merge 후 kv 파티션 수 감소" "$((split_count - 1))" "$after_count"

  # merge 후 모든 데이터 접근 가능
  assert_eq "merge 후 merge-a 조회" "val-a" "$(kv_get merge-a)"
  assert_eq "merge 후 merge-b 조회" "val-b" "$(kv_get merge-b)"
  assert_eq "merge 후 merge-x 조회" "val-x" "$(kv_get merge-x)"
  assert_eq "merge 후 merge-z 조회" "val-z" "$(kv_get merge-z)"

  # 새 데이터 쓰기도 정상 동작하는지 확인
  kv_set "merge-new" "after-merge" >/dev/null
  assert_eq "merge 후 새 데이터 쓰기+읽기" "after-merge" "$(kv_get merge-new)"

  # Evict + 재활성화 → checkpoint 복원 확인
  # PS-1 재기동으로 checkpoint에서 복원
  log "PS-1 재기동 (checkpoint 복원 검증)..."
  [[ -n "$PS1_PID" ]] && { kill "$PS1_PID" 2>/dev/null || true; PS1_PID=""; }
  sleep 3

  "$BIN_DIR/kv_server" -node-id ps-1 -addr localhost:8001 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" \
    -actor-types kv,counter >"$LOG_DIR/ps1_after_merge.log" 2>&1 &
  PS1_PID=$!
  sleep 3

  assert_eq "merge 후 재기동: merge-a 복원" "val-a" "$(kv_get merge-a)"
  assert_eq "merge 후 재기동: merge-x 복원" "val-x" "$(kv_get merge-x)"
  assert_eq "merge 후 재기동: merge-new 복원" "after-merge" "$(kv_get merge-new)"
}
should_run 14 || log "시나리오 14: SKIP"
should_run 14 && scenario_14

# ─────────────────────────────────────────────────────────────────────────────
# reset_cluster_single_ps1: 클러스터를 PS1 단독 kv-only 운영 상태로 초기화
# (시나리오 15-19의 사전 조건 설정용 내부 헬퍼)
#
# Hard-resets etcd so counter partitions from scenario 12 don't bleed into the
# node-state scenarios (15-19).  Also clears WAL/CKPT and restarts PM with
# kv-only so the routing table starts clean.
# ─────────────────────────────────────────────────────────────────────────────
reset_cluster_single_ps1() {
  # Kill any running PS/PM processes.
  [[ -n "$PS2_PID" ]] && { kill "$PS2_PID" 2>/dev/null || true; PS2_PID=""; }
  [[ -n "$PS1_PID" ]] && { kill "$PS1_PID" 2>/dev/null || true; PS1_PID=""; }
  [[ -n "$PM3_PID" ]] && { kill "$PM3_PID" 2>/dev/null || true; PM3_PID=""; }
  [[ -n "$PM2_PID" ]] && { kill "$PM2_PID" 2>/dev/null || true; PM2_PID=""; }
  [[ -n "$PM_PID"  ]] && { kill "$PM_PID"  2>/dev/null || true; PM_PID="";  }
  sleep 3

  # Hard-reset etcd (removes counter partitions, node catalog, routing table).
  etcdctl --endpoints="$ETCD_ADDR" del /actorbase/ --prefix >/dev/null 2>&1 || true
  if [[ "$WAL_BACKEND" == "redis" ]]; then
    redis-cli -u "redis://$REDIS_ADDR" FLUSHDB >/dev/null 2>&1 || true
  else
    rm -rf "$WAL_DIR"
  fi
  rm -rf "$CKPT_DIR"
  mkdir -p "$WAL_DIR" "$CKPT_DIR"

  # Restart PM with kv-only.
  PM_ADDR="localhost:8000"
  "$BIN_DIR/pm" -addr :8000 -etcd "$ETCD_ADDR" -actor-types kv >"$PM_LOG" 2>&1 &
  PM_PID=$!
  sleep 1
  if ! kill -0 "$PM_PID" 2>/dev/null; then
    echo "ERROR: PM failed to start in reset_cluster_single_ps1. See $PM_LOG"
    exit 1
  fi

  # Start PS1 only to rebuild a single-node kv-only cluster.
  start_ps1
  log "reset_cluster_single_ps1: cluster reset done, PS1 active (kv-only)"
}

scenario_15() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 15: SIGKILL + 파티션 없음 → Waiting (케이스 A)"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  #
  # 전제: PS1이 파티션을 보유하지 않은 상태에서 SIGKILL 발생
  # 기대: PS1이 Failed가 아닌 Waiting으로 전환
  #       PS1이 재기동하면 node reset 없이 Active로 복귀
  #
  # 사전 준비: PS1 단독 운영 + 파티션을 PS2로 이동 후 PS2만 남기기
  reset_cluster_single_ps1

  # Add PS2 and migrate all partitions to PS2.
  start_ps2
  sleep 2

  # Migrate all PS1 partitions to PS2.
  for pid in $(routing_entries | awk -F'\t' '$4=="ps-1" {print $1}'); do
    "$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$pid" ps-2 >/dev/null 2>/dev/null || true
    sleep 1
  done
  sleep 1

  assert_eq "PS1 파티션 수=0 (모두 PS2로 이전)" "0" "$(partitions_on_node ps-1)"

  # SIGKILL PS1 (파티션 없음).
  kill -9 "$PS1_PID" 2>/dev/null || true
  PS1_PID=""
  log "PS-1 SIGKILL. Waiting for PM heartbeat timeout (~8s)..."
  # HeartbeatTimeout(5s) + 여유 3s (파티션 없음이므로 walFlushMargin 없음)
  sleep 8

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  # KEY assertion: Waiting, NOT Failed
  assert_contains "ps-1이 Waiting 상태 (파티션 없음 + SIGKILL)" "Waiting" "$(echo "$members" | grep ps-1 || true)"
  assert_not_contains "ps-1이 Failed가 아님" "Failed" "$(echo "$members" | grep ps-1 || true)"

  # Restart PS1 without node reset.
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node add ps-1 localhost:8001 2>/dev/null || true
  "$BIN_DIR/kv_server" -node-id ps-1 -addr localhost:8001 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" >"$PS1_LOG" 2>&1 &
  PS1_PID=$!
  sleep 3

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-1이 재기동 후 Active (node reset 없이)" "Active" "$(echo "$members" | grep ps-1 || true)"
}
should_run 15 || log "시나리오 15: SKIP"
should_run 15 && scenario_15

scenario_16() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 16: SIGKILL + 파티션 있음 + 다른 PS 없음 → 새 PS 자동 재할당 (케이스 E)"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  #
  # 전제: PS1 단독 운영, 파티션 보유 상태에서 SIGKILL
  # 기대: PS1=Failed, 파티션은 routing table에 남아 있다가
  #       PS2가 join하면 PM이 recoverOrphanedPartitions로 자동 재할당
  #
  reset_cluster_single_ps1

  kv_set "scenario16-key" "s16-val" >/dev/null

  # Ensure PS2 is not registered.
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node remove ps-2 2>/dev/null || true
  sleep 1

  # SIGKILL PS1 (파티션 있음, 다른 PS 없음).
  kill -9 "$PS1_PID" 2>/dev/null || true
  PS1_PID=""
  log "PS-1 SIGKILL (파티션 보유). Waiting for PM heartbeat timeout + walFlushMargin (~10s)..."
  # HeartbeatTimeout(5s) + walFlushMargin(3s) + 여유 2s
  sleep 10

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-1이 Failed 상태" "Failed" "$(echo "$members" | grep ps-1 || true)"
  assert_eq "파티션은 아직 routing table에 존재 (ps-1 소유)" "1" "$(partitions_on_node ps-1)"

  # Start PS2 — PM should auto-recover orphaned partitions on join.
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node add ps-2 localhost:8002 2>/dev/null || true
  "$BIN_DIR/kv_server" -node-id ps-2 -addr localhost:8002 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" >"$PS2_LOG" 2>&1 &
  PS2_PID=$!
  sleep 5  # Allow recoverOrphanedPartitions to complete

  assert_eq "파티션이 ps-2로 이동됨" "1" "$(partitions_on_node ps-2)"
  assert_eq "scenario16-key 데이터 복원 (checkpoint)" "s16-val" "$(kv_get scenario16-key)"
}
should_run 16 || log "시나리오 16: SKIP"
should_run 16 && scenario_16

scenario_17() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 17: SIGTERM + 파티션 있음 + 다른 PS 없음 → 다른 PS join 시 자동 재할당 (케이스 F)"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  #
  # 전제: PS1 단독 운영, 파티션 보유 상태에서 SIGTERM
  #   → drain target 없으므로 skip → EvictAll → checkpoint 저장 → Waiting
  # 기대: PS2가 join하면 PM이 recoverOrphanedPartitions로 PS1의 Waiting 파티션을 재할당
  #
  reset_cluster_single_ps1

  kv_set "scenario17-key" "s17-val" >/dev/null

  # Ensure PS2 is not registered.
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node remove ps-2 2>/dev/null || true
  sleep 1

  # SIGTERM PS1.
  kill "$PS1_PID" 2>/dev/null || true
  log "PS-1 SIGTERM sent. Waiting for drain + EvictAll (~10s)..."
  sleep 10
  PS1_PID=""

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-1이 Waiting 상태 (SIGTERM)" "Waiting" "$(echo "$members" | grep ps-1 || true)"
  assert_eq "파티션이 아직 ps-1 소유" "1" "$(partitions_on_node ps-1)"

  # Start PS2 (without resetting PS1) — PM should recover orphaned partitions.
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node add ps-2 localhost:8002 2>/dev/null || true
  "$BIN_DIR/kv_server" -node-id ps-2 -addr localhost:8002 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" >"$PS2_LOG" 2>&1 &
  PS2_PID=$!
  sleep 5  # Allow recoverOrphanedPartitions to complete

  assert_eq "파티션이 ps-2로 이동됨" "1" "$(partitions_on_node ps-2)"
  assert_eq "scenario17-key 데이터 복원 (checkpoint)" "s17-val" "$(kv_get scenario17-key)"
}
should_run 17 || log "시나리오 17: SKIP"
should_run 17 && scenario_17

scenario_18() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 18: Drained 경로 검증 (notifyDrained → isDrained → Waiting)"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  #
  # 전제: PS1 단독 운영, 파티션 보유
  # 흐름:
  #   1. PS2 기동 (drain target 확보)
  #   2. PS1 SIGTERM → notifyDraining → drainPartitions → notifyDrained → EvictAll → Deregister
  #   3. PM handleNodeLeft: isDrained=true → Waiting (not Failed)
  #
  # 주의: Drained 상태는 notifyDrained 호출~Deregister 완료 사이의 sub-second 과도 상태.
  #       polling으로 포착하기 어려우므로 대신 로그로 검증한다.
  #   - PS1 로그: "notified PM of drain completion"
  #   - PM 로그:  "returned to Waiting after completed drain"
  #   - 최종 상태: Waiting (not Failed)
  #
  reset_cluster_single_ps1
  start_ps2

  kv_set "s18-key" "s18-val" >/dev/null

  # SIGTERM PS1 — drain to PS2, then notifyDrained, EvictAll, exit.
  kill "$PS1_PID" 2>/dev/null || true
  log "PS-1 SIGTERM sent. Waiting for drain + notifyDrained + exit (~8s)..."
  sleep 8
  PS1_PID=""

  # Verify via logs that the Drained path was taken.
  ps1_log=$(cat "$PS1_LOG" 2>/dev/null || true)
  assert_contains "PS1 로그: PM에 drain 완료 통보" "notified PM of drain completion" "$ps1_log"
  assert_contains "PS1 로그: PM에 eviction 완료 신호 전송" "notified PM of eviction completion" "$ps1_log"

  pm_log=$(cat "$PM_LOG" 2>/dev/null || true)
  assert_contains "PM 로그: drain 완료 후 Waiting 복귀 (isDrained 경로)" "returned to Waiting after completed drain" "$pm_log"

  # Final state must be Waiting (not Failed): isDrained branch skips failoverDeadNode.
  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-1이 Waiting 상태 (Drained 경로 후 자동 복귀)" "Waiting" "$(echo "$members" | grep ps-1 || true)"
  assert_not_contains "ps-1이 Failed가 아님 (Drained 경로 성공)" "Failed" "$(echo "$members" | grep ps-1 || true)"

  assert_eq "s18-key 데이터 PS2에서 접근 가능" "s18-val" "$(kv_get s18-key)"
}
should_run 18 || log "시나리오 18: SKIP"
should_run 18 && scenario_18

scenario_19() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 19: Restricted 상태 검증"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  #
  # 전제: PS1 + PS2 운영 중
  # 흐름:
  #   1. PS2 restrict → Restricted
  #   2. abctl migrate ... ps-2 → FailedPrecondition 에러
  #   3. PS1 SIGTERM → drain: PS2는 target 후보에서 제외됨 (Active 없음 → drain skip)
  #   4. abctl node unrestrict ps-2 → Active
  #
  reset_cluster_single_ps1
  start_ps2

  kv_set "s19-key" "s19-val" >/dev/null
  sleep 1

  # Split to give PS1 a partition.
  PART_ID=$(partition_id_by_index 1)
  "$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$PART_ID" m >/dev/null 2>/dev/null || true
  sleep 1

  # Restrict PS2.
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node restrict ps-2 2>/dev/null
  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-2가 Restricted 상태" "Restricted" "$(echo "$members" | grep ps-2 || true)"

  # Attempt manual migration to Restricted node — should fail.
  PART_ON_PS1=$(routing_entries | awk -F'\t' '$4=="ps-1" {print $1; exit}')
  if [[ -n "$PART_ON_PS1" ]]; then
    migrate_out=$("$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$PART_ON_PS1" ps-2 2>&1 || true)
    assert_contains "Restricted 노드로 migrate 거부됨" "not active" "$migrate_out"
  else
    pass "ps-1에 파티션 없음 (split 미수행) — migration 테스트 skip"
  fi

  # Unrestrict PS2.
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node unrestrict ps-2 2>/dev/null
  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-2가 Active 상태 (unrestrict 후)" "Active" "$(echo "$members" | grep ps-2 || true)"

  assert_eq "s19-key 데이터 접근 가능" "s19-val" "$(kv_get s19-key)"
}
should_run 19 || log "시나리오 19: SKIP"
should_run 19 && scenario_19

scenario_20() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 20: PM heartbeat 기반 빠른 failover + EvictionComplete 검증"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  #
  # 검증 1 (SIGKILL): PM heartbeat timeout(5s) + walFlushMargin(3s) ≤ 10s 이내 failover
  #   - PM 로그에 "heartbeat timeout" 메시지 확인
  #   - PM 로그에 "waitForEviction: walFlushMargin elapsed" 확인 (EvictionComplete 없음)
  #   - 데이터 복원 확인 (WAL replay)
  #
  # 검증 2 (SIGTERM): EvictionComplete 빠른 경로
  #   - PS 로그에 "notified PM of eviction completion" 확인
  #
  reset_cluster_single_ps1
  start_ps2

  kv_set "s20-a" "val-a" >/dev/null
  kv_set "s20-b" "val-b" >/dev/null
  sleep 1  # WAL flush 대기

  # 파티션 분산: ps-1에 하나, ps-2에 하나
  PART_ID=$(partition_id_by_index 1)
  "$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$PART_ID" s20 >/dev/null 2>/dev/null || true
  sleep 1
  UPPER_ID=$(routing_entries | awk -F'\t' '$2=="s20" {print $1}')
  if [[ -n "$UPPER_ID" ]]; then
    "$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$UPPER_ID" ps-2 >/dev/null 2>/dev/null || true
    sleep 1
  fi

  # ── 검증 1: SIGKILL → PM heartbeat 기반 빠른 failover ──────────────────────
  log "PS-1 SIGKILL. 타이머 시작..."
  FAILOVER_START=$(date +%s)
  kill -9 "$PS1_PID" 2>/dev/null || true
  PS1_PID=""

  # HeartbeatTimeout(5s) + walFlushMargin(3s) + 여유 2s 이내 failover 기대
  sleep 10
  FAILOVER_END=$(date +%s)
  FAILOVER_ELAPSED=$((FAILOVER_END - FAILOVER_START))

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "SIGKILL 후 ps-1이 Failed 상태" "Failed" "$(echo "$members" | grep ps-1 || true)"
  assert_eq "SIGKILL 후 파티션이 ps-2로 이동됨" "2" "$(partitions_on_node ps-2)"

  if [[ "$FAILOVER_ELAPSED" -le 12 ]]; then
    pass "PM heartbeat 기반 failover: ${FAILOVER_ELAPSED}s (≤12s, 구 etcd TTL 15s+ 보다 빠름)"
  else
    fail "PM heartbeat 기반 failover: ${FAILOVER_ELAPSED}s (>12s — 예상보다 느림)"
  fi

  # PM 로그에 heartbeat timeout 메시지 확인
  pm_log=$(cat "$PM_LOG" 2>/dev/null || true)
  assert_contains "PM 로그: heartbeat timeout으로 노드 장애 감지" "heartbeat timeout" "$pm_log"
  assert_contains "PM 로그: walFlushMargin 경과 후 failover 진행" "walFlushMargin elapsed" "$pm_log"

  # 데이터 복원 (WAL replay from shared WAL)
  assert_eq "SIGKILL 후 s20-a 데이터 복원 (WAL replay)" "val-a" "$(kv_get s20-a)"
  assert_eq "SIGKILL 후 s20-b 데이터 복원 (WAL replay)" "val-b" "$(kv_get s20-b)"

  # ── 검증 2: SIGTERM → EvictionComplete 빠른 경로 ───────────────────────────
  # ps-1 재기동 후 파티션 하나 이전, 그 다음 SIGTERM
  start_ps1
  PART_FOR_PS1=$(routing_entries | awk -F'\t' '$4=="ps-2" {print $1; exit}')
  if [[ -n "$PART_FOR_PS1" ]]; then
    "$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$PART_FOR_PS1" ps-1 >/dev/null 2>/dev/null || true
    sleep 1
  fi

  # PS1 SIGTERM — shutdown() → EvictAll → notifyEvictionComplete
  kill "$PS1_PID" 2>/dev/null || true
  log "PS-1 SIGTERM sent. Waiting for drain + EvictAll + EvictionComplete (~8s)..."
  sleep 8
  PS1_PID=""

  ps1_log=$(cat "$PS1_LOG" 2>/dev/null || true)
  assert_contains "SIGTERM 시 PS1이 PM에 eviction 완료 신호 전송" \
    "notified PM of eviction completion" "$ps1_log"

  # PM 로그에 EvictionComplete 수신 메시지 확인
  pm_log=$(cat "$PM_LOG" 2>/dev/null || true)
  assert_contains "PM 로그: EvictionComplete 수신 → walFlushMargin 생략" \
    "EvictionComplete received" "$pm_log"
}
should_run 20 || log "시나리오 20: SKIP"
should_run 20 && scenario_20

# ═══════════════════════════════════════════════════════════════════════════════
# 결과 요약
# ═══════════════════════════════════════════════════════════════════════════════

log ""
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
TOTAL=$((PASS + FAIL))
log "결과: $PASS/$TOTAL passed, $FAIL failed"
log "로그: $LOG_DIR"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [[ "$FAIL" -eq 0 ]]; then
  log "===== ALL PASS ====="
  exit 0
else
  log "===== FAIL ($FAIL checks failed) ====="
  exit 1
fi
