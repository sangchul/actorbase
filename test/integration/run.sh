#!/usr/bin/env bash
# test/integration/run.sh — actorbase 통합 시나리오 테스트 자동화
#
# 실행 방법 (프로젝트 루트에서):
#   bash test/integration/run.sh
#
# 사전 요건:
#   - etcd 실행 중 (localhost:2379)
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

set -euo pipefail

# ── 경로/설정 ─────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN_DIR="$ROOT_DIR/bin"

PM_ADDR="localhost:8000"
ETCD_ADDR="localhost:2379"
WAL_DIR="/tmp/actorbase-itest/wal"
CKPT_DIR="/tmp/actorbase-itest/checkpoint"

RUN_ID="$(date '+%Y%m%d_%H%M%S')"
LOG_DIR="/tmp/actorbase-itest/logs/$RUN_ID"
mkdir -p "$LOG_DIR"

PM_LOG="$LOG_DIR/pm.log"
PS1_LOG="$LOG_DIR/ps1.log"
PS2_LOG="$LOG_DIR/ps2.log"

PM_PID=""
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

cleanup() {
  [[ -n "$PS2_PID" ]] && kill "$PS2_PID" 2>/dev/null || true
  [[ -n "$PS1_PID" ]] && kill "$PS1_PID" 2>/dev/null || true
  [[ -n "$PM_PID"  ]] && kill "$PM_PID"  2>/dev/null || true
  pkill -f "kv_stress" 2>/dev/null || true
  log "Cleanup done. Logs: $LOG_DIR"
}
trap cleanup EXIT

# ── 바이너리 확인 ──────────────────────────────────────────────────────────────

for bin in pm abctl kv_server kv_client kv_stress; do
  if [[ ! -x "$BIN_DIR/$bin" ]]; then
    echo "ERROR: $BIN_DIR/$bin not found. Run: go build -o bin/$bin ./..."
    exit 1
  fi
done

# ── etcd 확인 ─────────────────────────────────────────────────────────────────

if ! etcdctl --endpoints="$ETCD_ADDR" endpoint health >/dev/null 2>&1; then
  echo "ERROR: etcd is not running at $ETCD_ADDR"
  exit 1
fi

# ── 환경 초기화 ───────────────────────────────────────────────────────────────

log "Cleaning old data (etcd, WAL, checkpoint)..."
etcdctl --endpoints="$ETCD_ADDR" del /actorbase/ --prefix >/dev/null 2>&1 || true
rm -rf "$WAL_DIR" "$CKPT_DIR"
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
  "$BIN_DIR/kv_server" -node-id ps-1 -addr localhost:8001 -etcd "$ETCD_ADDR" \
    -wal-dir "$WAL_DIR" -checkpoint-dir "$CKPT_DIR" >"$PS1_LOG" 2>&1 &
  PS1_PID=$!
  sleep 2
}

start_ps2() {
  "$BIN_DIR/kv_server" -node-id ps-2 -addr localhost:8002 -etcd "$ETCD_ADDR" \
    -wal-dir "$WAL_DIR" -checkpoint-dir "$CKPT_DIR" >"$PS2_LOG" 2>&1 &
  PS2_PID=$!
  sleep 2
}

# ═══════════════════════════════════════════════════════════════════════════════
# 시나리오 1: 클러스터 부트스트랩
# ═══════════════════════════════════════════════════════════════════════════════

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

# ═══════════════════════════════════════════════════════════════════════════════
# 시나리오 2: 기본 KV 동작
# ═══════════════════════════════════════════════════════════════════════════════

log ""
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "시나리오 2: 기본 KV 동작 (set/get/del)"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

assert_eq "set user:1001" "ok"             "$(kv_set user:1001 '{"name":"alice"}')"
assert_eq "get user:1001" '{"name":"alice"}' "$(kv_get user:1001)"
assert_eq "del user:1001" "ok"             "$(kv_del user:1001)"
assert_eq "get after del (not found)" ""  "$(kv_get user:1001)"

# ═══════════════════════════════════════════════════════════════════════════════
# 시나리오 3: 파티션 Split
# ═══════════════════════════════════════════════════════════════════════════════

log ""
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "시나리오 3: 파티션 Split"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

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

# ═══════════════════════════════════════════════════════════════════════════════
# 시나리오 4: Scale-out + Migrate
# ═══════════════════════════════════════════════════════════════════════════════

log ""
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "시나리오 4: Scale-out + Migrate"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

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

# ═══════════════════════════════════════════════════════════════════════════════
# 시나리오 5: 예기치 않은 장애 복구 (SIGKILL → Failover + WAL replay)
# ═══════════════════════════════════════════════════════════════════════════════

log ""
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "시나리오 5: SIGKILL → 자동 Failover + WAL replay"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# checkpoint 이후 WAL에만 기록되는 데이터 삽입
kv_set apple  red2  >/dev/null  # 이전 값 red → red2
kv_set avocado green >/dev/null
kv_set cherry  red3  >/dev/null

# PS-1 강제 종료 (SIGKILL)
kill -9 "$PS1_PID" 2>/dev/null || true
PS1_PID=""
log "PS-1 killed. Waiting for etcd lease expiry (~15s)..."

# etcd TTL 만료 대기 (기본 10s, 여유 5s)
sleep 15

members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
assert_not_contains "ps-1이 members에서 제거됨" "ps-1" "$members"
assert_contains     "ps-2가 active 상태"        "ps-2" "$members"

assert_eq "failover 후 Version=4"         "4"  "$(routing_version)"
assert_eq "하위 파티션이 ps-2로 이동됨"   "2"  "$(partitions_on_node ps-2)"

# checkpoint 이전 데이터
assert_eq "banana (checkpoint 복원)" "yellow" "$(kv_get banana)"
assert_eq "mango  (checkpoint 복원)" "orange" "$(kv_get mango)"

# WAL replay 데이터 (checkpoint 이후 기록)
assert_eq "apple  (WAL replay: red2, not red)" "red2"  "$(kv_get apple)"
assert_eq "avocado (WAL replay)"               "green" "$(kv_get avocado)"
assert_eq "cherry  (WAL replay)"               "red3"  "$(kv_get cherry)"

# ═══════════════════════════════════════════════════════════════════════════════
# 시나리오 6: SDK 라우팅 자동 갱신 (부하 중 split, fail=0)
# ═══════════════════════════════════════════════════════════════════════════════

log ""
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "시나리오 6: SDK 라우팅 자동 갱신 (부하 중 split)"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

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

# ═══════════════════════════════════════════════════════════════════════════════
# 시나리오 7: Graceful Shutdown (SIGTERM → drainPartitions)
# ═══════════════════════════════════════════════════════════════════════════════

log ""
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "시나리오 7: Graceful Shutdown (SIGTERM → drain)"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

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
assert_not_contains "ps-1이 members에서 제거됨 (SIGTERM)" "ps-1" "$members"

# drain 후 ps-1 파티션이 ps-2로 이전되었는지
assert_eq "drain 후 ps-2에 모든 파티션" "$(partition_count)" "$(partitions_on_node ps-2)"

# 데이터 정합성
assert_eq "apple 조회 (drain 후)"  "red2"  "$(kv_get apple)"
assert_eq "mango 조회 (drain 후)"  "orange" "$(kv_get mango)"

wait "$STRESS7_PID" 2>/dev/null || true
PS1_PID=""

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
