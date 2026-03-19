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
#   8. Range Scan (다중 파티션 fan-out)
#   9. PM HA Failover (standby PM이 리더를 인계받음)
#  10. Actor Eviction + Re-activation (EvictionScheduler → getOrActivate)
#  11. SDK HA Mode 자동 재발견 (etcd 모드, PM 장애 중 자동 재연결)
#  12. Multi-actor-type 동시 운영 (kv + counter 파티션 공존)
#  13. drainPartitions 타임아웃 (PM 없는 환경 → EvictAll → checkpoint 복원)

set -euo pipefail

# ── 경로/설정 ─────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN_DIR="$ROOT_DIR/bin"

PM_ADDR="localhost:8000"
PM2_ADDR="localhost:8003"
ETCD_ADDR="localhost:2379"
WAL_DIR="/tmp/actorbase-itest/wal"
CKPT_DIR="/tmp/actorbase-itest/checkpoint"

RUN_ID="$(date '+%Y%m%d_%H%M%S')"
LOG_DIR="/tmp/actorbase-itest/logs/$RUN_ID"
mkdir -p "$LOG_DIR"

PM_LOG="$LOG_DIR/pm.log"
PM2_LOG="$LOG_DIR/pm2.log"
PS1_LOG="$LOG_DIR/ps1.log"
PS2_LOG="$LOG_DIR/ps2.log"

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

cleanup() {
  [[ -n "$PS2_PID"  ]] && kill "$PS2_PID"  2>/dev/null || true
  [[ -n "$PS1_PID"  ]] && kill "$PS1_PID"  2>/dev/null || true
  [[ -n "$PM3_PID"  ]] && kill "$PM3_PID"  2>/dev/null || true
  [[ -n "$PM2_PID"  ]] && kill "$PM2_PID"  2>/dev/null || true
  [[ -n "$PM_PID"   ]] && kill "$PM_PID"   2>/dev/null || true
  pkill -f "kv_stress" 2>/dev/null || true
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
# 시나리오 8: Range Scan (다중 파티션 조회)
# ═══════════════════════════════════════════════════════════════════════════════
#
# 이 시점의 클러스터 상태:
#   파티션: [start,f), [f,m), [m,end) — 모두 ps-2에 존재
#   키:  apple=red2, avocado=green, banana=yellow, cherry=red3 → [start,f) 또는 [f,m)
#        mango=orange, zebra=black → [m,end)
#
# 검증 목표:
#   - 전체 range scan이 여러 파티션에 걸쳐 누락 없이 동작하는지
#   - 부분 range scan이 올바른 파티션만 조회하는지

log ""
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "시나리오 8: Range Scan (다중 파티션 fan-out)"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

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

# ═══════════════════════════════════════════════════════════════════════════════
# 시나리오 9: PM HA Failover (standby PM이 리더를 인계받음)
# ═══════════════════════════════════════════════════════════════════════════════
#
# 이 시점: 클러스터가 정상 동작 중 (PM-1 leader, PS-2 active)
# 절차:
#   1. PM-2를 standby로 기동 (CampaignLeader에서 블로킹, gRPC 미오픈)
#   2. PM-1 SIGKILL → etcd 리더십 해제 → PM-2가 리더 승계
#   3. PM-2 gRPC 포트 오픈 확인 (라우팅 테이블 조회 성공)
#   4. 데이터 접근 정상 확인 (기존 라우팅 테이블 etcd에서 복원)

log ""
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "시나리오 9: PM HA Failover"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

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

# ═══════════════════════════════════════════════════════════════════════════════
# 시나리오 10: Actor Eviction + Re-activation
# ═══════════════════════════════════════════════════════════════════════════════
#
# 이 시점: PM-2가 리더, PS-2가 모든 파티션 소유
# 절차:
#   1. PS-2를 짧은 idle-timeout(5s)으로 재기동
#   2. 데이터 set → actor 활성화 → stats 확인 (partitions>0)
#   3. idle-timeout + evict-interval 대기 (10s)
#   4. stats 확인 → partitions=0 (EvictionScheduler가 evict)
#   5. 데이터 get → getOrActivate (checkpoint+WAL replay) → 값 정합성 확인
#   6. stats 확인 → partitions>0 (re-activation)

log ""
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "시나리오 10: Actor Eviction + Re-activation"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# PS-2 종료 후 짧은 idle-timeout으로 재기동
log "PS-2 재기동 (idle-timeout=5s, evict-interval=2s)..."
[[ -n "$PS2_PID" ]] && kill "$PS2_PID" 2>/dev/null || true
PS2_PID=""
sleep 5  # graceful shutdown 대기 (drain 실패 후 evictAll, deregister)

"$BIN_DIR/kv_server" -node-id ps-2 -addr localhost:8002 -etcd "$ETCD_ADDR" \
  -wal-dir "$WAL_DIR" -checkpoint-dir "$CKPT_DIR" \
  -idle-timeout 5s -evict-interval 2s >"$LOG_DIR/ps2_evict.log" 2>&1 &
PS2_PID=$!
sleep 3  # etcd 등록 + 초기 라우팅 테이블 수신 대기

members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
assert_contains "ps-2 재기동 후 members에 존재" "ps-2" "$members"

# 데이터 set → 해당 파티션의 actor 활성화 (getOrActivate: checkpoint+WAL replay)
kv_set "evict-key" "evict-value" >/dev/null
sleep 1

# set 직후 stats → 활성 partition 1개 이상
stats_after_set=$("$BIN_DIR/abctl" -pm "$PM_ADDR" stats 2>/dev/null)
ps2_active_set=$(echo "$stats_after_set" | grep "Node: ps-2" | grep -oE 'partitions=[0-9]+' | grep -oE '[0-9]+' || echo "0")
if [[ "$ps2_active_set" -gt 0 ]]; then
  pass "set 직후 actor active (ps-2 partitions=$ps2_active_set)"
else
  fail "set 직후 actor active 확인 실패 (ps-2 partitions=$ps2_active_set)"
fi

# idle-timeout(5s) + evict-interval(2s) + 여유(3s) 대기
log "EvictionScheduler 대기 (10s)..."
sleep 10

# eviction 확인 → partitions=0
stats_after_evict=$("$BIN_DIR/abctl" -pm "$PM_ADDR" stats 2>/dev/null)
ps2_active_evict=$(echo "$stats_after_evict" | grep "Node: ps-2" | grep -oE 'partitions=[0-9]+' | grep -oE '[0-9]+' || echo "0")
assert_eq "EvictionScheduler 동작: actor evict됨 (partitions=0)" "0" "$ps2_active_evict"

# re-activation: get 요청 → getOrActivate → checkpoint+WAL replay
val=$(kv_get "evict-key")
assert_eq "re-activation 후 값 복원 (evict-value)" "evict-value" "$val"
sleep 1

# re-activation 후 stats → partitions>0
stats_after_get=$("$BIN_DIR/abctl" -pm "$PM_ADDR" stats 2>/dev/null)
ps2_active_get=$(echo "$stats_after_get" | grep "Node: ps-2" | grep -oE 'partitions=[0-9]+' | grep -oE '[0-9]+' || echo "0")
if [[ "$ps2_active_get" -gt 0 ]]; then
  pass "re-activation 후 actor active 복원 (ps-2 partitions=$ps2_active_get)"
else
  fail "re-activation 후 actor active 복원 실패 (ps-2 partitions=$ps2_active_get)"
fi

# ═══════════════════════════════════════════════════════════════════════════════
# 시나리오 11: SDK HA Mode 자동 재발견
# ═══════════════════════════════════════════════════════════════════════════════
#
# 이 시점: PM-2가 리더 (localhost:8003), PS-2가 kv 파티션 소유
# 절차:
#   1. PM-3 standby로 기동 (:8004)
#   2. kv_stress를 etcd 모드로 실행 (PM 주소 없이 etcd에서 자동 발견)
#   3. 5초 후 PM-2 SIGKILL
#   4. PM-3가 리더 승계 (~15s)
#   5. kv_stress fail 수 검증 (cached routing → PS 직접 접속으로 대부분 성공)

log ""
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "시나리오 11: SDK HA Mode 자동 재발견"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

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
stress_result=$(grep "완료:" "$STRESS_LOG" 2>/dev/null | tail -1 || true)
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

# ═══════════════════════════════════════════════════════════════════════════════
# 시나리오 12: Multi-actor-type 동시 운영
# ═══════════════════════════════════════════════════════════════════════════════
#
# 절차:
#   1. 클러스터 전체 재기동 (kv + counter 두 타입 지원)
#   2. 라우팅 테이블에 kv, counter 두 타입 파티션 존재 확인
#   3. kv 요청 정상 동작 확인
#   4. abctl split 후 두 타입 파티션 모두 유지되는지 확인

log ""
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "시나리오 12: Multi-actor-type 동시 운영"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# 기존 클러스터 정리
log "기존 클러스터 정리..."
[[ -n "$PM3_PID" ]] && { kill "$PM3_PID" 2>/dev/null || true; PM3_PID=""; }
[[ -n "$PS2_PID" ]] && { kill "$PS2_PID" 2>/dev/null || true; PS2_PID=""; }
[[ -n "$PS1_PID" ]] && { kill "$PS1_PID" 2>/dev/null || true; PS1_PID=""; }
sleep 3

# etcd 데이터 초기화 및 WAL/CKPT 정리 (클린 재기동)
etcdctl --endpoints="$ETCD_ADDR" del /actorbase/ --prefix >/dev/null 2>&1 || true
rm -rf "$WAL_DIR" "$CKPT_DIR"
mkdir -p "$WAL_DIR" "$CKPT_DIR"

# PM 재기동: kv + counter 두 타입 지원
log "PM 재기동 with -actor-types kv,counter..."
PM_ADDR="localhost:8000"
"$BIN_DIR/pm" -addr :8000 -etcd "$ETCD_ADDR" -actor-types kv,counter \
  >"$LOG_DIR/pm_multi.log" 2>&1 &
PM_PID=$!
sleep 1

# PS-1 재기동: kv + counter 두 타입 등록
log "PS-1 재기동 with -actor-types kv,counter..."
"$BIN_DIR/kv_server" -node-id ps-1 -addr localhost:8001 -etcd "$ETCD_ADDR" \
  -wal-dir "$WAL_DIR" -checkpoint-dir "$CKPT_DIR" \
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

# abctl split 후 두 타입 모두 유지되는지 확인
kv_part_id=$(routing_entries | awk -F'\t' 'NR==1 {print $1}')
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

# ═══════════════════════════════════════════════════════════════════════════════
# 시나리오 13: drainPartitions 타임아웃 (PM 없는 환경)
# ═══════════════════════════════════════════════════════════════════════════════
#
# 절차:
#   1. 테스트 데이터 삽입 + WAL flush 대기
#   2. PS-1을 drain-timeout=3s로 재기동
#   3. PM 종료 (drain 실패 유도)
#   4. PS-1 SIGTERM → drain 3s 후 타임아웃 → EvictAll(checkpoint 저장) → 종료
#   5. PM + PS-1 재기동 → checkpoint에서 데이터 복원 확인

log ""
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "시나리오 13: drainPartitions 타임아웃"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# 테스트 데이터 삽입 (checkpoint 복원 검증용)
kv_set "drain-key-1" "drain-val-1" >/dev/null
kv_set "drain-key-2" "drain-val-2" >/dev/null
sleep 1  # WAL flush 대기

# PS-1을 짧은 drain-timeout(3s)으로 재기동
log "PS-1 재기동 (drain-timeout=3s)..."
[[ -n "$PS1_PID" ]] && { kill "$PS1_PID" 2>/dev/null || true; PS1_PID=""; }
sleep 2

"$BIN_DIR/kv_server" -node-id ps-1 -addr localhost:8001 -etcd "$ETCD_ADDR" \
  -wal-dir "$WAL_DIR" -checkpoint-dir "$CKPT_DIR" \
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
  -wal-dir "$WAL_DIR" -checkpoint-dir "$CKPT_DIR" \
  -actor-types kv,counter >"$LOG_DIR/ps1_after_drain.log" 2>&1 &
PS1_PID=$!
sleep 3

PM_ADDR="localhost:8000"
assert_eq "drain 후 재기동: drain-key-1 복원" "drain-val-1" "$(kv_get drain-key-1)"
assert_eq "drain 후 재기동: drain-key-2 복원" "drain-val-2" "$(kv_get drain-key-2)"

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
