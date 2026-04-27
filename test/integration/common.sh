#!/usr/bin/env bash
# test/integration/common.sh — shared config, utilities, and cluster setup helpers.
# Source this file; do not execute directly.
#
# Usage:
#   In run.sh:          source "$INTEGRATION_DIR/common.sh"
#   In scenario files:  source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

# Guard against double-sourcing.
[[ -n "${_ACTORBASE_COMMON_LOADED:-}" ]] && return 0
_ACTORBASE_COMMON_LOADED=1

set -euo pipefail

# ── Paths and ports ───────────────────────────────────────────────────────────

INTEGRATION_DIR="${INTEGRATION_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
ROOT_DIR="$(cd "$INTEGRATION_DIR/../.." && pwd)"
BIN_DIR="$ROOT_DIR/bin"

PM_ADDR="localhost:8000"
PM2_ADDR="localhost:8003"
ETCD_ADDR="localhost:2379"
ETCD_DATA_DIR="/tmp/actorbase-itest/etcd"
WAL_DIR="/tmp/actorbase-itest/wal"
CKPT_DIR="/tmp/actorbase-itest/checkpoint"

# WAL backend: fs (default) or redis
WAL_BACKEND="${WAL_BACKEND:-fs}"
REDIS_ADDR="${REDIS_ADDR:-localhost:6379}"

# Checkpoint backend: fs (default) or minio
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

# WAL_ARGS / CKPT_ARGS are populated by setup_backends().
WAL_ARGS=()
CKPT_ARGS=()

# SELECTED_SCENARIOS must be set by the caller before sourcing.
# Default to empty (run all) if not already set.
if [[ -z "${SELECTED_SCENARIOS+_}" ]]; then
  SELECTED_SCENARIOS=()
fi

# ── Utilities ─────────────────────────────────────────────────────────────────

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

kv_scan() {
  local start="$1" end="$2"
  "$BIN_DIR/kv_client" -pm "$PM_ADDR" scan "$start" "$end" 2>/dev/null || true
}

routing() { "$BIN_DIR/abctl" -pm "$PM_ADDR" routing 2>/dev/null; }

# Parse routing table: id\tstart\tend\tnode
routing_entries() {
  routing | awk 'NR>4 && $1!="" && $1!~"^-" {print $1"\t"$3"\t"$4"\t"$5}'
}

routing_version() {
  routing | awk '/^Version:/ {print $2}'
}

# Retrieve epoch for a specific partition ID ($6 column in routing output).
partition_epoch() {
  local partID="$1"
  routing | awk -v id="$partID" 'NR>4 && $1==id {print $6}'
}

# Number of partitions on a given node.
partitions_on_node() {
  local node="$1"
  routing_entries | awk -F'\t' -v n="$node" '$4==n {count++} END {print count+0}'
}

# Partition ID by 1-based index from routing_entries.
partition_id_by_index() {
  local idx="$1"
  routing_entries | awk -F'\t' "NR==$idx {print \$1}"
}

# Total partition count.
partition_count() {
  routing_entries | wc -l | tr -d ' '
}

# kv-only routing entries (multi-actor-type environments).
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

# Routing entries from a specific PM address (used in split-brain scenarios).
routing_entries_from() {
  local addr="$1"
  "$BIN_DIR/abctl" -pm "$addr" routing 2>/dev/null \
    | awk 'NR>4 && $1!="" && $1!~"^-" {print $1"\t"$3"\t"$4"\t"$5}'
}

# ── Scenario selector ─────────────────────────────────────────────────────────

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

# ── Cleanup ───────────────────────────────────────────────────────────────────

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

# ── Infrastructure functions ──────────────────────────────────────────────────

check_binaries() {
  for bin in pm abctl kv_server kv_client kv_stress; do
    if [[ ! -x "$BIN_DIR/$bin" ]]; then
      echo "ERROR: $BIN_DIR/$bin not found. Run: go build -o bin/$bin ./..."
      exit 1
    fi
  done
}

start_etcd() {
  if ! command -v etcd >/dev/null 2>&1; then
    echo "ERROR: etcd binary not found in PATH"
    exit 1
  fi

  # Kill any process holding ports 2379/2380 (leftover etcd from previous runs).
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
}

setup_backends() {
  if [[ "$WAL_BACKEND" == "redis" ]]; then
    log "WAL_BACKEND=redis — checking Redis at $REDIS_ADDR..."
    if ! redis-cli -u "redis://$REDIS_ADDR" PING >/dev/null 2>&1; then
      echo "ERROR: Redis is not running at $REDIS_ADDR (required for WAL_BACKEND=redis)"
      exit 1
    fi
  fi

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

  if [[ "$WAL_BACKEND" == "redis" ]]; then
    WAL_ARGS=(-wal-backend redis -redis-addr "$REDIS_ADDR")
  else
    WAL_ARGS=(-wal-dir "$WAL_DIR")
  fi

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
}

clean_data() {
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
}

start_pm() {
  log "Starting PM ($PM_ADDR)..."
  "$BIN_DIR/pm" -addr :8000 -etcd "$ETCD_ADDR" -actor-types kv >"$PM_LOG" 2>&1 &
  PM_PID=$!
  sleep 1
  if ! kill -0 "$PM_PID" 2>/dev/null; then
    echo "ERROR: PM failed to start. See $PM_LOG"
    exit 1
  fi
}

start_ps1() {
  # If node is in Failed state (after SIGKILL), reset to Waiting so PM accepts RequestJoin.
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node reset ps-1 2>/dev/null || true
  # Pre-register ps-1 in the PM catalog (Waiting → Active on startup).
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node add ps-1 localhost:8001 2>/dev/null || true
  "$BIN_DIR/kv_server" -node-id ps-1 -addr localhost:8001 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" >"$PS1_LOG" 2>&1 &
  PS1_PID=$!
  sleep 2
  # Poll until ps-1 appears as Active (PM sends PreparePartition and actor is ready).
  for _i in 1 2 3 4 5 6 7 8; do
    "$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null | grep -q "ps-1" && return
    sleep 1
  done
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
  # Poll until ps-2 appears in members.
  for _i in 1 2 3 4 5 6 7 8; do
    "$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null | grep -q "ps-2" && return
    sleep 1
  done
}

# ── Cluster state setup helpers ───────────────────────────────────────────────

# Common base: kill all processes, reset etcd/WAL/CKPT, restart PM (kv-only).
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

# PM(kv) + PS1 single node (1 partition) — for scenarios 2, 3.
setup_basic_cluster() {
  _base_reset
  start_ps1
}

# PM(kv) + PS1 + split(m) complete (2 partitions, basic data) — for scenario 4.
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

# PM(kv) + PS1[start,m) + PS2[m,end) + basic data — for scenario 5.
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

# PM(kv) + PS2 only (all partitions migrated to PS2, PS1 stopped) — for scenarios 6, 7, 9, 10, 11.
setup_ps2_only() {
  setup_split_cluster
  local pid
  for pid in $(routing_entries | awk -F'\t' '$4=="ps-1" {print $1}'); do
    "$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$pid" ps-2 >/dev/null 2>&1 || true
    sleep 1
  done
  if [[ -n "$PS1_PID" ]]; then
    kill "$PS1_PID" 2>/dev/null || true
    PS1_PID=""
  fi
  sleep 4  # wait for drain
}

# PM(kv) + PS2 only, 3 partitions [start,f), [f,m), [m,end) + specific data — for scenario 8.
setup_three_partitions_ps2() {
  setup_basic_cluster
  kv_set apple   red2   >/dev/null
  kv_set banana  yellow >/dev/null
  kv_set avocado green  >/dev/null
  kv_set cherry  red3   >/dev/null
  kv_set mango   orange >/dev/null
  kv_set zebra   black  >/dev/null
  local pid
  pid=$(partition_id_by_index 1)
  "$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$pid" m >/dev/null 2>&1
  sleep 1
  local lower_id
  lower_id=$(routing_entries | awk -F'\t' '$2!="m" {print $1; exit}')
  "$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$lower_id" f >/dev/null 2>&1
  sleep 1
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

# PM(kv,counter) + PS1(kv,counter) — for scenarios 13, 14.
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

# Hard-resets the cluster to a single kv-only PS1 state.
# Clears etcd fully (removes counter partitions from scenario 12) and restarts PM
# with kv-only so the routing table starts clean.
reset_cluster_single_ps1() {
  [[ -n "$PS2_PID" ]] && { kill "$PS2_PID" 2>/dev/null || true; PS2_PID=""; }
  [[ -n "$PS1_PID" ]] && { kill "$PS1_PID" 2>/dev/null || true; PS1_PID=""; }
  [[ -n "$PM3_PID" ]] && { kill "$PM3_PID" 2>/dev/null || true; PM3_PID=""; }
  [[ -n "$PM2_PID" ]] && { kill "$PM2_PID" 2>/dev/null || true; PM2_PID=""; }
  [[ -n "$PM_PID"  ]] && { kill "$PM_PID"  2>/dev/null || true; PM_PID="";  }
  sleep 3

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
    echo "ERROR: PM failed to start in reset_cluster_single_ps1. See $PM_LOG"
    exit 1
  fi

  start_ps1
  log "reset_cluster_single_ps1: cluster reset done, PS1 active (kv-only)"
}

# ── Summary ───────────────────────────────────────────────────────────────────

print_summary() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  local TOTAL=$((PASS + FAIL))
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
}
