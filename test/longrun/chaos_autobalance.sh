#!/usr/bin/env bash
# chaos_autobalance.sh — auto balancer longrun 테스트용 chaos 이벤트 주입기
#
# 사용법: chaos_autobalance.sh <bin_dir> <pm_addr> <wal_dir> <ckpt_dir> <etcd_addr>
#
# auto balancer가 주기적으로 split/migrate를 수행하는 상태에서
# 수동 split/migrate와 장애/재기동을 함께 주입해 충돌 없이 동작하는지 검증한다.
#
# 이벤트 스케줄:
#   T+30s  수동 split
#   T+60s  SIGKILL PS-3  → auto failover (OnNodeLeft)
#   T+90s  PS-3 재기동   → auto rebalance (OnNodeJoined)
#   T+120s 수동 migrate → PS-2
#   T+150s SIGTERM PS-1  → graceful drain + OnNodeLeft
#   T+180s PS-1 재기동   → auto rebalance (OnNodeJoined)
#   T+210s 수동 split
#   T+240s SIGKILL PS-2  → auto failover (OnNodeLeft)
#   T+270s PS-2 재기동   → auto rebalance (OnNodeJoined)
#   T+300s 수동 migrate → PS-3

set -euo pipefail

BIN_DIR="${1:-./bin}"
PM_ADDR="${2:-localhost:8000}"
WAL_DIR="${3:-/tmp/actorbase/wal}"
CKPT_DIR="${4:-/tmp/actorbase/checkpoint}"
ETCD_ADDR="${5:-localhost:2379}"

ABCTL="$BIN_DIR/abctl"
KV_SERVER="$BIN_DIR/kv_server"

PS1_PID_FILE="/tmp/ab_ps1.pid"
PS2_PID_FILE="/tmp/ab_ps2.pid"
PS3_PID_FILE="/tmp/ab_ps3.pid"

log() {
  echo "[$(date '+%H:%M:%S')] [chaos] $*"
}

# 라우팅 테이블 파싱: id\tstart\tend\tnode
get_routing_entries() {
  "$ABCTL" -pm "$PM_ADDR" routing 2>/dev/null \
    | awk 'NR>4 && $1!="" && $1!~"^-" {print $1"\t"$3"\t"$4"\t"$5}'
}

key_to_num() {
  local k="$1"
  case "$k" in
    "(start)") echo "-1" ;;
    "(end)")   echo "10000" ;;
    key:*)
      local num="${k#key:}"
      echo "$((10#$num))"
      ;;
    *) echo "0" ;;
  esac
}

do_split() {
  local entries
  entries=$(get_routing_entries)
  if [[ -z "$entries" ]]; then
    log "WARN: no partitions found for split, skipping"
    return
  fi

  local chosen_id="" chosen_key=""
  while IFS=$'\t' read -r pid pstart pend pnode; do
    local s e mid
    s=$(key_to_num "$pstart")
    e=$(key_to_num "$pend")
    mid=$(( (s + e) / 2 ))
    if [[ $mid -gt $s && $mid -lt $e ]]; then
      chosen_id="$pid"
      chosen_key=$(printf "key:%08d" "$mid")
      break
    fi
  done <<< "$entries"

  if [[ -z "$chosen_id" ]]; then
    log "WARN: no splittable partition found, skipping"
    return
  fi
  log "manual split partition=$chosen_id at key=$chosen_key"
  "$ABCTL" -pm "$PM_ADDR" split kv "$chosen_id" "$chosen_key" \
    && log "split done" || log "WARN: split failed (continuing)"
}

do_migrate() {
  local target_node="$1"
  local entries
  entries=$(get_routing_entries)
  if [[ -z "$entries" ]]; then
    log "WARN: no partitions found for migrate, skipping"
    return
  fi

  local chosen_id=""
  while IFS=$'\t' read -r pid pstart pend pnode; do
    if [[ "$pnode" != "$target_node" ]]; then
      chosen_id="$pid"
      break
    fi
  done <<< "$entries"

  if [[ -z "$chosen_id" ]]; then
    log "WARN: all partitions already on $target_node, skipping"
    return
  fi
  log "manual migrate partition=$chosen_id to node=$target_node"
  "$ABCTL" -pm "$PM_ADDR" migrate kv "$chosen_id" "$target_node" \
    && log "migrate done" || log "WARN: migrate failed (continuing)"
}

kill_ps() {
  local node_id="$1"
  local signal="$2"
  local pid_file
  case "$node_id" in
    ps-1) pid_file="$PS1_PID_FILE" ;;
    ps-2) pid_file="$PS2_PID_FILE" ;;
    ps-3) pid_file="$PS3_PID_FILE" ;;
    *) log "WARN: unknown node $node_id"; return ;;
  esac

  if [[ ! -f "$pid_file" ]]; then
    log "WARN: pid file not found for $node_id: $pid_file"
    return
  fi
  local pid
  pid=$(cat "$pid_file")
  if kill -0 "$pid" 2>/dev/null; then
    log "Sending SIG$signal to $node_id (pid=$pid)"
    kill "-$signal" "$pid"
    if [[ "$signal" == "9" ]]; then
      rm -f "$pid_file"
    fi
  else
    log "WARN: $node_id (pid=$pid) already dead"
    rm -f "$pid_file"
  fi
}

start_ps() {
  local node_id="$1"
  local addr="$2"
  local pid_file
  case "$node_id" in
    ps-1) pid_file="$PS1_PID_FILE" ;;
    ps-2) pid_file="$PS2_PID_FILE" ;;
    ps-3) pid_file="$PS3_PID_FILE" ;;
  esac

  local port
  port="${addr##*:}"

  local holder
  holder=$(lsof -iTCP:"$port" -sTCP:LISTEN -P -n 2>/dev/null | tail -n +2 || true)
  if [[ -n "$holder" ]]; then
    log "WARN: port $port is already in use before starting $node_id:"
    echo "$holder" | while IFS= read -r line; do log "  $line"; done
  fi

  log "Restarting $node_id at $addr"
  "$KV_SERVER" \
    -node-id "$node_id" \
    -addr "$addr" \
    -etcd "$ETCD_ADDR" \
    -wal-dir "$WAL_DIR" \
    -checkpoint-dir "$CKPT_DIR" \
    >> "/tmp/ab_${node_id//-/_}.log" 2>&1 &
  local pid=$!
  echo $pid > "$pid_file"

  sleep 0.5
  if ! kill -0 "$pid" 2>/dev/null; then
    log "ERROR: $node_id (pid=$pid) exited immediately."
  else
    log "$node_id restarted (pid=$pid)"
  fi
}

log "=== Auto-balance chaos schedule starting (PM=$PM_ADDR) ==="
log "    auto balancer + 수동 split/migrate/failover 동시 동작 검증"

sleep 30;  do_split
sleep 30;  kill_ps "ps-3" "9"
sleep 30;  start_ps "ps-3" "localhost:8003"
sleep 30;  do_migrate "ps-2"
sleep 30;  kill_ps "ps-1" "15"
sleep 30;  start_ps "ps-1" "localhost:8001"
sleep 30;  do_split
sleep 30;  kill_ps "ps-2" "9"
sleep 30;  start_ps "ps-2" "localhost:8002"
sleep 30;  do_migrate "ps-3"

log "=== All chaos events completed. ==="
