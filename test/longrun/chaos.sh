#!/usr/bin/env bash
# chaos.sh — actorbase longrun 테스트용 chaos 이벤트 주입기
#
# 사용법: chaos.sh <bin_dir> <pm_addr> <wal_dir> <ckpt_dir> <etcd_addr>
#
# 이벤트 스케줄 (longrun-test.md 기준):
#   T+30s  split
#   T+60s  migrate → PS-2
#   T+90s  split
#   T+120s SIGKILL PS-3
#   T+150s PS-3 재기동
#   T+180s migrate → PS-3
#   T+210s split
#   T+240s SIGTERM PS-1
#   T+270s PS-1 재기동
#   T+300s migrate → PS-1
#   T+330s split
#   T+360s SIGKILL PS-2
#   T+390s PS-2 재기동

set -euo pipefail

BIN_DIR="${1:-./bin}"
PM_ADDR="${2:-localhost:8000}"
WAL_DIR="${3:-/tmp/actorbase/wal}"
CKPT_DIR="${4:-/tmp/actorbase/checkpoint}"
ETCD_ADDR="${5:-localhost:2379}"

ABCTL="$BIN_DIR/abctl"
KV_SERVER="$BIN_DIR/kv_server"

PS3_PID_FILE="/tmp/ab_ps3.pid"
PS1_PID_FILE="/tmp/ab_ps1.pid"
PS2_PID_FILE="/tmp/ab_ps2.pid"

log() {
  echo "[$(date '+%H:%M:%S')] [chaos] $*"
}

# 라우팅 테이블 파싱: 한 줄에 id\tstart\tend\tnode (탭 구분)
# 출력 형식: PARTITION-ID  ACTOR-TYPE  KEY-START  KEY-END  NODE-ID  NODE-ADDR
# $1=PARTITION-ID  $2=ACTOR-TYPE  $3=KEY-START  $4=KEY-END  $5=NODE-ID
get_routing_entries() {
  "$ABCTL" -pm "$PM_ADDR" routing 2>/dev/null \
    | awk 'NR>4 && $1!="" && $1!~"^-" {print $1"\t"$3"\t"$4"\t"$5}'
}

# "key:NNNNNNNN" → 정수, "(start)" → -1, "(end)" → 10000
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

# do_split: 라우팅 테이블에서 분할 가능한 파티션을 찾아 중간점으로 split
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
    # split key가 파티션 범위 내에 있어야 함 (strictly inside)
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
  log "split partition=$chosen_id at key=$chosen_key"
  "$ABCTL" -pm "$PM_ADDR" split kv "$chosen_id" "$chosen_key" \
    && log "split done" || log "WARN: split failed (continuing)"
}

# do_migrate: target_node에 없는 파티션을 찾아 migrate
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
  log "migrate partition=$chosen_id to node=$target_node"
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
    # SIGKILL은 pid file 제거 (run.sh watcher가 재기동)
    if [[ "$signal" == "9" ]]; then
      rm -f "$pid_file"
    fi
    # SIGTERM은 프로세스가 스스로 종료하면 watcher가 감지
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

  # 포트 점유 여부 사전 확인
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

  # 기동 직후 실패 여부 확인 (0.5s)
  sleep 0.5
  if ! kill -0 "$pid" 2>/dev/null; then
    log "ERROR: $node_id (pid=$pid) exited immediately. Port holder:"
    lsof -iTCP:"$port" -sTCP:LISTEN -P -n 2>/dev/null | while IFS= read -r line; do log "  $line"; done
  else
    log "$node_id restarted (pid=$pid)"
  fi
}

log "=== Chaos schedule starting (PM=$PM_ADDR) ==="

sleep 30;  do_split
sleep 30;  do_migrate "ps-2"
sleep 30;  do_split
sleep 30;  kill_ps "ps-3" "9"
sleep 30;  start_ps "ps-3" "localhost:8003"
sleep 30;  do_migrate "ps-3"
sleep 30;  do_split
sleep 30;  kill_ps "ps-1" "15"
sleep 30;  start_ps "ps-1" "localhost:8001"
sleep 30;  do_migrate "ps-1"
sleep 30;  do_split
sleep 30;  kill_ps "ps-2" "9"
sleep 30;  start_ps "ps-2" "localhost:8002"

log "=== All chaos events completed. ==="
