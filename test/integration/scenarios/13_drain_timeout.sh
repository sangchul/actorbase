#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/13_drain_timeout.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(13); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_13() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 13: drainPartitions 타임아웃"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_multi_actor_cluster; fi

  kv_set "drain-key-1" "drain-val-1" >/dev/null
  kv_set "drain-key-2" "drain-val-2" >/dev/null
  sleep 1

  log "PS-1 재기동 (drain-timeout=3s)..."
  [[ -n "$PS1_PID" ]] && { kill "$PS1_PID" 2>/dev/null || true; PS1_PID=""; }
  sleep 2

  "$BIN_DIR/kv_server" -node-id ps-1 -addr localhost:8001 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" \
    -actor-types kv,counter -drain-timeout 3s \
    >"$LOG_DIR/ps1_drain.log" 2>&1 &
  PS1_PID=$!
  sleep 3

  assert_eq "drain-timeout PS 재기동 후 데이터 확인" "drain-val-1" "$(kv_get drain-key-1)"

  log "PM 종료 (drain 실패 유도)..."
  [[ -n "$PM_PID" ]] && { kill "$PM_PID" 2>/dev/null || true; PM_PID=""; }
  sleep 1

  log "PS-1 SIGTERM → drain timeout(3s) + EvictAll 대기..."
  kill -TERM "$PS1_PID" 2>/dev/null || true
  sleep 10
  PS1_PID=""

  ps1_drain_log=$(cat "$LOG_DIR/ps1_drain.log" 2>/dev/null || true)
  if echo "$ps1_drain_log" | grep -qi "drain\|shutdown\|evict"; then
    pass "PS-1 로그에서 drain/shutdown/evict 관련 메시지 확인"
  else
    fail "PS-1 로그에 drain/shutdown 메시지 없음"
  fi

  log "PM + PS-1 재기동 (checkpoint 복원 확���)..."
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

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_13
  print_summary
fi
