#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/10_eviction_reactivation.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(10); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_10() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 10: Actor Eviction + Re-activation"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_ps2_only; fi

  log "PS-2 재기동 (idle-timeout=5s, evict-interval=2s)..."
  [[ -n "$PS2_PID" ]] && kill "$PS2_PID" 2>/dev/null || true
  PS2_PID=""
  sleep 5

  "$BIN_DIR/kv_server" -node-id ps-2 -addr localhost:8002 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" \
    -idle-timeout 5s -evict-interval 2s >"$LOG_DIR/ps2_evict.log" 2>&1 &
  PS2_PID=$!
  sleep 3

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-2 재기동 후 members에 존재" "ps-2" "$members"

  kv_set "evict-key" "evict-value" >/dev/null
  sleep 1

  stats_after_set=$("$BIN_DIR/abctl" -pm "$PM_ADDR" stats 2>/dev/null)
  ps2_active_set=$(echo "$stats_after_set" | awk '/^ps-2/ && $4 != "-" {count++} END {print count+0}')
  if [[ "$ps2_active_set" -gt 0 ]]; then
    pass "set 직후 actor active (ps-2 partitions=$ps2_active_set)"
  else
    fail "set 직후 actor active 확인 실패 (ps-2 partitions=$ps2_active_set)"
  fi

  log "EvictionScheduler 대기 (10s)..."
  sleep 10

  stats_after_evict=$("$BIN_DIR/abctl" -pm "$PM_ADDR" stats 2>/dev/null)
  ps2_active_evict=$(echo "$stats_after_evict" | awk '/^ps-2/ && $4 != "-" {count++} END {print count+0}')
  assert_eq "EvictionScheduler 동작: actor evict됨 (partitions=0)" "0" "$ps2_active_evict"

  val=$(kv_get "evict-key")
  assert_eq "re-activation 후 값 복원 (evict-value)" "evict-value" "$val"
  sleep 1

  stats_after_get=$("$BIN_DIR/abctl" -pm "$PM_ADDR" stats 2>/dev/null)
  ps2_active_get=$(echo "$stats_after_get" | awk '/^ps-2/ && $4 != "-" {count++} END {print count+0}')
  if [[ "$ps2_active_get" -gt 0 ]]; then
    pass "re-activation 후 actor active 복원 (ps-2 partitions=$ps2_active_get)"
  else
    fail "re-activation 후 actor active 복원 실패 (ps-2 partitions=$ps2_active_get)"
  fi
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_10
  print_summary
fi
