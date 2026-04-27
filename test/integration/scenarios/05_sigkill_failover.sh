#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/05_sigkill_failover.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(5); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_5() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 5: SIGKILL → 자동 Failover + WAL replay"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # Standalone: prepare PS1[start,m) + PS2[m,end) + basic data.
  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_split_cluster; fi

  # Data written after checkpoint (WAL-only).
  kv_set apple  red2  >/dev/null
  kv_set avocado green >/dev/null
  kv_set cherry  red3  >/dev/null

  kill -9 "$PS1_PID" 2>/dev/null || true
  PS1_PID=""
  log "PS-1 killed. Waiting for PM heartbeat timeout + walFlushMargin (~10s)..."
  sleep 10

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-1이 Failed 상태 (SIGKILL)" "Failed" "$(echo "$members" | grep ps-1 || true)"
  assert_contains "ps-2가 active 상태"           "ps-2"   "$members"

  assert_eq "failover 후 Version=4"         "4"  "$(routing_version)"
  assert_eq "하위 파티션이 ps-2로 이동됨"   "2"  "$(partitions_on_node ps-2)"

  assert_eq "banana (checkpoint 복원)" "yellow" "$(kv_get banana)"
  assert_eq "mango  (checkpoint 복원)" "orange" "$(kv_get mango)"

  assert_eq "apple  (WAL replay: red2, not red)" "red2"  "$(kv_get apple)"
  assert_eq "avocado (WAL replay)"               "green" "$(kv_get avocado)"
  assert_eq "cherry  (WAL replay)"               "red3"  "$(kv_get cherry)"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_5
  print_summary
fi
