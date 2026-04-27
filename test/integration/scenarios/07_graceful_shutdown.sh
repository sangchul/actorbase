#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/07_graceful_shutdown.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(7); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_7() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 7: Graceful Shutdown (SIGTERM → drain)"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then
    setup_ps2_only
    kv_set apple  red2   >/dev/null
    kv_set mango  orange >/dev/null
  fi

  start_ps1

  SOME_ID=$(routing_entries | awk -F'\t' '$4=="ps-2" {print $1; exit}')
  "$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$SOME_ID" ps-1 >/dev/null 2>/dev/null || true
  sleep 1

  partitions_before=$(partitions_on_node ps-1)

  STRESS7_LOG="$LOG_DIR/stress7.log"
  "$BIN_DIR/kv_stress" -pm "$PM_ADDR" -duration 20s >"$STRESS7_LOG" 2>&1 &
  STRESS7_PID=$!
  sleep 2

  kill "$PS1_PID" 2>/dev/null || true
  log "PS-1 SIGTERM sent. Waiting for drain..."
  sleep 6

  drain_log=$(grep "drain" "$PS1_LOG" 2>/dev/null || true)
  assert_contains "drain 완료 로그 출력됨" "drain: partition migrated" "$drain_log"

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-1이 Waiting 상태 (SIGTERM 후 drain)" "Waiting" "$(echo "$members" | grep ps-1 || true)"

  assert_eq "drain 후 ps-2에 모든 파티션" "$(partition_count)" "$(partitions_on_node ps-2)"

  assert_eq "apple 조회 (drain 후)"  "red2"  "$(kv_get apple)"
  assert_eq "mango 조회 (drain 후)"  "orange" "$(kv_get mango)"

  wait "$STRESS7_PID" 2>/dev/null || true
  PS1_PID=""
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_7
  print_summary
fi
