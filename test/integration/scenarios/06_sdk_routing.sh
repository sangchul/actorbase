#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/06_sdk_routing.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(6); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_6() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 6: SDK 라우팅 자동 갱신 (부하 중 split)"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_ps2_only; fi

  LOWER_ID=$(routing_entries | awk -F'\t' '$2!="m" {print $1; exit}')

  STRESS_LOG="$LOG_DIR/stress6.log"
  "$BIN_DIR/kv_stress" -pm "$PM_ADDR" -duration 20s >"$STRESS_LOG" 2>&1 &
  STRESS_PID=$!
  sleep 3

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

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_6
  print_summary
fi
