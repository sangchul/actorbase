#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/15_sigkill_no_partitions.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(15); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_15() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 15: SIGKILL + 파티션 없음 → Waiting (케이스 A)"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  reset_cluster_single_ps1

  start_ps2
  sleep 2

  for pid in $(routing_entries | awk -F'\t' '$4=="ps-1" {print $1}'); do
    "$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$pid" ps-2 >/dev/null 2>/dev/null || true
    sleep 1
  done
  sleep 1

  assert_eq "PS1 파티션 수=0 (모두 PS2로 이전)" "0" "$(partitions_on_node ps-1)"

  kill -9 "$PS1_PID" 2>/dev/null || true
  PS1_PID=""
  log "PS-1 SIGKILL. Waiting for PM heartbeat timeout (~8s)..."
  sleep 8

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-1이 Waiting 상태 (파티션 없음 + SIGKILL)" "Waiting" "$(echo "$members" | grep ps-1 || true)"
  assert_not_contains "ps-1이 Failed가 아님" "Failed" "$(echo "$members" | grep ps-1 || true)"

  "$BIN_DIR/abctl" -pm "$PM_ADDR" node add ps-1 localhost:8001 2>/dev/null || true
  "$BIN_DIR/kv_server" -node-id ps-1 -addr localhost:8001 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" >"$PS1_LOG" 2>&1 &
  PS1_PID=$!
  sleep 3

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-1이 재기동 후 Active (node reset 없이)" "Active" "$(echo "$members" | grep ps-1 || true)"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_15
  print_summary
fi
