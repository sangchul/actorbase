#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/16_sigkill_with_partitions.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(16); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_16() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 16: SIGKILL + 파티션 있음 + 다른 PS 없음 → 새 PS 자동 재할당 (케이스 E)"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  reset_cluster_single_ps1

  kv_set "scenario16-key" "s16-val" >/dev/null

  "$BIN_DIR/abctl" -pm "$PM_ADDR" node remove ps-2 2>/dev/null || true
  sleep 1

  kill -9 "$PS1_PID" 2>/dev/null || true
  PS1_PID=""
  log "PS-1 SIGKILL (파티션 보유). Waiting for PM heartbeat timeout + walFlushMargin (~10s)..."
  sleep 10

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-1이 Failed 상태" "Failed" "$(echo "$members" | grep ps-1 || true)"
  assert_eq "파티션은 아직 routing table에 존재 (ps-1 소유)" "1" "$(partitions_on_node ps-1)"

  "$BIN_DIR/abctl" -pm "$PM_ADDR" node add ps-2 localhost:8002 2>/dev/null || true
  "$BIN_DIR/kv_server" -node-id ps-2 -addr localhost:8002 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" >"$PS2_LOG" 2>&1 &
  PS2_PID=$!
  sleep 5

  assert_eq "파티션이 ps-2로 이동됨" "1" "$(partitions_on_node ps-2)"
  assert_eq "scenario16-key 데이터 복원 (checkpoint)" "s16-val" "$(kv_get scenario16-key)"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_16
  print_summary
fi
