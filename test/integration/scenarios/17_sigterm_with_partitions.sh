#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/17_sigterm_with_partitions.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(17); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_17() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 17: SIGTERM + 파티션 있음 + 다른 PS 없음 → 다른 PS join 시 자동 재할당 (케이스 F)"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  #
  # 전제: PS1 단독 운영, 파티션 보유 상태에서 SIGTERM
  #   → drain target 없으므로 skip → EvictAll → checkpoint 저장 → Waiting
  # 기대: PS2가 join하면 PM이 recoverOrphanedPartitions로 PS1의 Waiting 파티션을 재할당
  #
  reset_cluster_single_ps1

  kv_set "scenario17-key" "s17-val" >/dev/null

  # Ensure PS2 is not registered.
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node remove ps-2 2>/dev/null || true
  sleep 1

  # SIGTERM PS1.
  kill "$PS1_PID" 2>/dev/null || true
  log "PS-1 SIGTERM sent. Waiting for drain + EvictAll (~10s)..."
  sleep 10
  PS1_PID=""

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-1이 Waiting 상태 (SIGTERM)" "Waiting" "$(echo "$members" | grep ps-1 || true)"
  assert_eq "파티션이 아직 ps-1 소유" "1" "$(partitions_on_node ps-1)"

  # Start PS2 (without resetting PS1) — PM should recover orphaned partitions.
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node add ps-2 localhost:8002 2>/dev/null || true
  "$BIN_DIR/kv_server" -node-id ps-2 -addr localhost:8002 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" >"$PS2_LOG" 2>&1 &
  PS2_PID=$!
  sleep 5  # Allow recoverOrphanedPartitions to complete

  assert_eq "파티션이 ps-2로 이동됨" "1" "$(partitions_on_node ps-2)"
  assert_eq "scenario17-key 데이터 복원 (checkpoint)" "s17-val" "$(kv_get scenario17-key)"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_17
  print_summary
fi
