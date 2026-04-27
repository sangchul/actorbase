#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/18_drained_state.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(18); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_18() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 18: Drained 경로 검증 (notifyDrained → isDrained → Waiting)"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  #
  # 전제: PS1 단독 운영, 파티션 보유
  # 흐름:
  #   1. PS2 기동 (drain target 확보)
  #   2. PS1 SIGTERM → notifyDraining → drainPartitions → notifyDrained → EvictAll → Deregister
  #   3. PM handleNodeLeft: isDrained=true → Waiting (not Failed)
  #
  # 주의: Drained 상태는 notifyDrained 호출~Deregister 완료 사이의 sub-second 과도 상태.
  #       polling으로 포착하기 어려우므로 대신 로그로 검증한다.
  #   - PS1 로그: "notified PM of drain completion"
  #   - PM 로그:  "returned to Waiting after completed drain"
  #   - 최종 상태: Waiting (not Failed)
  #
  reset_cluster_single_ps1
  start_ps2

  kv_set "s18-key" "s18-val" >/dev/null

  # SIGTERM PS1 — drain to PS2, then notifyDrained, EvictAll, exit.
  kill "$PS1_PID" 2>/dev/null || true
  log "PS-1 SIGTERM sent. Waiting for drain + notifyDrained + exit (~8s)..."
  sleep 8
  PS1_PID=""

  # Verify via logs that the Drained path was taken.
  ps1_log=$(cat "$PS1_LOG" 2>/dev/null || true)
  assert_contains "PS1 로그: PM에 drain 완료 통보" "notified PM of drain completion" "$ps1_log"
  assert_contains "PS1 로그: PM에 eviction 완료 신호 전송" "notified PM of eviction completion" "$ps1_log"

  pm_log=$(cat "$PM_LOG" 2>/dev/null || true)
  assert_contains "PM 로그: drain 완료 후 Waiting 복귀 (isDrained 경로)" "returned to Waiting after completed drain" "$pm_log"

  # Final state must be Waiting (not Failed): isDrained branch skips failoverDeadNode.
  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-1이 Waiting 상태 (Drained 경로 후 자동 복귀)" "Waiting" "$(echo "$members" | grep ps-1 || true)"
  assert_not_contains "ps-1이 Failed가 아님 (Drained 경로 성공)" "Failed" "$(echo "$members" | grep ps-1 || true)"

  assert_eq "s18-key 데이터 PS2에서 접근 가능" "s18-val" "$(kv_get s18-key)"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_18
  print_summary
fi
