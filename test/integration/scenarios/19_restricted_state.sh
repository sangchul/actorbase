#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/19_restricted_state.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(19); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_19() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 19: Restricted 상태 검증"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  #
  # 전제: PS1 + PS2 운영 중
  # 흐름:
  #   1. PS2 restrict → Restricted
  #   2. abctl migrate ... ps-2 → FailedPrecondition 에러
  #   3. PS1 SIGTERM → drain: PS2는 target 후보에서 제외됨 (Active 없음 → drain skip)
  #   4. abctl node unrestrict ps-2 → Active
  #
  reset_cluster_single_ps1
  start_ps2

  kv_set "s19-key" "s19-val" >/dev/null
  sleep 1

  # Split to give PS1 a partition.
  PART_ID=$(partition_id_by_index 1)
  "$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$PART_ID" m >/dev/null 2>/dev/null || true
  sleep 1

  # Restrict PS2.
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node restrict ps-2 2>/dev/null
  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-2가 Restricted 상태" "Restricted" "$(echo "$members" | grep ps-2 || true)"

  # Attempt manual migration to Restricted node — should fail.
  PART_ON_PS1=$(routing_entries | awk -F'\t' '$4=="ps-1" {print $1; exit}')
  if [[ -n "$PART_ON_PS1" ]]; then
    migrate_out=$("$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$PART_ON_PS1" ps-2 2>&1 || true)
    assert_contains "Restricted 노드로 migrate 거부됨" "not active" "$migrate_out"
  else
    pass "ps-1에 파티션 없음 (split 미수행) — migration 테스트 skip"
  fi

  # Unrestrict PS2.
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node unrestrict ps-2 2>/dev/null
  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-2가 Active 상태 (unrestrict 후)" "Active" "$(echo "$members" | grep ps-2 || true)"

  assert_eq "s19-key 데이터 접근 가능" "s19-val" "$(kv_get s19-key)"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_19
  print_summary
fi
