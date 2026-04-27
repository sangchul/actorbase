#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/03_split.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(3); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_3() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 3: 파티션 Split"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_basic_cluster; fi

  kv_set apple  red    >/dev/null
  kv_set banana yellow >/dev/null
  kv_set mango  orange >/dev/null
  kv_set zebra  black  >/dev/null

  PARTITION_ID=$(partition_id_by_index 1)
  split_out=$("$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$PARTITION_ID" m 2>/dev/null)
  assert_contains "split 명령 성공" "split successful" "$split_out"

  sleep 1
  rt=$(routing)
  assert_eq      "split 후 파티션 수=2"     "2" "$(partition_count)"
  assert_eq      "split 후 Version=2"       "2" "$(routing_version)"
  assert_contains "하위 파티션 key-end=m"   "m"        "$rt"
  assert_contains "상위 파티션 key-start=m" "m"        "$rt"

  assert_eq "apple (< m) 조회" "red"    "$(kv_get apple)"
  assert_eq "mango (>= m) 조회" "orange" "$(kv_get mango)"
  assert_eq "zebra (>= m) 조회" "black"  "$(kv_get zebra)"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_3
  print_summary
fi
