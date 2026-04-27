#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/04_scale_out_migrate.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(4); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_4() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 4: Scale-out + Migrate"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # Standalone: prepare PS1 + split(m) state (PS2 is started by this scenario).
  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_cluster_pre_migrate; fi

  start_ps2

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-2 등록됨" "ps-2" "$members"

  UPPER_ID=$(routing_entries | awk -F'\t' '$2=="m" {print $1}')
  migrate_out=$("$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$UPPER_ID" ps-2 2>/dev/null)
  assert_contains "migrate 명령 성공" "migrate successful" "$migrate_out"

  sleep 1
  assert_eq "migrate 후 Version=3"    "3" "$(routing_version)"
  assert_eq "ps-1에 파티션 1개"       "1" "$(partitions_on_node ps-1)"
  assert_eq "ps-2에 파티션 1개"       "1" "$(partitions_on_node ps-2)"
  assert_eq "migrate 후 mango 조회"  "orange" "$(kv_get mango)"
  assert_eq "migrate 후 zebra 조회"  "black"  "$(kv_get zebra)"
  assert_eq "migrate 후 apple 조회"  "red"    "$(kv_get apple)"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_4
  print_summary
fi
