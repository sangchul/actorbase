#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/02_kv_crud.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(2); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_2() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 2: 기본 KV 동작 (set/get/del)"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_basic_cluster; fi

  assert_eq "set user:1001" "ok"             "$(kv_set user:1001 '{"name":"alice"}')"
  assert_eq "get user:1001" '{"name":"alice"}' "$(kv_get user:1001)"
  assert_eq "del user:1001" "ok"             "$(kv_del user:1001)"
  assert_eq "get after del (not found)" ""  "$(kv_get user:1001)"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_2
  print_summary
fi
