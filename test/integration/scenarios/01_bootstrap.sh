#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/01_bootstrap.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(1); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_1() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 1: 클러스터 부트스트랩"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  start_ps1

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "ps-1이 active 상태로 등록됨" "ps-1" "$members"
  assert_contains "ps-1 주소가 localhost:8001" "localhost:8001" "$members"

  rt=$(routing)
  assert_eq      "라우팅 테이블 Version=1" "1" "$(echo "$rt" | awk '/^Version:/ {print $2}')"
  assert_contains "actor-type=kv 파티션 존재" "kv" "$rt"
  assert_contains "key range (start)" "(start)" "$rt"
  assert_contains "key range (end)"   "(end)"   "$rt"
  assert_eq      "초기 파티션 수=1" "1" "$(partition_count)"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_1
  print_summary
fi
