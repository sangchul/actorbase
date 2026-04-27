#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/08_range_scan.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(8); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_8() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 8: Range Scan (다중 파티션 fan-out)"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # Standalone: prepare 3 partitions [start,f), [f,m), [m,end) on PS2.
  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_three_partitions_ps2; fi

  kv_set peach pink >/dev/null
  kv_set quince yellow2 >/dev/null

  scan_all=$(kv_scan "" "")
  assert_contains "scan 전체: apple 포함"   "apple"   "$scan_all"
  assert_contains "scan 전체: banana 포함"  "banana"  "$scan_all"
  assert_contains "scan 전체: mango 포함"   "mango"   "$scan_all"
  assert_contains "scan 전체: zebra 포함"   "zebra"   "$scan_all"
  assert_contains "scan 전체: peach 포함"   "peach"   "$scan_all"
  assert_contains "scan 전체: quince 포함"  "quince"  "$scan_all"

  scan_upper=$(kv_scan "m" "")
  assert_contains     "scan [m,): mango 포함"  "mango"  "$scan_upper"
  assert_contains     "scan [m,): zebra 포함"  "zebra"  "$scan_upper"
  assert_not_contains "scan [m,): apple 제외"  "apple"  "$scan_upper"
  assert_not_contains "scan [m,): banana 제외" "banana" "$scan_upper"

  scan_lower=$(kv_scan "a" "m")
  assert_contains     "scan [a,m): apple 포함"   "apple"   "$scan_lower"
  assert_contains     "scan [a,m): banana 포함"  "banana"  "$scan_lower"
  assert_contains     "scan [a,m): cherry 포함"  "cherry"  "$scan_lower"
  assert_not_contains "scan [a,m): mango 제외"   "mango"   "$scan_lower"
  assert_not_contains "scan [a,m): zebra 제외"   "zebra"   "$scan_lower"

  assert_contains "scan 전체: apple=red2"   "red2"   "$scan_all"
  assert_contains "scan 전체: mango=orange" "orange" "$scan_all"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_8
  print_summary
fi
