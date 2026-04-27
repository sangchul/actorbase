#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/09_pm_ha_failover.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(9); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_9() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 9: PM HA Failover"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_ps2_only; fi

  kv_set "ha_test" "before_failover" >/dev/null

  log "Starting PM-2 as standby ($PM2_ADDR)..."
  "$BIN_DIR/pm" -addr :8003 -etcd "$ETCD_ADDR" -actor-types kv >"$PM2_LOG" 2>&1 &
  PM2_PID=$!
  sleep 2
  if ! kill -0 "$PM2_PID" 2>/dev/null; then
    fail "PM-2 process died immediately. See $PM2_LOG"
  else
    pass "PM-2 standby 프로세스 실행 중"
  fi

  if "$BIN_DIR/abctl" -pm "$PM2_ADDR" routing >/dev/null 2>&1; then
    fail "PM-2가 standby인데 gRPC 포트가 열림 (예상: 실패)"
  else
    pass "PM-2 standby 상태: gRPC 포트 미오픈 확인"
  fi

  log "Killing PM-1 (SIGKILL)..."
  kill -9 "$PM_PID" 2>/dev/null || true
  PM_PID=""
  log "PM-1 killed. Waiting for etcd lease expiry (~15s) and PM-2 election..."
  sleep 18

  pm2_rt=$("$BIN_DIR/abctl" -pm "$PM2_ADDR" routing 2>/dev/null || true)
  if [[ -n "$pm2_rt" ]]; then
    pass "PM-2 gRPC 서버 오픈됨 (라우팅 테이블 조회 성공)"
  else
    fail "PM-2 gRPC 서버 미오픈 (라우팅 테이블 조회 실패)"
  fi

  assert_contains "PM-2 라우팅 테이블: kv 파티션 존재" "kv" "$pm2_rt"

  pm2_log_leader=$(grep "elected as leader" "$PM2_LOG" 2>/dev/null || true)
  assert_contains "PM-2 로그: 리더 선출 확인" "elected as leader" "$pm2_log_leader"

  PM_ADDR="$PM2_ADDR"
  assert_eq "failover 후 ha_test 조회" "before_failover" "$(kv_get ha_test)"
  assert_eq "failover 후 기존 데이터 접근 (apple)" "red2" "$(kv_get apple)"

  kv_set "ha_test2" "after_failover" >/dev/null
  assert_eq "failover 후 신규 데이터 저장/조회" "after_failover" "$(kv_get ha_test2)"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_9
  print_summary
fi
