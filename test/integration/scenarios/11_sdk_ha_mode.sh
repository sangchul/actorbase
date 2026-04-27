#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/11_sdk_ha_mode.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(11); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_11() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 11: SDK HA Mode 자동 재발견"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then
    setup_ps2_only
    PM2_PID=""
  fi

  PM3_ADDR="localhost:8004"
  PM3_LOG="$LOG_DIR/pm3.log"

  log "PM-3 standby 기동 ($PM3_ADDR)..."
  "$BIN_DIR/pm" -addr :8004 -etcd "$ETCD_ADDR" -actor-types kv >"$PM3_LOG" 2>&1 &
  PM3_PID=$!
  sleep 2
  if ! kill -0 "$PM3_PID" 2>/dev/null; then
    fail "PM-3 프로세스 시작 실패. See $PM3_LOG"
  else
    pass "PM-3 standby 프로세스 실행 중"
  fi

  STRESS_LOG="$LOG_DIR/stress_ha.log"
  "$BIN_DIR/kv_stress" -etcd "$ETCD_ADDR" -duration 50s -interval 200ms -max-retries 10 \
    >"$STRESS_LOG" 2>&1 &
  STRESS_PID=$!
  log "kv_stress (etcd 모드) 시작. 5초 후 PM-2 종료..."
  sleep 5

  log "PM-2 SIGKILL..."
  kill -9 "$PM2_PID" 2>/dev/null || true
  PM2_PID=""
  log "PM-2 killed. PM-3 리더 승계 대기 (~18s)..."
  sleep 20

  pm3_rt=$("$BIN_DIR/abctl" -pm "$PM3_ADDR" routing 2>/dev/null || true)
  if [[ -n "$pm3_rt" ]]; then
    pass "PM-3 gRPC 서버 오픈됨 (라우팅 테이블 조회 성공)"
  else
    fail "PM-3 gRPC 서버 미오픈 (라우팅 테이블 조회 실패)"
  fi

  wait "$STRESS_PID" 2>/dev/null || true

  stress_result=$(grep "done:" "$STRESS_LOG" 2>/dev/null | tail -1 || true)
  log "kv_stress 결과: $stress_result"
  stress_fail=$(echo "$stress_result" | grep -oE 'fail=[0-9]+' | grep -oE '[0-9]+' || echo "999")
  stress_success=$(echo "$stress_result" | grep -oE 'success=[0-9]+' | grep -oE '[0-9]+' || echo "0")

  if [[ "$stress_fail" -le 30 ]]; then
    pass "SDK HA 자동 재발견: success=$stress_success fail=$stress_fail (≤30)"
  else
    fail "SDK HA 자동 재발견 실패: success=$stress_success fail=$stress_fail (>30)"
  fi

  PM_ADDR="$PM3_ADDR"
  assert_eq "HA 재발견 후 기존 데이터 접근 (apple)" "red2" "$(kv_get apple)"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_11
  print_summary
fi
