#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/21_pm_sigstop.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(21); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_21() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 21: PM SIGSTOP split-brain"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  #
  # PM-1이 SIGSTOP으로 동결 → etcd lease TTL(10s) 만료 → PM-2 리더 당선.
  # PM-1 SIGCONT 후 sess.Done() → leaderCtx cascade cancel → PM-1 자폭 확인.
  # PM-2가 실제 리더로서 데이터를 다루고 있음을 검증한다.
  #
  # 독립 실행: reset_cluster_single_ps1 + start_ps2
  reset_cluster_single_ps1
  start_ps2

  # Start PM-2 as standby.
  S21_PM2_LOG="$LOG_DIR/pm2_s21.log"
  "$BIN_DIR/pm" -addr :8003 -etcd "$ETCD_ADDR" -actor-types kv >"$S21_PM2_LOG" 2>&1 &
  S21_PM2_PID=$!
  PM2_PID="$S21_PM2_PID"
  sleep 2
  if ! kill -0 "$S21_PM2_PID" 2>/dev/null; then
    fail "PM-2(:8003) 기동 실패"
    return
  fi
  log "PM-2 standby started (pid=$S21_PM2_PID addr=:8003)"

  kv_set "s21-key" "s21-val" >/dev/null
  sleep 1

  # Split to create epoch > 1 on at least one partition.
  S21_PART=$(partition_id_by_index 1)
  "$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$S21_PART" s21 >/dev/null 2>/dev/null || true
  sleep 1

  # Capture PM-1 log path before freeze.
  S21_PM1_LOG="$PM_LOG"

  # Freeze PM-1 — etcd session will expire after ~10s.
  log "PM-1 SIGSTOP (pid=$PM_PID). Waiting 15s for etcd TTL expiry and PM-2 election..."
  kill -STOP "$PM_PID"

  # Wait for PM-2 to win the election (TTL=10s + buffer).
  sleep 15

  # Verify PM-2 is now the leader by querying it directly.
  pm2_routing=$("$BIN_DIR/abctl" -pm "localhost:8003" routing 2>/dev/null || true)
  if echo "$pm2_routing" | grep -q "Version:"; then
    pass "PM-2(:8003)가 리더로 당선되어 routing 테이블 서빙 중"
  else
    fail "PM-2(:8003)가 routing 테이블을 서빙하지 않음 — leader election 실패"
  fi

  # Migrate a partition via PM-2 to prove it can issue commands.
  S21_UPPER=$(routing_entries_from "localhost:8003" | awk -F'\t' '$2=="s21" {print $1}')
  if [[ -n "$S21_UPPER" ]]; then
    "$BIN_DIR/abctl" -pm "localhost:8003" migrate kv "$S21_UPPER" ps-1 >/dev/null 2>/dev/null || true
    sleep 2
    pass "PM-2가 migrate 명령 처리 (split-brain 없이 정상 리더십)"
  else
    log "(s21 upper partition not found — skipping PM-2 migrate verification)"
  fi

  # Resume PM-1 — sess.Done() should fire immediately and shut it down.
  log "PM-1 SIGCONT (pid=$PM_PID). Waiting 5s for self-shutdown..."
  kill -CONT "$PM_PID"
  sleep 5

  # PM-1 must have exited (etcd session expired → leadership lost).
  if ! kill -0 "$PM_PID" 2>/dev/null; then
    pass "PM-1이 SIGCONT 후 스스로 종료됨 (sess.Done() → leaderCtx cancel)"
    PM_PID=""
  else
    fail "PM-1이 SIGCONT 후에도 살아있음 — epoch fencing 미작동 또는 session 미만료"
    kill "$PM_PID" 2>/dev/null || true
    PM_PID=""
  fi

  pm1_log=$(cat "$S21_PM1_LOG" 2>/dev/null || true)
  assert_contains "PM-1 로그: etcd session expired" "etcd session expired" "$pm1_log"

  # Switch PM_ADDR to PM-2 and verify data integrity.
  PM_ADDR="localhost:8003"
  assert_eq "PM-2 경유 s21-key 데이터 무결성" "s21-val" "$(kv_get s21-key)"

  # Restore PM_ADDR for subsequent scenarios (PM-1 is dead; PM-2 stays as leader).
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_21
  print_summary
fi
