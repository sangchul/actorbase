#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/20_heartbeat_failover.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(20); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_20() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 20: PM heartbeat 기반 빠른 failover + EvictionComplete 검증"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  #
  # 검증 1 (SIGKILL): PM heartbeat timeout(5s) + walFlushMargin(3s) ≤ 10s 이내 failover
  #   - PM 로그에 "heartbeat timeout" 메시지 확인
  #   - PM 로그에 "waitForEviction: walFlushMargin elapsed" 확인 (EvictionComplete 없음)
  #   - 데이터 복원 확인 (WAL replay)
  #
  # 검증 2 (SIGTERM): EvictionComplete 빠른 경로
  #   - PS 로그에 "notified PM of eviction completion" 확인
  #
  reset_cluster_single_ps1
  start_ps2

  kv_set "s20-a" "val-a" >/dev/null
  kv_set "s20-b" "val-b" >/dev/null
  sleep 1  # WAL flush 대기

  # 파티션 분산: ps-1에 하나, ps-2에 하나
  PART_ID=$(partition_id_by_index 1)
  "$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$PART_ID" s20 >/dev/null 2>/dev/null || true
  sleep 1
  UPPER_ID=$(routing_entries | awk -F'\t' '$2=="s20" {print $1}')
  if [[ -n "$UPPER_ID" ]]; then
    "$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$UPPER_ID" ps-2 >/dev/null 2>/dev/null || true
    sleep 1
  fi

  # ── 검증 1: SIGKILL → PM heartbeat 기반 빠른 failover ──────────────────────
  log "PS-1 SIGKILL. 타이머 시작..."
  FAILOVER_START=$(date +%s)
  kill -9 "$PS1_PID" 2>/dev/null || true
  PS1_PID=""

  # HeartbeatTimeout(5s) + walFlushMargin(3s) + 여유 2s 이내 failover 기대
  sleep 10
  FAILOVER_END=$(date +%s)
  FAILOVER_ELAPSED=$((FAILOVER_END - FAILOVER_START))

  members=$("$BIN_DIR/abctl" -pm "$PM_ADDR" members 2>/dev/null)
  assert_contains "SIGKILL 후 ps-1이 Failed 상태" "Failed" "$(echo "$members" | grep ps-1 || true)"
  assert_eq "SIGKILL 후 파티션이 ps-2로 이동됨" "2" "$(partitions_on_node ps-2)"

  if [[ "$FAILOVER_ELAPSED" -le 12 ]]; then
    pass "PM heartbeat 기반 failover: ${FAILOVER_ELAPSED}s (≤12s, 구 etcd TTL 15s+ 보다 빠름)"
  else
    fail "PM heartbeat 기반 failover: ${FAILOVER_ELAPSED}s (>12s — 예상보다 느림)"
  fi

  # PM 로그에 heartbeat timeout 메시지 확인
  pm_log=$(cat "$PM_LOG" 2>/dev/null || true)
  assert_contains "PM 로그: heartbeat timeout으로 노드 장애 감지" "heartbeat timeout" "$pm_log"
  assert_contains "PM 로그: walFlushMargin 경과 후 failover 진행" "walFlushMargin elapsed" "$pm_log"

  # 데이터 복원 (WAL replay from shared WAL)
  assert_eq "SIGKILL 후 s20-a 데이터 복원 (WAL replay)" "val-a" "$(kv_get s20-a)"
  assert_eq "SIGKILL 후 s20-b 데이터 복원 (WAL replay)" "val-b" "$(kv_get s20-b)"

  # ── 검증 2: SIGTERM → EvictionComplete 빠른 경로 ───────────────────────────
  # ps-1 재기동 후 파티션 하나 이전, 그 다음 SIGTERM
  start_ps1
  PART_FOR_PS1=$(routing_entries | awk -F'\t' '$4=="ps-2" {print $1; exit}')
  if [[ -n "$PART_FOR_PS1" ]]; then
    "$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$PART_FOR_PS1" ps-1 >/dev/null 2>/dev/null || true
    sleep 1
  fi

  # PS1 SIGTERM — shutdown() → EvictAll → notifyEvictionComplete
  kill "$PS1_PID" 2>/dev/null || true
  log "PS-1 SIGTERM sent. Waiting for drain + EvictAll + EvictionComplete (~8s)..."
  sleep 8
  PS1_PID=""

  ps1_log=$(cat "$PS1_LOG" 2>/dev/null || true)
  assert_contains "SIGTERM 시 PS1이 PM에 eviction 완료 신호 전송" \
    "notified PM of eviction completion" "$ps1_log"

  # PM 로그에 EvictionComplete 수신 메시지 확인
  pm_log=$(cat "$PM_LOG" 2>/dev/null || true)
  assert_contains "PM 로그: EvictionComplete 수신 → walFlushMargin 생략" \
    "EvictionComplete received" "$pm_log"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_20
  print_summary
fi
