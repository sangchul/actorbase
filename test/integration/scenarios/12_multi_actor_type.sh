#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/12_multi_actor_type.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(12); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_12() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 12: Multi-actor-type 동시 운영"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  log "기존 클러스터 정리..."
  [[ -n "$PM3_PID" ]] && { kill "$PM3_PID" 2>/dev/null || true; PM3_PID=""; }
  [[ -n "$PM2_PID" ]] && { kill "$PM2_PID" 2>/dev/null || true; PM2_PID=""; }
  [[ -n "$PM_PID"  ]] && { kill "$PM_PID"  2>/dev/null || true; PM_PID="";  }
  [[ -n "$PS2_PID" ]] && { kill "$PS2_PID" 2>/dev/null || true; PS2_PID=""; }
  [[ -n "$PS1_PID" ]] && { kill "$PS1_PID" 2>/dev/null || true; PS1_PID=""; }
  sleep 3

  etcdctl --endpoints="$ETCD_ADDR" del /actorbase/ --prefix >/dev/null 2>&1 || true
  if [[ "$WAL_BACKEND" == "redis" ]]; then
    redis-cli -u "redis://$REDIS_ADDR" FLUSHDB >/dev/null 2>&1 || true
  else
    rm -rf "$WAL_DIR"
  fi
  rm -rf "$CKPT_DIR"
  mkdir -p "$WAL_DIR" "$CKPT_DIR"

  log "PM 재기동 with -actor-types kv,counter..."
  PM_ADDR="localhost:8000"
  "$BIN_DIR/pm" -addr :8000 -etcd "$ETCD_ADDR" -actor-types kv,counter \
    >"$LOG_DIR/pm_multi.log" 2>&1 &
  PM_PID=$!
  sleep 1

  log "PS-1 재기동 with -actor-types kv,counter..."
  "$BIN_DIR/abctl" -pm "$PM_ADDR" node add ps-1 localhost:8001 2>/dev/null || true
  "$BIN_DIR/kv_server" -node-id ps-1 -addr localhost:8001 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" \
    -actor-types kv,counter >"$LOG_DIR/ps1_multi.log" 2>&1 &
  PS1_PID=$!
  sleep 4

  rt_multi=$(routing)
  assert_contains "multi-type: kv 파티션 존재"     "kv"      "$rt_multi"
  assert_contains "multi-type: counter 파티션 존재" "counter" "$rt_multi"

  kv_count=$(routing_entries | awk -F'\t' 'BEGIN{c=0} {c++} END{print c+0}')
  if [[ "$kv_count" -ge 1 ]]; then
    pass "kv 타입 라우팅 항목 ${kv_count}개 이상 존재"
  else
    fail "kv 타입 라우팅 항목 없음"
  fi

  kv_set "mt-key" "mt-value" >/dev/null
  assert_eq "multi-type: kv set/get 정상" "mt-value" "$(kv_get mt-key)"

  kv_part_id=$("$BIN_DIR/abctl" -pm "$PM_ADDR" routing 2>/dev/null \
    | awk 'NR>4 && $1!="" && $1!~"^-" && $2=="kv" {print $1; exit}')
  if [[ -n "$kv_part_id" ]]; then
    rt_ver_before=$(routing_version)
    "$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$kv_part_id" m >/dev/null 2>&1 || true
    sleep 2
    rt_ver_after=$(routing_version)
    if [[ "$rt_ver_after" -gt "$rt_ver_before" ]]; then
      pass "split 후 라우팅 버전 증가 (${rt_ver_before}→${rt_ver_after})"
    else
      fail "split 후 라우팅 버전 미증가"
    fi
    rt_after_split=$(routing)
    assert_contains "split 후 kv 파티션 유지"     "kv"      "$rt_after_split"
    assert_contains "split 후 counter 파티션 유지" "counter" "$rt_after_split"
  else
    fail "kv 파티션 ID를 가져오지 못함"
  fi
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_12
  print_summary
fi
