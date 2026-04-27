#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/14_merge.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(14); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_14() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 14: 파티션 Merge (Split → Merge → 데이터 무결성)"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  if [[ ${#SELECTED_SCENARIOS[@]} -gt 0 ]]; then setup_multi_actor_cluster; fi

  if [[ -z "$PS1_PID" ]] || ! kill -0 "$PS1_PID" 2>/dev/null; then
    start_ps1
  fi

  kv_set "merge-a" "val-a" >/dev/null
  kv_set "merge-b" "val-b" >/dev/null
  kv_set "merge-x" "val-x" >/dev/null
  kv_set "merge-z" "val-z" >/dev/null
  sleep 1

  local before_count
  before_count=$(kv_partition_count)
  log "현재 kv 파티션 수: $before_count"

  if [[ "$before_count" -lt 2 ]]; then
    local pid
    pid=$(kv_partition_id_by_index 1)
    "$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$pid" "merge-m" >/dev/null 2>&1
    sleep 1
  fi

  local split_count
  split_count=$(kv_partition_count)
  log "split 후 kv 파티션 수: $split_count"

  local lower_id upper_id
  lower_id=$(kv_routing_entries | awk -F'\t' '$3=="merge-m" || ($2=="(start)" && $3=="merge-m") {print $1}' | head -1)
  upper_id=$(kv_routing_entries | awk -F'\t' '$2=="merge-m" {print $1}' | head -1)

  if [[ -z "$lower_id" || -z "$upper_id" ]]; then
    lower_id=$(kv_partition_id_by_index 1)
    upper_id=$(kv_partition_id_by_index 2)
  fi

  log "merge 대상: lower=$lower_id, upper=$upper_id"

  local lower_node upper_node
  lower_node=$(kv_routing_entries | awk -F'\t' -v id="$lower_id" '$1==id {print $4}')
  upper_node=$(kv_routing_entries | awk -F'\t' -v id="$upper_id" '$1==id {print $4}')

  if [[ "$lower_node" != "$upper_node" ]]; then
    log "upper를 $lower_node으로 migrate..."
    "$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$upper_id" "$lower_node" >/dev/null 2>&1
    sleep 2
  fi

  merge_out=$("$BIN_DIR/abctl" -pm "$PM_ADDR" merge kv "$lower_id" "$upper_id" 2>/dev/null) || merge_out=""
  assert_contains "merge 명령 성공" "merge successful" "$merge_out"

  sleep 1

  local after_count
  after_count=$(kv_partition_count)
  assert_eq "merge 후 kv 파티션 수 감소" "$((split_count - 1))" "$after_count"

  assert_eq "merge 후 merge-a 조회" "val-a" "$(kv_get merge-a)"
  assert_eq "merge 후 merge-b 조회" "val-b" "$(kv_get merge-b)"
  assert_eq "merge 후 merge-x 조회" "val-x" "$(kv_get merge-x)"
  assert_eq "merge 후 merge-z 조회" "val-z" "$(kv_get merge-z)"

  kv_set "merge-new" "after-merge" >/dev/null
  assert_eq "merge 후 새 데이터 쓰기+읽기" "after-merge" "$(kv_get merge-new)"

  log "PS-1 재기동 (checkpoint 복원 검증)..."
  [[ -n "$PS1_PID" ]] && { kill "$PS1_PID" 2>/dev/null || true; PS1_PID=""; }
  sleep 3

  "$BIN_DIR/kv_server" -node-id ps-1 -addr localhost:8001 -etcd "$ETCD_ADDR" \
    "${WAL_ARGS[@]}" "${CKPT_ARGS[@]}" \
    -actor-types kv,counter >"$LOG_DIR/ps1_after_merge.log" 2>&1 &
  PS1_PID=$!
  sleep 3

  assert_eq "merge 후 재기동: merge-a 복원" "val-a" "$(kv_get merge-a)"
  assert_eq "merge 후 재기동: merge-x 복원" "val-x" "$(kv_get merge-x)"
  assert_eq "merge 후 재기동: merge-new 복원" "after-merge" "$(kv_get merge-new)"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_14
  print_summary
fi
