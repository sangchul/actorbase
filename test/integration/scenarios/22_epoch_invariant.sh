#!/usr/bin/env bash
# Standalone: bash test/integration/scenarios/22_epoch_invariant.sh
_SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then SELECTED_SCENARIOS=(22); fi
source "$_SCENARIO_DIR/../common.sh"

scenario_22() {
  log ""
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  log "시나리오 22: Epoch 불변식 shell 검증"
  log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  #
  # abctl routing의 EPOCH 컬럼($6)을 이용해 split/migrate 이후
  # entry.Epoch == RT version 불변식과 bystander epoch 보존을 shell에서 검증한다.
  #
  # 독립 실행: reset_cluster_single_ps1 + start_ps2
  reset_cluster_single_ps1

  kv_set "s22-key" "s22-val" >/dev/null
  sleep 1

  start_ps2

  # ── 검증 1: split 후 epoch 불변식 ─────────────────────────────────────────

  # We need at least 2 partitions to verify bystander invariant.
  # First split to get 2 partitions, then split one of them for the actual check.
  S22_PART1=$(partition_id_by_index 1)
  "$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$S22_PART1" g >/dev/null 2>/dev/null || true
  sleep 1

  # S22_LOWER  = upper partition [g, end): will be split at "s22" (s22 > g, so in-range).
  # S22_BYSTANDER = lower partition [start, g): untouched bystander.
  S22_LOWER=$(routing_entries | awk -F'\t' '$2=="g" {print $1; exit}')
  S22_BYSTANDER=$(routing_entries | awk -F'\t' '$2!="g" {print $1; exit}')

  if [[ -z "$S22_BYSTANDER" ]]; then
    log "  (bystander partition not found — skipping bystander epoch check)"
    S22_BYSTANDER=""
  fi

  BYSTANDER_EPOCH_BEFORE=$(partition_epoch "$S22_BYSTANDER" 2>/dev/null || echo "")
  VER_BEFORE=$(routing_version)

  # Split S22_LOWER at "s22" to create lower/upper pair.
  "$BIN_DIR/abctl" -pm "$PM_ADDR" split kv "$S22_LOWER" s22 >/dev/null 2>/dev/null || true
  sleep 1

  VER_AFTER=$(routing_version)
  assert_eq "split 후 RT version +1" "$((VER_BEFORE+1))" "$VER_AFTER"

  S22_SPLIT_LOWER_EPOCH=$(partition_epoch "$S22_LOWER")
  assert_eq "split lower epoch == new RT version" "$VER_AFTER" "$S22_SPLIT_LOWER_EPOCH"

  S22_UPPER_ID=$(routing | awk 'NR>4 && $3=="s22" {print $1}')
  if [[ -n "$S22_UPPER_ID" ]]; then
    S22_UPPER_EPOCH=$(partition_epoch "$S22_UPPER_ID")
    assert_eq "split upper epoch == new RT version" "$VER_AFTER" "$S22_UPPER_EPOCH"
  else
    fail "split upper partition (KEY-START=s22) not found in routing"
  fi

  if [[ -n "$S22_BYSTANDER" && -n "$BYSTANDER_EPOCH_BEFORE" ]]; then
    BYSTANDER_EPOCH_AFTER=$(partition_epoch "$S22_BYSTANDER")
    assert_eq "bystander epoch 불변 (split 후 false rejection 없음)" \
      "$BYSTANDER_EPOCH_BEFORE" "$BYSTANDER_EPOCH_AFTER"
  fi

  # ── 검증 2: migrate 후 epoch 불변식 ───────────────────────────────────────

  # Use s22 upper partition as target for migrate (it's on ps-1, move to ps-2).
  S22_MIGRATE_PART=""
  if [[ -n "$S22_UPPER_ID" ]]; then
    S22_MIGRATE_PART="$S22_UPPER_ID"
  else
    S22_MIGRATE_PART=$(routing_entries | awk -F'\t' '$4=="ps-1" {print $1; exit}')
  fi

  if [[ -z "$S22_MIGRATE_PART" ]]; then
    fail "migrate 대상 파티션을 찾을 수 없음"
    return
  fi

  # Bystander for migrate check: S22_LOWER (on ps-1, stays there).
  S22_MIG_BYSTANDER="$S22_LOWER"
  S22_MIG_BYSTANDER_EPOCH_BEFORE=$(partition_epoch "$S22_MIG_BYSTANDER")
  VER_BEFORE=$(routing_version)

  "$BIN_DIR/abctl" -pm "$PM_ADDR" migrate kv "$S22_MIGRATE_PART" ps-2 >/dev/null 2>/dev/null || true
  sleep 2

  # migrate: buildMigratedTable uses original rt.Version()+1, so final RT version = V+1.
  VER_AFTER=$(routing_version)
  assert_eq "migrate 후 RT version +1" "$((VER_BEFORE+1))" "$VER_AFTER"

  S22_MIG_EPOCH=$(partition_epoch "$S22_MIGRATE_PART")
  assert_eq "migrated entry epoch == new RT version" "$VER_AFTER" "$S22_MIG_EPOCH"

  S22_MIG_BYSTANDER_EPOCH_AFTER=$(partition_epoch "$S22_MIG_BYSTANDER")
  if [[ -n "$S22_MIG_BYSTANDER_EPOCH_BEFORE" ]]; then
    assert_eq "bystander epoch 불변 (migrate 후 false rejection 없음)" \
      "$S22_MIG_BYSTANDER_EPOCH_BEFORE" "$S22_MIG_BYSTANDER_EPOCH_AFTER"
  fi

  assert_eq "s22-key 데이터 무결성" "s22-val" "$(kv_get s22-key)"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  check_binaries; start_etcd; setup_backends; clean_data; start_pm
  scenario_22
  print_summary
fi
