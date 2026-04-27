#!/usr/bin/env bash
# test/integration/run.sh — actorbase 통합 시나리오 테스트 자동화
#
# 실행 방법 (프로젝트 루트에서):
#   bash test/integration/run.sh           # 전체 실행
#   bash test/integration/run.sh 12        # 시나리오 12만 실행
#   bash test/integration/run.sh 1 2 3     # 시나리오 1, 2, 3만 실행
#   bash test/integration/run.sh 12 13     # 시나리오 12, 13만 실행
#
# 개별 시나리오 직접 실행:
#   bash test/integration/scenarios/05_sigkill_failover.sh
#
# 사전 요건:
#   - etcd 바이너리가 PATH에 존재할 것 (스크립트가 직접 기동/종료)
#   - go build 완료 (bin/ 디렉토리에 바이너리 존재)
#     go build -o bin/pm ./cmd/pm
#     go build -o bin/abctl ./cmd/abctl
#     go build -o bin/kv_server ./examples/kv_server
#     go build -o bin/kv_client ./examples/kv_client
#     go build -o bin/kv_stress ./examples/kv_stress
#
# 시나리오:
#   1. 클러스터 부트스트랩
#   2. 기본 KV 동작 (set/get/del)
#   3. 파티션 Split
#   4. Scale-out + Migrate
#   5. 예기치 않은 장애 복구 (SIGKILL → 자동 Failover + WAL replay)
#   6. SDK 라우팅 자동 갱신 (부하 중 split, fail=0 검증)
#   7. Graceful Shutdown (SIGTERM → drainPartitions)
#   8. Range Scan (다중 파티션 fan-out)
#   9. PM HA Failover (standby PM이 리더를 인계받음)
#  10. Actor Eviction + Re-activation (EvictionScheduler → getOrActivate)
#  11. SDK HA Mode 자동 재발견 (etcd 모드, PM 장애 중 자동 재연결)
#  12. Multi-actor-type 동시 운영 (kv + counter 파티션 공존)
#  13. drainPartitions 타임아웃 (PM 없는 환경 → EvictAll → checkpoint 복원)
#  14. 파티션 Merge (Split → Merge → 데이터 무결성 + checkpoint 복원)
#  15. SIGKILL + 파티션 없음 → Waiting (케이스 A: node reset 불필요)
#  16. SIGKILL + 파티션 있음 + 다른 PS 없음 → 새 PS join 시 자동 재할당 (케이스 E)
#  17. SIGTERM + 파티션 있음 + 다른 PS 없음 → 다른 PS join 시 자동 재할당 (케이스 F)
#  18. Drained 상태 검증 (drain 완료 후 Drained, activate로 Active 복귀)
#  19. Restricted 상태 검증 (restrict 후 migration 거부, unrestrict 후 복귀)
#  20. PM heartbeat 기반 빠른 failover (HeartbeatTimeout 5s + WalFlushMargin 3s ≤ 10s)
#      + EvictionComplete 로그 검증 (SIGTERM 시 PS → PM WAL flush 완료 신호 전송)
#  21. PM SIGSTOP split-brain (etcd session 만료 → PM-2 리더 당선 → PM-1 SIGCONT 후 자폭)
#  22. Epoch 불변식 shell 검증 (split/migrate 후 변경 entry epoch == new RT version, bystander 불변)

set -euo pipefail

_RUN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Scenario selection must be set before sourcing common.sh.
SELECTED_SCENARIOS=("$@")

source "$_RUN_DIR/common.sh"

# Source all scenario files (defines scenario_N functions).
for _f in "$_RUN_DIR"/scenarios/[0-9][0-9]_*.sh; do
  source "$_f"
done

# ── Infrastructure ────────────────────────────────────────────────────────────
check_binaries
start_etcd
setup_backends
clean_data
start_pm

# ── Run scenarios ─────────────────────────────────────────────────────────────
should_run  1 || log "시나리오  1: SKIP"; should_run  1 && scenario_1
should_run  2 || log "시나리오  2: SKIP"; should_run  2 && scenario_2
should_run  3 || log "시나리오  3: SKIP"; should_run  3 && scenario_3
should_run  4 || log "시나리오  4: SKIP"; should_run  4 && scenario_4
should_run  5 || log "시나리오  5: SKIP"; should_run  5 && scenario_5
should_run  6 || log "시나리오  6: SKIP"; should_run  6 && scenario_6
should_run  7 || log "시나리오  7: SKIP"; should_run  7 && scenario_7
should_run  8 || log "시나리오  8: SKIP"; should_run  8 && scenario_8
should_run  9 || log "시나리오  9: SKIP"; should_run  9 && scenario_9
should_run 10 || log "시나리오 10: SKIP"; should_run 10 && scenario_10
should_run 11 || log "시나리오 11: SKIP"; should_run 11 && scenario_11
should_run 12 || log "시나리오 12: SKIP"; should_run 12 && scenario_12
should_run 13 || log "시나리오 13: SKIP"; should_run 13 && scenario_13
should_run 14 || log "시나리오 14: SKIP"; should_run 14 && scenario_14
should_run 15 || log "시나리오 15: SKIP"; should_run 15 && scenario_15
should_run 16 || log "시나리오 16: SKIP"; should_run 16 && scenario_16
should_run 17 || log "시나리오 17: SKIP"; should_run 17 && scenario_17
should_run 18 || log "시나리오 18: SKIP"; should_run 18 && scenario_18
should_run 19 || log "시나리오 19: SKIP"; should_run 19 && scenario_19
should_run 20 || log "시나리오 20: SKIP"; should_run 20 && scenario_20
should_run 21 || log "시나리오 21: SKIP"; should_run 21 && scenario_21
should_run 22 || log "시나리오 22: SKIP"; should_run 22 && scenario_22

print_summary
