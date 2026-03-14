# Auto Balancer 설계

PM이 주기적으로 클러스터 상태를 수집하고, 정책 기반으로 파티션 split과 migrate를 자동 수행하는 기능.

---

## 요구사항 (인터뷰 기반)

### 트리거 기준

세 가지 지표를 모두 사용한다.

- **파티션 RPS**: 파티션이 받는 초당 요청 수
- **파티션 key 수**: 파티션 내 저장된 key 개수
- **노드 간 부하 균형**: 노드 간 파티션 수 차이, RPS 차이

### 메트릭 수집

PM이 주기적으로 모든 PS에 `GetStats` RPC를 호출하여 stats를 수집한다. PS는 노드 단위 stats와 파티션 단위 stats를 함께 반환한다. 개별 PS의 stats는 `abctl stats [node-id]`로 운영자가 직접 조회할 수 있다.

### 정책 설정

YAML 파일로 정책을 기술한다. `abctl policy apply policy.yaml`로 PM에 반영하며, etcd에 영속화되어 PM 재시작 후에도 유지된다. `abctl policy get`으로 현재 적용 중인 정책을 조회할 수 있다.

YAML 최상단에 `algorithm` 필드를 명시하여, 향후 다른 알고리즘이 추가될 때 같은 인터페이스로 교체 가능하도록 확장성을 확보한다.

```yaml
algorithm: threshold   # 향후 다른 알고리즘 추가 시 교체

check_interval: 30s

cooldown:
  global: 60s       # 마지막 split/migrate 후 전체 대기
  partition: 120s   # 파티션별 개별 대기

split:
  rps_threshold: 1000
  key_threshold: 10000

balance:
  max_partition_diff: 2     # 노드 간 파티션 수 허용 차이
  rps_imbalance_pct: 30     # 노드 간 RPS 차이 허용 %
```

임계값은 런타임에 동적으로 변경 가능하다 (`abctl policy apply`로 재반영).

### split key 결정

자동 split 시 split key는 파티션 key 범위의 midpoint로 결정한다.

### 수동/자동 전환 및 배타적 운영

**policy 유무가 곧 모드**다.

- `abctl policy apply policy.yaml` → AutoPolicy 활성화
- `abctl policy clear` → policy 제거, ManualPolicy로 복귀
- PM 시작 시 etcd에 저장된 policy가 있으면 자동으로 AutoPolicy 복원

AutoPolicy 활성 중에는 `abctl split`, `abctl migrate` 같은 수동 명령을 거부한다. 운영자가 수동으로 개입하려면 `abctl policy clear`로 먼저 AutoPolicy를 해제해야 한다.

AutoPolicy에서는 split과 migrate가 모두 자동으로 동작한다.

---

## 전체 구조

```
[PS] engine → 파티션별 stats 집계
  ↑ GetStats RPC (check_interval마다 PM이 poll)
[PM] AutoBalancer → split/migrate 판단 → RequestSplit/RequestMigrate 재활용
  ↕ etcd
[PM] policy 저장/로드 (ApplyPolicy / GetPolicy RPC)
  ↑
[abctl] stats / policy apply / policy get
```

---

## 작업 계획

### Phase 1 — Stats 수집 인프라

**engine: 통계 수집**

- `internal/engine/stats.go` 신규: `PartitionStats{KeyCount, RPS}` 타입 정의
- `ActorHost`에 파티션별 key count 추적 (Receive 호출 시 증감)
- RPS: 슬라이딩 윈도우 카운터 (최근 N초 평균)
- `ActorHost.GetStats() NodeStats` 메서드 추가

**proto: GetStats RPC 추가**

`PartitionServerService`에 `GetStats` RPC 추가.

- 요청: 없음 (노드 전체 반환)
- 응답: `NodeStats` — node_id, node_rps, partition_count, 파티션별 `PartitionStats` 목록
- `PartitionStats` 필드: partition_id, actor_type, key_count, rps

**PS: GetStats 핸들러**

`ps/server.go`에 `GetStats` 구현. 등록된 각 dispatcher에서 stats를 수집하여 집계 후 반환.

**transport: PMClient에 GetStats 추가**

`internal/transport/client.go`에 `GetStats(nodeAddr) (NodeStats, error)` 추가.

**abctl: stats 명령**

```bash
abctl stats           # 모든 노드 요약 (노드별 파티션 수, RPS)
abctl stats ps-1      # 특정 노드 상세 (파티션별 key_count, RPS)
```

---

### Phase 2 — Policy YAML 정의 및 저장

**policy 타입 정의**

`pm/policy/threshold.go`에 `ThresholdConfig` 구조체 정의. `algorithm` 필드로 분기하여 향후 다른 알고리즘 추가 시 확장 가능.

**YAML 파싱**

`pm/policy/loader.go`: YAML → `ThresholdConfig` 파싱. `algorithm` 값에 따라 적절한 Config 타입으로 역직렬화.

**etcd 영속화**

`internal/cluster/policy.go` 신규: `SavePolicy(yaml string)` / `LoadPolicy() (string, error)`. 키: `/actorbase/policy`. PM 시작 시 etcd에서 기존 policy를 로드하여 AutoBalancer를 복원한다.

**proto: PM에 ApplyPolicy / GetPolicy RPC 추가**

`PartitionManagerService`에 두 RPC 추가. policy는 YAML raw string으로 전달한다.

**abctl: policy 명령**

```bash
abctl policy apply policy.yaml   # 파일을 읽어 PM에 전송 → AutoPolicy 활성화
abctl policy get                  # 현재 적용 중인 policy YAML 출력
abctl policy clear                # policy 제거 → ManualPolicy로 복귀
```

---

### Phase 3 — PM AutoBalancer

`pm/balancer.go` 신규. `check_interval`마다 아래 루프를 실행한다.

**루프 알고리즘**

```
1. 모든 PS에 GetStats 호출 (병렬)
2. global cooldown 확인 → 쿨다운 중이면 전체 skip
3. split 대상 탐색:
   - 파티션 RPS > split.rps_threshold  OR
   - 파티션 key_count > split.key_threshold
   - 해당 파티션 쿨다운 미경과 → skip
   → midpoint 계산 후 RequestSplit 호출
4. balance 대상 탐색:
   - 노드 간 파티션 수 차이 > balance.max_partition_diff  OR
   - 노드 간 RPS 차이 > balance.rps_imbalance_pct %
   → 파티션 수/RPS가 가장 많은 노드에서 가장 적은 노드로 RequestMigrate
5. 작업 발생 시 cooldown 타이머 갱신 (global + partition)
```

**cooldown 관리**

- `globalLastAction time.Time`
- `partitionLastAction map[partitionID]time.Time`
- split/migrate 발생 시 두 타이머 모두 갱신

**PM Server 통합**

- AutoPolicy 적용 중일 때만 Balancer goroutine 시작
- `ApplyPolicy` 호출 시 기존 Balancer 중단 후 새 config로 재시작
- `ClearPolicy` 호출 시 Balancer 중단 → ManualPolicy 상태로 전환

---

### Phase 4 — 수동 명령 제한

`pm/manager_handler.go`의 `RequestSplit` / `RequestMigrate` 핸들러에서 AutoPolicy 활성 여부를 확인한다. 활성 중이면 `codes.FailedPrecondition`으로 거부한다.

```
"manual split/migrate not allowed while AutoPolicy is active"
```

`abctl split/migrate`에서 이 에러를 명확하게 출력한다.

---

## 파일 변경 요약

| 파일 | 변경 내용 |
|---|---|
| `internal/transport/actorbase.proto` | GetStats, ApplyPolicy, GetPolicy, ClearPolicy RPC 추가 |
| `internal/engine/stats.go` | 신규: PartitionStats, 슬라이딩 RPS 카운터 |
| `internal/engine/host.go` | stats 수집 연동 |
| `internal/cluster/policy.go` | 신규: etcd policy 저장/로드 |
| `pm/policy/threshold.go` | 신규: ThresholdConfig + YAML 파싱 |
| `pm/balancer.go` | 신규: AutoBalancer 루프 |
| `pm/server.go` | Balancer 시작/재시작, GetStats/ApplyPolicy/GetPolicy 핸들러 연동 |
| `pm/manager_handler.go` | AutoPolicy 중 수동 명령 거부 |
| `ps/server.go` | GetStats 핸들러 추가 |
| `cmd/abctl/main.go` | stats, policy apply/get 명령 추가 |

---

## 구현 순서

Phase 1 → 2 → 3 → 4 순서로 진행한다. Phase 1 완료 후 `abctl stats`로 stats 수집 자체를 먼저 검증한다. Phase 3 진입 전에 split/migrate 판단 로직을 단위 테스트로 검증한다.
