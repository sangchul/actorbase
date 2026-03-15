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

RPS는 60초 슬라이딩 윈도우(초 단위 버킷)로 계산한다. actor가 `provider.Countable` 인터페이스를 구현하면 key count를 반환하고, 구현하지 않으면 `-1`(n/a)로 표시된다.

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

split key는 Policy가 아닌 **PS(Actor)가 결정**한다. 결정 우선순위:

1. 호출자가 명시한 key (수동 `abctl split <id> <key>`)
2. `SplitHinter.SplitHint()` — Actor가 선택적으로 구현. 내부 상태(hotspot 등) 기반으로 최적 위치 제안.
3. `KeyRangeMidpoint(start, end)` — 파티션 key 범위의 바이트 레벨 산술 평균 (midpoint fallback)

Policy에서 ActionSplit을 반환할 때는 split key를 제공하지 않는다 (`BalanceAction`에 SplitKey 필드 없음). Splitter는 splitKey="" 로 `ExecuteSplit`을 호출하고, PS가 반환한 실제 key로 라우팅 테이블을 갱신한다.

### 수동/자동 전환 및 배타적 운영

**policy 유무가 곧 모드**다.

- `abctl policy apply policy.yaml` → AutoPolicy 활성화
- `abctl policy clear` → policy 제거, ManualPolicy로 복귀
- PM 시작 시 etcd에 저장된 policy가 있으면 자동으로 AutoPolicy 복원

AutoPolicy 활성 중에는 `abctl split`, `abctl migrate` 같은 수동 명령을 거부한다 (`codes.PermissionDenied`). 운영자가 수동으로 개입하려면 `abctl policy clear`로 먼저 AutoPolicy를 해제해야 한다.

AutoPolicy에서는 split과 migrate가 모두 자동으로 동작한다.

---

## 전체 구조

```
[PS] engine → 파티션별 stats 집계 (RPS 슬라이딩 윈도우, KeyCount)
  ↑ GetStats RPC (check_interval마다 PM이 poll)
[PM] AutoBalancer → split/migrate 판단 → splitter/migrator 직접 호출
  ↕ etcd
[PM] policy 저장/로드 (ApplyPolicy / GetPolicy RPC)
  ↑
[abctl] stats / policy apply / policy get / policy clear
```

AutoBalancer는 `splitter.Split` / `migrator.Migrate`를 직접 호출한다. gRPC handler의 AutoPolicy 차단 로직을 우회하기 위함이다.

---

## 구현

### Stats 수집 인프라

**`provider/actor.go` — Countable 인터페이스**

actor가 선택적으로 구현하는 인터페이스. key count를 반환한다.

```go
type Countable interface {
    KeyCount() int64
}
```

**`internal/engine/stats.go` — PartitionStats, rpsCounter**

- `PartitionStats`: 파티션 하나의 통계 (PartitionID, KeyCount, RPS)
- `rpsCounter`: 60초 슬라이딩 윈도우, 초 단위 버킷, goroutine-safe

**`internal/engine/mailbox.go`**

- `rpsCounter`: Receive 성공 시 `rps.inc()` 호출
- `keyCount atomic.Int64`: WAL 확인 후 actor가 Countable이면 갱신 (actor goroutine에서 호출 → 안전)
- `stats()` 메서드: `(keyCount int64, rps float64)` 반환

**`internal/engine/host.go` — GetStats**

모든 활성 actor의 stats를 수집하여 `[]PartitionStats` 반환.

### Policy YAML 및 etcd 저장

**`policy/policy.go` — 공통 타입 + ParsePolicy**

공통 설정과 정책 파싱 로직을 담는다. 알고리즘별 코드는 별도 파일로 분리.

```go
// RunnerConfig: balancerRunner 실행 파라미터. 알고리즘과 무관한 공통 설정.
type RunnerConfig struct {
    CheckInterval time.Duration
    Cooldown      CooldownConfig
}
type CooldownConfig struct {
    Global    time.Duration
    Partition time.Duration
}
type BalanceConfig struct {
    MaxPartitionDiff int
    RPSImbalancePct  float64
}
```

`ParsePolicy([]byte) (provider.BalancePolicy, *RunnerConfig, error)` — `algorithm` 필드로 구현체 선택.
현재 지원 알고리즘:
- `"threshold"` → `ThresholdPolicy` (RPS·key count 절대 임계값)
- `"relative"` → `RelativePolicy` (클러스터 평균 RPS 대비 배수)

새 알고리즘 추가 시 `policy/policy.go`의 `ParsePolicy`에만 case를 추가하면 된다.

**`policy/threshold.go` — ThresholdConfig, ThresholdPolicy**

```go
type ThresholdConfig struct {
    Algorithm     string
    CheckInterval time.Duration
    Cooldown      CooldownConfig
    Split         SplitConfig
    Balance       BalanceConfig
}
type SplitConfig struct {
    RPSThreshold float64
    KeyThreshold int64
}
```

`ThresholdPolicy`는 `provider.BalancePolicy`를 구현한다.

**`policy/relative.go` — RelativeConfig, RelativePolicy**

```go
// RelativePolicy: 클러스터 평균 RPS 대비 배수 기반 split 정책.
// 평균 RPS가 MinAvgRPS 이하이면 발동하지 않는다.
type RelativeConfig struct {
    Algorithm     string
    CheckInterval time.Duration
    Cooldown      CooldownConfig
    Split         RelativeSplit
    Balance       BalanceConfig
}
type RelativeSplit struct {
    RPSMultiplier float64 // 파티션 RPS > 평균 × multiplier 이면 split
    MinAvgRPS     float64 // 평균 RPS 하한. 낮은 트래픽에서 오동작 방지
}
```

relative 정책 YAML 예시:

```yaml
algorithm: relative
check_interval: 30s
cooldown:
  global: 60s
  partition: 120s
split:
  rps_multiplier: 3.0    # 평균 RPS의 3배 초과 시 split
  min_avg_rps: 10.0      # 평균 RPS < 10이면 split 발동 안 함
balance:
  max_partition_diff: 2
  rps_imbalance_pct: 30
```

**`policy/noop.go` — NoopBalancePolicy**

```go
type NoopBalancePolicy struct{}
// Evaluate / OnNodeJoined / OnNodeLeft 모두 nil 반환
```
`pm.Config.BalancePolicy`가 nil일 때 기본값으로 사용된다.

**`internal/cluster/policy.go`**

etcd 키 `/actorbase/policy`에 YAML raw string 저장/로드/삭제.

### PM Server 통합

**`pm/server.go`**

- `serverCtx context.Context` — `Start()`의 ctx를 저장. balancer goroutine의 lifetime parent context로 사용 (request context 사용 시 RPC 반환 직후 종료되는 문제 방지).
- `applyPolicy(ctx, yamlStr, pol provider.BalancePolicy, runnerCfg *policy.RunnerConfig)` — 기존 balancer 중단 → 새 balancer 시작. 알고리즘 종류를 모른다.
- `activeBalancePolicy()` — `activeRunnerCfg != nil`이면 YAML policy, 아니면 `cfg.BalancePolicy` 반환.
- `activePolicy provider.BalancePolicy` + `activeRunnerCfg *policy.RunnerConfig` — 현재 YAML policy 상태 저장.
- `clearPolicy(ctx)` — balancer 중단 → NoopBalancePolicy
- `Start()` — etcd에서 policy 복원 시 balancer도 함께 시작

### balancerRunner 루프

**`pm/balancer.go`**

`balancerRunner`는 `check_interval`마다 아래 루프를 실행한다.
`pol provider.BalancePolicy`를 주입받아 판단 로직을 위임한다.
`cfg *policy.RunnerConfig`로 check_interval과 cooldown을 설정한다.

```
1. 라우팅 테이블 + 노드 목록 조회
2. 모든 PS에 GetStats 병렬 호출 (5초 timeout)
3. global cooldown 확인 → 쿨다운 중이면 전체 skip
4. pol.Evaluate(ctx, clusterStats) → []BalanceAction
5. 액션 순서대로 실행:
   - ActionSplit  → splitter.Split(ctx, actorType, partitionID, "" /*splitKey auto*/)
                    split key는 PS가 SplitHinter 또는 midpoint로 결정
   - ActionMigrate → migrator.Migrate(ctx, actorType, partitionID, targetNode)
   - ActionFailover → migrator.Failover(ctx, partitionID, targetNode)
6. 작업 발생 시 cooldown 타이머 갱신 (global + partition)
```

### proto RPC 추가

**`PartitionControlService`**

- `GetStats(GetStatsRequest) → GetStatsResponse`

**`PartitionManagerService`**

- `GetClusterStats(GetClusterStatsRequest) → GetClusterStatsResponse`
- `ApplyPolicy(ApplyPolicyRequest) → ApplyPolicyResponse`
- `GetPolicy(GetPolicyRequest) → GetPolicyResponse`
- `ClearPolicy(ClearPolicyRequest) → ClearPolicyResponse`

### 수동 명령 제한

`pm/manager_handler.go`의 `RequestSplit` / `RequestMigrate`에서 AutoPolicy 활성 여부를 확인하여 `codes.PermissionDenied`로 거부한다.

`internal/transport/errors.go`의 `fromGRPCStatus`에 `codes.PermissionDenied → raw message` 처리 추가 (에러 메시지 보존).

### abctl 명령

```bash
abctl stats                        # 클러스터 전체 stats
abctl stats <node-id>              # 특정 노드 stats
abctl policy apply <file>          # AutoPolicy 활성화
abctl policy get                   # 현재 policy 조회
abctl policy clear                 # ManualPolicy로 복귀
```

---

## 파일 변경 요약

| 파일 | 변경 내용 |
|---|---|
| `provider/actor.go` | Countable 인터페이스 추가; SplitHinter 인터페이스 추가 |
| `provider/balance.go` | 신규: BalancePolicy 인터페이스 + 관련 타입 (NodeInfo, ClusterStats, BalanceAction 등); BalanceAction.SplitKey 제거 (split key는 PS가 결정) |
| `internal/engine/stats.go` | 신규: PartitionStats, rpsCounter (60s 슬라이딩 윈도우) |
| `internal/engine/mailbox.go` | RPS 추적, KeyCount atomic 갱신, stats() 메서드; split 신호 수신 시 SplitHinter → midpoint fallback 체인 처리 |
| `internal/engine/host.go` | GetStats() []PartitionStats 추가; Split 반환값 (string, error)로 변경 (실제 사용된 splitKey 반환); KeyRangeMidpoint 함수 추가 |
| `internal/transport/proto/actorbase.proto` | GetStats, GetClusterStats, ApplyPolicy, GetPolicy, ClearPolicy RPC 추가; ExecuteSplitRequest에 key_range_start/end 추가; ExecuteSplitResponse에 split_key 추가 |
| `internal/cluster/policy.go` | 신규: etcd policy 저장/로드/삭제 |
| `provider/balance.go` | 신규: BalancePolicy 인터페이스 + 관련 타입 |
| `policy/policy.go` | 신규: ParsePolicy, RunnerConfig, CooldownConfig, BalanceConfig (threshold에서 공통 코드 분리) |
| `policy/threshold.go` | 신규 (pm/policy에서 이동): ThresholdConfig, SplitConfig, ThresholdPolicy |
| `policy/relative.go` | 신규: RelativeConfig, RelativeSplit, RelativePolicy (클러스터 평균 RPS 대비 배수 기반 정책) |
| `policy/noop.go` | 신규 (pm/policy에서 이동): NoopBalancePolicy |
| `pm/policy/` | 삭제 (policy/, manual.go, auto.go 모두 제거) |
| `pm/balancer.go` | balancerRunner: cfg를 *policy.RunnerConfig로, pol을 provider.BalancePolicy로 변경 |
| `pm/config.go` | Policy → BalancePolicy provider.BalancePolicy |
| `pm/server.go` | failoverNode 삭제, handleNodeJoined/handleNodeLeft/activeBalancePolicy/quickClusterStats/executeBalanceActions 추가, applyPolicy 시그니처 변경 |
| `pm/client.go` | 신규: pm.Client — PM 관리 플레인 공개 클라이언트 |
| `pm/manager_handler.go` | AutoPolicy 중 수동 명령 거부, ApplyPolicy/GetPolicy/ClearPolicy/GetClusterStats 핸들러 |
| `ps/server.go` | actorDispatcher에 GetStats(), TypeID() 추가 |
| `ps/control_handler.go` | GetStats 핸들러 |
| `internal/transport/client.go` | PSControlClient.GetStats, PMClient policy/stats 메서드 |
| `internal/transport/errors.go` | PermissionDenied → raw message |
| `cmd/abctl/main.go` | pm.Client 사용으로 전환 (internal/transport 직접 의존 제거), stats/policy 명령 |
| `examples/kv_server/main.go` | kvActor.KeyCount() 추가 |
| `examples/s3_server/bucket_actor.go` | 신규 (main.go에서 분리): BucketRequest, BucketResponse, bucketActor (KeyCount 구현, SplitHinter 미구현 → midpoint split) |
| `examples/s3_server/object_actor.go` | 신규 (main.go에서 분리): ObjectRequest, ObjectResponse, objectActor (KeyCount 구현, SplitHinter 구현 → hotspot 기반 split) |
| `test/integration/run.sh` | 신규: 통합 시나리오 1~7 자동화 스크립트 (44개 assertion) |
