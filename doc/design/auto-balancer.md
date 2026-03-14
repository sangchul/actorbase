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

자동 split 시 split key는 파티션 key 범위의 midpoint로 결정한다. 두 문자열의 바이트 레벨 산술 평균을 사용한다.

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

**`pm/policy/threshold.go` — ThresholdConfig**

```go
type ThresholdConfig struct {
    Algorithm     string        // "threshold"
    CheckInterval time.Duration
    Cooldown      CooldownConfig
    Split         SplitConfig
    Balance       BalanceConfig
}
```

`ParsePolicy([]byte) (*ThresholdConfig, error)` — `gopkg.in/yaml.v3` 사용.

**`internal/cluster/policy.go`**

etcd 키 `/actorbase/policy`에 YAML raw string 저장/로드/삭제.

### PM Server 통합

**`pm/server.go`**

- `serverCtx context.Context` — `Start()`의 ctx를 저장. balancer goroutine의 lifetime parent context로 사용 (request context 사용 시 RPC 반환 직후 종료되는 문제 방지).
- `applyPolicy(ctx, yamlStr, cfg)` — 기존 balancer 중단 → 새 balancer 시작
- `clearPolicy(ctx)` — balancer 중단 → ManualPolicy
- `Start()` — etcd에서 policy 복원 시 balancer도 함께 시작

### AutoBalancer 루프

**`pm/balancer.go`**

`check_interval`마다 아래 루프를 실행한다.

```
1. 라우팅 테이블 + 노드 목록 조회
2. 모든 PS에 GetStats 병렬 호출 (5초 timeout)
3. global cooldown 확인 → 쿨다운 중이면 전체 skip
4. split 대상 탐색:
   - 파티션 RPS > split.rps_threshold  OR
   - 파티션 key_count > split.key_threshold
   - 해당 파티션 쿨다운 미경과 → skip
   → keyRangeMidpoint 계산 후 splitter.Split 호출
   → 한 사이클에 split 하나만 실행 후 return
5. balance 대상 탐색:
   - 노드 간 파티션 수 차이 > balance.max_partition_diff  OR
   - 노드 간 RPS 차이 > balance.rps_imbalance_pct %
   → 파티션 수가 가장 많은 노드 → 가장 적은 노드로 migrator.Migrate
6. 작업 발생 시 cooldown 타이머 갱신 (global + partition)
```

**keyRangeMidpoint**: 두 문자열의 바이트 레벨 산술 평균. 빈 범위 처리 포함.

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
| `provider/actor.go` | Countable 인터페이스 추가 |
| `internal/engine/stats.go` | 신규: PartitionStats, rpsCounter (60s 슬라이딩 윈도우) |
| `internal/engine/mailbox.go` | RPS 추적, KeyCount atomic 갱신, stats() 메서드 |
| `internal/engine/host.go` | GetStats() []PartitionStats 추가 |
| `internal/transport/proto/actorbase.proto` | GetStats, GetClusterStats, ApplyPolicy, GetPolicy, ClearPolicy RPC 추가 |
| `internal/cluster/policy.go` | 신규: etcd policy 저장/로드/삭제 |
| `pm/policy/threshold.go` | 신규: ThresholdConfig + ParsePolicy (YAML) |
| `pm/balancer.go` | 신규: autoBalancer 루프, checkSplit, checkBalance, keyRangeMidpoint |
| `pm/server.go` | serverCtx, applyPolicy/clearPolicy (balancer 시작/중단), Start() 복원 |
| `pm/manager_handler.go` | AutoPolicy 중 수동 명령 거부, ApplyPolicy/GetPolicy/ClearPolicy/GetClusterStats 핸들러 |
| `ps/server.go` | actorDispatcher에 GetStats(), TypeID() 추가 |
| `ps/control_handler.go` | GetStats 핸들러 |
| `internal/transport/client.go` | PSControlClient.GetStats, PMClient policy/stats 메서드 |
| `internal/transport/errors.go` | PermissionDenied → raw message |
| `cmd/abctl/main.go` | stats, policy apply/get/clear 명령 |
| `examples/kv_server/main.go` | kvActor.KeyCount() 추가 |
| `examples/s3_server/main.go` | bucketActor.KeyCount(), objectActor.KeyCount() 추가 |
