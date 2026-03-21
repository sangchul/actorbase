# 패키지 구조 설계

## 1. Clean Architecture 레이어 매핑

actorbase의 구성 요소를 Clean Architecture 4개 레이어에 매핑한다.

| 레이어 | 역할 | actorbase 구성 요소 |
|---|---|---|
| **Entities** (Domain) | 핵심 비즈니스 개념. 외부에 의존하지 않음 | `Partition`, `KeyRange`, `NodeInfo`, `RoutingTable` |
| **Use Cases** (Application) | 애플리케이션 로직. Domain만 의존 | Actor 생명주기, 요청 라우팅, Split/Migration 조율 |
| **Interface Adapters** | 외부 시스템과 Use Case를 연결 | gRPC 핸들러, etcd 어댑터 |
| **Frameworks & Drivers** | 외부 프레임워크, 라이브러리 | gRPC, etcd 클라이언트 |

**핵심 의존성 규칙**: 의존성은 항상 안쪽(Domain)을 향한다. Domain은 아무것도 import하지 않는다.

---

## 2. 패키지 구조

```
actorbase/
│
├── provider/                  # 사용자와의 계약 패키지 (인터페이스만)
│   ├── actor.go               # Actor[Req,Resp], ActorFactory, Context, Countable
│   ├── wal.go                 # WALStore, WALEntry
│   ├── checkpoint.go          # CheckpointStore
│   ├── codec.go               # Codec
│   ├── metrics.go             # Metrics 인터페이스 (Counter, Gauge, Histogram)
│   ├── balance.go             # BalancePolicy, ClusterStats, BalanceAction 등
│   └── error.go               # 사용자에게 노출되는 공개 에러 타입
│
├── policy/                    # BalancePolicy 참조 구현체 (adapter/와 같은 위상)
│   ├── policy.go              # ParsePolicy(yaml) — YAML → BalancePolicy 파싱
│   ├── threshold.go           # ThresholdPolicy, ThresholdConfig
│   ├── relative.go            # RelativePolicy — 클러스터 평균 대비 배수 기반 정책
│   └── noop.go                # NoopBalancePolicy (기본값: 아무 작업도 하지 않음)
│
├── adapter/                   # provider 인터페이스의 기본 구현체
│   ├── fs/                    # 파일시스템 기반 WALStore, CheckpointStore
│   │   ├── wal.go
│   │   └── checkpoint.go
│   ├── json/                  # JSON 기반 Codec 구현체
│   │   └── codec.go
│   ├── redis/                 # Redis Streams 기반 WALStore
│   │   └── wal.go
│   └── s3/                    # S3(MinIO 호환) 기반 CheckpointStore
│       └── checkpoint.go
│
├── internal/                  # 모듈 내 공유 라이브러리 (외부 import 불가)
│   ├── domain/                # [Entities] 핵심 도메인. 의존성 없음.
│   │   ├── partition.go       # Partition, KeyRange, PartitionStatus
│   │   ├── node.go            # NodeInfo (주소, 상태)
│   │   └── routing.go         # RoutingTable (KeyRange → NodeInfo 매핑)
│   │
│   ├── engine/                # [Use Cases] Actor 실행 엔진. PS가 사용.
│   │   ├── host.go            # ActorHost: Actor 생명주기 (활성화/eviction/split)
│   │   ├── mailbox.go         # 메시지 큐 (채널 기반, Actor당 1개)
│   │   ├── flusher.go         # WALFlusher: WAL batch commit
│   │   ├── stats.go           # PartitionStats, rpsCounter (60s 슬라이딩 윈도우)
│   │   └── scheduler.go       # idle Actor eviction + checkpoint 스케줄러
│   │
│   ├── cluster/               # [Interface Adapters] 클러스터 멤버십/라우팅. PS·PM 공용.
│   │   ├── registry.go        # NodeRegistry: etcd lease 기반 노드 등록/조회
│   │   ├── membership.go      # MembershipWatcher: 노드 join/leave 이벤트
│   │   ├── routing.go         # RoutingTableStore: 라우팅 테이블 저장/조회/watch
│   │   ├── policy.go          # etcd 기반 policy YAML 저장/로드/삭제
│   │   └── pm.go              # PM 리더 선출 (CampaignLeader, WaitForLeader, GetLeaderAddr)
│   │
│   ├── rebalance/             # [Use Cases] Split/Migration 조율. PM이 사용.
│   │   ├── splitter.go        # 파티션 분할 조율
│   │   └── migrator.go        # 파티션 이동 조율 (Migrate + Failover)
│   │
│   └── transport/             # [Interface Adapters] gRPC 서버/클라이언트 공통
│       ├── proto/             # .proto 파일 및 generated 코드
│       ├── server.go          # 공통 gRPC 서버 팩토리 (interceptor 포함)
│       ├── client.go          # PSClient, PMClient, PSControlClient, ConnPool
│       └── errors.go          # gRPC status ↔ provider error 변환
│
├── ps/                        # Partition Server 조립 패키지
│   ├── config.go              # BaseConfig, TypeConfig[Req,Resp]
│   ├── server.go              # Server, ServerBuilder, Register[Req,Resp]
│   ├── partition_handler.go   # PartitionService 핸들러 (SDK → PS)
│   └── control_handler.go     # PartitionControlService 핸들러 (PM → PS)
│
├── pm/                        # Partition Manager 조립 패키지
│   ├── config.go              # Config (BalancePolicy, ActorTypes 등)
│   ├── server.go              # Server: 컴포넌트 조립 및 생명주기 관리
│   ├── manager_handler.go     # PartitionManagerService 핸들러 (SDK/abctl → PM)
│   ├── balancer.go            # balancerRunner: 주기적 stats 수집 + policy.Evaluate 실행
│   └── client.go              # Client: PM 관리 플레인 클라이언트 (abctl 등이 사용)
│
├── sdk/                       # Go SDK (클라이언트 애플리케이션이 import)
│   ├── config.go              # Config[Req, Resp]
│   └── client.go              # Client[Req, Resp]: PM 라우팅 구독, PS 요청 전송
│
├── cmd/
│   ├── pm/main.go             # Partition Manager 실행 바이너리
│   └── abctl/main.go          # CLI tool: 클러스터 상태 조회, split/migrate, policy 관리
│
└── examples/
    ├── kv_server/             # KV Actor 구현 예시 (PS 바이너리 빌드 참고)
    ├── kv_client/             # KV CLI 클라이언트 예시
    ├── kv_longrun/            # 장기 부하 + 정합성 검증 도구
    ├── kv_stress/             # 부하 생성기 예시
    ├── s3_server/             # S3 메타데이터 Actor 예시 (bucket + object)
    └── s3_client/             # S3 CLI 클라이언트 예시
```

---

## 3. 의존성 방향

```
examples/, cmd/pm
    │ import
    ▼
ps / pm / sdk                  # 플랫폼 공개 API
    │ import
    ▼
internal/cluster
internal/transport
internal/engine
internal/rebalance
    │ import
    ▼
internal/domain                # 의존성 없음

provider                       # 인터페이스만. 표준 라이브러리 외 의존 없음.
policy                         # provider.BalancePolicy 구현체. pm이 내부에서 사용.
adapter/fs, adapter/json,       # provider 인터페이스 구현체. examples가 사용.
adapter/redis, adapter/s3       # 네트워크 기반 구현체. examples에서 선택적 사용.

cmd/abctl → pm.Client          # internal/* 직접 의존 없음
```

**의존성 규칙:**
- `internal/domain` → 아무것도 import하지 않는다.
- `internal/engine` → `internal/domain` + `provider`
- `internal/rebalance` → `internal/domain` + `internal/cluster` + `internal/transport`
- `internal/cluster` → `internal/domain`
- `internal/transport` → `internal/domain` + `provider`
- `ps`, `pm`, `sdk` → `internal/*` + `provider`
- `pm` → `policy` (ParsePolicy, NoopBalancePolicy를 내부에서 사용)
- `policy` → `provider`
- `adapter/fs`, `adapter/json` → `provider`
- `cmd/abctl` → `pm` (pm.Client만 사용. `internal/*` 직접 접근 없음)
- `cmd/pm` → `pm`
- `examples/*` → `ps` + `sdk` + `adapter/*` + `provider`

---

## 4. 공개 패키지 역할

### provider/ — 인터페이스 계약

사용자가 구현해야 하는 인터페이스와 관련 타입만 정의한다. 외부 의존 없음.

| 파일 | 내용 |
|---|---|
| `actor.go` | `Actor[Req,Resp]`, `ActorFactory`, `Context`, `Countable` |
| `wal.go` | `WALStore`, `WALEntry` |
| `checkpoint.go` | `CheckpointStore` |
| `codec.go` | `Codec` |
| `metrics.go` | `Metrics`, `Counter`, `Gauge`, `Histogram` |
| `balance.go` | `BalancePolicy`, `ClusterStats`, `NodeStats`, `PartitionInfo`, `BalanceAction`, `NodeLeaveReason` |
| `error.go` | `ErrNotFound`, `ErrPartitionMoved`, `ErrPartitionBusy` 등 |

### policy/ — BalancePolicy 참조 구현체

`adapter/`가 WALStore/Codec의 참조 구현인 것처럼, `policy/`는 `provider.BalancePolicy`의 참조 구현이다.

사용자는 `ThresholdPolicy`를 그대로 사용하거나, 임베딩해서 일부 메서드만 재정의할 수 있다.

```go
// OnNodeJoined/OnNodeLeft는 ThresholdPolicy 그대로, Evaluate만 커스텀
type MyPolicy struct {
    policy.ThresholdPolicy
}
func (p *MyPolicy) Evaluate(ctx context.Context, stats provider.ClusterStats) []provider.BalanceAction {
    // 커스텀 split/migrate 판단 로직
}
```

| 파일 | 내용 |
|---|---|
| `policy.go` | `ParsePolicy(yaml)` — YAML → `BalancePolicy` + `RunnerConfig` 파싱 |
| `threshold.go` | `ThresholdPolicy`, `ThresholdConfig` |
| `relative.go` | `RelativePolicy` — 클러스터 평균 RPS 대비 배수 기반 정책 |
| `noop.go` | `NoopBalancePolicy` — 모든 메서드가 nil 반환. pm.Config 기본값. |

### adapter/ — provider 구현체

| 패키지 | 내용 |
|---|---|
| `adapter/fs` | 파일시스템 기반 `WALStore`, `CheckpointStore` |
| `adapter/json` | JSON 기반 `Codec` |
| `adapter/redis` | Redis Streams 기반 `WALStore` (명시적 uint64 ID) |
| `adapter/s3` | S3/MinIO 호환 `CheckpointStore` (aws-sdk-go-v2) |

### pm.Client — PM 관리 플레인 클라이언트

`pm.Client`는 `internal/transport` 없이 PM에 명령을 보낼 수 있는 공개 클라이언트다.
abctl이 사용하며, 운영 도구를 직접 만들 때도 활용할 수 있다.

```go
client, _ := pm.NewClient("localhost:8000")
members, _ := client.ListMembers(ctx)
snap := <-client.WatchRouting(ctx, "my-tool")
client.RequestSplit(ctx, "kv", partitionID, splitKey)
client.ApplyPolicy(ctx, yamlStr)
```

반환 타입(`Member`, `RoutingSnapshot`, `NodeStat` 등)은 `internal/domain`을 노출하지 않는다.

---

## 5. 패키지별 책임 요약

| 패키지 | 책임 |
|---|---|
| `provider` | 사용자와의 인터페이스 계약 정의 |
| `policy` | BalancePolicy 참조 구현체 (ThresholdPolicy, NoopBalancePolicy) |
| `adapter/fs` | 파일시스템 기반 WALStore, CheckpointStore |
| `adapter/json` | JSON 기반 Codec |
| `adapter/redis` | Redis Streams 기반 WALStore |
| `adapter/s3` | S3/MinIO 호환 CheckpointStore |
| `internal/domain` | 파티션, 노드, 라우팅 테이블 핵심 도메인 타입 |
| `internal/engine` | Actor 생명주기, mailbox, WAL batch commit, stats 수집 |
| `internal/cluster` | etcd 기반 멤버십, 라우팅 테이블, policy YAML, PM 리더 선출 |
| `internal/rebalance` | 파티션 split/migrate/failover 조율 |
| `internal/transport` | gRPC 서버/클라이언트, proto 관리, 에러 변환 |
| `ps` | Partition Server 조립 및 기동 (ServerBuilder, Register) |
| `pm` | Partition Manager 조립 및 기동, pm.Client 제공 |
| `sdk` | 클라이언트 앱용 Go SDK (라우팅 구독, 요청 전송) |
| `cmd/pm` | PM 실행 바이너리 |
| `cmd/abctl` | 운영 CLI (pm.Client 사용) |
| `examples/*` | 사용자 구현 예시 (KV, S3) |

---

## 6. 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| `policy/` 위치 | 최상위 패키지 (`adapter/`와 동일 위상) | 사용자가 임베딩해서 커스텀 정책을 만들 수 있도록 공개 위치에 배치. PM 내부 구현이 아닌 사용자용 참조 구현. |
| `pm.Client` 추가 | `pm/client.go`에 공개 클라이언트 제공 | `cmd/abctl`이 `internal/transport`를 직접 import하지 않도록. 운영 도구 개발 시 공개 API 제공. |
| `cmd/abctl` 의존성 | `pm.Client`만 사용 | `internal/*` 직접 접근 제거. internal 경계 준수. |
| `cmd/ps/` 삭제 | `examples/kv_server/`로 이전 | 플랫폼 중립성. PS 바이너리는 사용자가 자신의 Actor를 주입해서 빌드하는 것이 올바른 패턴. |
| proto 파일 위치 | `internal/transport/proto/` | 외부 노출 불필요. transport 구현과 함께 관리. |
| 오류 타입 | 외부용 `provider/error.go` | 사용자에게 노출할 에러만 provider에 정의. 내부 에러는 각 패키지 내부에서 처리. |
| WAL/Checkpoint 분리 | `provider/wal.go` + `provider/checkpoint.go` | 성격이 달라 적합한 백엔드가 다름. WAL: append-only, Checkpoint: 덮어쓰기. |
| 로깅 | `log/slog` 직접 사용 | Go 1.21+ 표준. 별도 Logger 인터페이스 추상화 불필요. |
| 인터페이스 위치 | 사용하는 패키지 안에 정의 | 별도 `port/` 패키지 없이 단순하게. |
