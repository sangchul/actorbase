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
├── provider/                  # 사용자와의 계약 패키지
│   ├── actor.go               # Actor[Req,Resp], ActorFactory, Context
│   ├── wal.go                 # WALStore, WALEntry
│   ├── checkpoint.go          # CheckpointStore
│   ├── codec.go               # Codec
│   ├── metrics.go             # Metrics 인터페이스 (Counter, Gauge, Histogram)
│   └── error.go               # 사용자에게 노출되는 공개 에러 타입
│
├── adapter/                   # provider 인터페이스의 기본 구현체
│   ├── json/                  # JSON 기반 Codec 구현체 (기본 제공)
│   │   └── codec.go
│   └── prometheus/            # Prometheus 기반 Metrics 구현체 (기본 제공)
│       └── metrics.go
│
├── internal/                  # 모듈 내 공유 라이브러리 (외부 import 불가)
│   ├── domain/                # [Entities] 핵심 도메인. 의존성 없음.
│   │   ├── partition.go       # Partition, KeyRange
│   │   ├── node.go            # NodeInfo (주소, 상태)
│   │   └── routing.go         # RoutingTable (KeyRange → NodeInfo 매핑)
│   │
│   ├── errors/                # 내부 에러 타입. 외부에 노출되지 않음.
│   │   └── errors.go
│   │
│   ├── engine/                # [Use Cases] Actor 실행 엔진. PS가 사용.
│   │   ├── host.go            # ActorHost: Actor 생명주기 (활성화/eviction/split)
│   │   ├── mailbox.go         # 메시지 큐 (채널 기반, Actor당 1개)
│   │   ├── flusher.go         # WALFlusher: WAL group commit
│   │   └── scheduler.go       # idle Actor eviction 스케줄러
│   │
│   ├── cluster/               # [Interface Adapters] 클러스터 멤버십/라우팅. PS·PM 공용.
│   │   ├── registry.go        # NodeRegistry: etcd lease 기반 노드 등록/조회
│   │   ├── membership.go      # MembershipWatcher: 노드 join/leave 이벤트
│   │   └── routing.go         # RoutingTableStore: 라우팅 테이블 저장/조회/watch
│   │
│   ├── rebalance/             # [Use Cases] Split/Migration 조율. PM이 사용.
│   │   ├── splitter.go        # 파티션 분할 로직
│   │   └── migrator.go        # 파티션 이동 로직
│   │
│   └── transport/             # [Interface Adapters] gRPC 서버/클라이언트 공통
│       ├── proto/             # .proto 파일 및 generated 코드
│       ├── server.go          # 공통 gRPC 서버 팩토리 (interceptor 포함)
│       └── client.go          # PSClient, PMClient, PSControlClient, ConnPool
│
├── ps/                        # Partition Server daemon
│   ├── config.go              # ps.Config
│   ├── server.go              # 컴포넌트 조립 및 생명주기 관리
│   ├── partition_handler.go   # PartitionService 핸들러 (SDK → PS)
│   └── control_handler.go     # PartitionControlService 핸들러 (PM → PS)
│
├── pm/                        # Partition Manager daemon
│   ├── config.go              # pm.Config
│   ├── server.go              # 컴포넌트 조립 및 생명주기 관리
│   ├── manager_handler.go     # PartitionManagerService 핸들러 (SDK/abctl → PM)
│   └── policy/
│       ├── policy.go          # RebalancePolicy 인터페이스
│       ├── manual.go          # ManualPolicy: 자동 rebalance 없음
│       └── auto.go            # AutoRebalancePolicy: 메트릭 기반 자동 rebalance
│
├── sdk/                       # Go SDK (클라이언트 애플리케이션이 import)
│   ├── config.go              # sdk.Config[Req, Resp]
│   └── client.go              # Client[Req, Resp]: PM 라우팅 구독, PS 요청 전송
│
└── cmd/
    ├── ps/main.go             # Partition Server 실행 바이너리
    ├── pm/main.go             # Partition Manager 실행 바이너리
    └── abctl/                 # CLI tool: 클러스터 상태 조회, split/migrate 요청
        ├── main.go
        └── config.go          # ~/.actorbase/config.yaml 로드 (플래그 > 환경변수 > 파일 순)
```

---

## 3. 의존성 방향

```
[Interface Adapters]
  internal/transport/
  internal/cluster/  (etcd 구현 포함)
        │
        ▼
[Use Cases]
  internal/engine/      (provider.WALStore, provider.CheckpointStore 사용)
  internal/rebalance/
        │
        ▼
[Entities]
  internal/domain/
```

**규칙:**
- `internal/domain` → 아무것도 import하지 않는다.
- `internal/engine` → `internal/domain` + `provider` import.
- `internal/rebalance` → `internal/domain` + `internal/cluster` + `internal/transport` import.
- `internal/cluster` → `internal/domain` import.
- `internal/transport` → `internal/domain` + `provider` import.
- `ps/`, `pm/`, `sdk/` → `internal/*` + `provider` import.
- `adapter/json` → `provider`(Codec) import.
- `adapter/prometheus` → `provider`(Metrics) import.
- `cmd/abctl` → `sdk/` + `internal/transport` import.
- 인터페이스는 별도 `port/` 패키지 없이 **사용하는 패키지 안에 정의**한다.
- 로깅은 `log/slog` 표준 라이브러리를 직접 사용한다. 별도 Logger 인터페이스를 두지 않는다.

---

## 4. provider/ 패키지

사용자와의 계약 패키지. 사용자가 구현해야 하는 인터페이스와 프레임워크가 제공하는 인터페이스를 함께 정의한다.

### provider/actor.go

```go
// =============================================
// 사용자가 구현해야 하는 인터페이스
// =============================================

// Actor는 하나의 파티션(key range)을 담당하는 비즈니스 로직 단위.
// Req는 수신 요청 타입, Resp는 응답 타입이다.
type Actor[Req, Resp any] interface {
    Receive(ctx Context, req Req) (resp Resp, walEntry []byte, err error)
    Replay(entry []byte) error
    Snapshot() ([]byte, error)
    Restore(data []byte) error
    Split(splitKey string) (upperHalf []byte, err error)
}

type ActorFactory[Req, Resp any] func(partitionID string) Actor[Req, Resp]

// =============================================
// 프레임워크가 제공하는 인터페이스 (사용만 함)
// =============================================

type Context interface {
    PartitionID() string
    Logger() *slog.Logger
}
```

### provider/wal.go

```go
type WALEntry struct {
    LSN  uint64
    Data []byte
}

type WALStore interface {
    Append(ctx context.Context, partitionID string, data []byte) (lsn uint64, err error)
    ReadFrom(ctx context.Context, partitionID string, fromLSN uint64) ([]WALEntry, error)
    TrimBefore(ctx context.Context, partitionID string, lsn uint64) error
}
```

### provider/checkpoint.go

```go
type CheckpointStore interface {
    Save(ctx context.Context, partitionID string, data []byte) error
    Load(ctx context.Context, partitionID string) ([]byte, error)
    Delete(ctx context.Context, partitionID string) error
}
```

### provider/codec.go

```go
type Codec interface {
    Marshal(v any) ([]byte, error)
    Unmarshal(data []byte, v any) error
}
```

---

## 5. 주요 패키지별 책임

| 패키지 | 책임 |
|---|---|
| `provider` | 사용자와의 인터페이스 계약 정의 (Actor, WALStore, CheckpointStore, Codec, Metrics, 공개 에러) |
| `adapter/json` | JSON 기반 Codec 기본 구현체 |
| `adapter/prometheus` | Prometheus 기반 Metrics 기본 구현체 |
| `internal/domain` | 파티션, 노드, 라우팅 테이블 등 핵심 도메인 타입 |
| `internal/errors` | 내부 에러 타입 (외부 미노출) |
| `internal/engine` | Actor 생명주기(활성화/eviction/split), mailbox, WAL group commit, eviction 스케줄러 |
| `internal/cluster` | etcd lease 기반 멤버십 등록/감지, 라우팅 테이블 저장/조회/watch |
| `internal/rebalance` | 파티션 split/migration 실행 조율 |
| `internal/transport` | gRPC 서버/클라이언트 공통 구현, proto 파일 관리 |
| `ps` | Partition Server 조립 및 기동 |
| `pm` | Partition Manager 조립 및 기동, rebalance 정책 실행 |
| `sdk` | 클라이언트 앱용 Go SDK (라우팅 구독, 요청 전송) |
| `cmd/ps` | PS 실행 바이너리 |
| `cmd/pm` | PM 실행 바이너리 |
| `cmd/abctl` | 운영 CLI (클러스터 상태 조회, split/migrate 요청, 데이터 읽기/쓰기) |

---

## 6. 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| proto 파일 위치 | `internal/transport/proto/` | 기업 내부용이므로 외부 노출 불필요. transport 구현과 함께 관리. |
| 오류 타입 | 외부용 `provider/error.go`, 내부용 `internal/errors/` | 사용자에게 노출할 에러와 내부 에러를 명확히 분리. |
| WAL/Checkpoint 분리 | `provider/wal.go` + `provider/checkpoint.go` | 성격이 달라 적합한 백엔드가 다름. WAL: append-only (Redis Stream 등), Checkpoint: 덮어쓰기 (S3 등). |
| `internal/wal`, `internal/checkpoint` 제거 | WAL 로직 → `engine/flusher.go`, Checkpoint 로직 → `engine/host.go` | 별도 패키지로 분리할 로직이 없음. engine이 provider 인터페이스를 직접 사용. |
| 로깅 | `log/slog` 직접 사용 | Go 1.21+ 표준. 별도 인터페이스 추상화 불필요. |
| 메트릭 | `provider/metrics.go` 인터페이스 + `adapter/prometheus/` 기본 구현체 | 테스트 용이성 및 교체 가능성 확보. |
| Codec | `provider/codec.go` 인터페이스 + `adapter/json/` 기본 구현체 | SDK·PS 양측에 동일 구현체 주입. 교체 가능. |
| abctl 설정 | `~/.actorbase/config.yaml`. 우선순위: 플래그 > 환경변수 > 설정 파일 | 단일 클러스터 대상. 초기 단순하게 시작하고 추후 보완. |
