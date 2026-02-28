# 패키지 구조 설계

## 1. Clean Architecture 레이어 매핑

actorbase의 구성 요소를 Clean Architecture 4개 레이어에 매핑한다.

| 레이어 | 역할 | actorbase 구성 요소 |
|---|---|---|
| **Entities** (Domain) | 핵심 비즈니스 개념. 외부에 의존하지 않음 | `Partition`, `KeyRange`, `NodeInfo`, `RoutingTable` |
| **Use Cases** (Application) | 애플리케이션 로직. Domain만 의존 | Actor 생명주기, 요청 라우팅, Split/Migration 조율 |
| **Interface Adapters** | 외부 시스템과 Use Case를 연결 | gRPC 핸들러, etcd 어댑터 |
| **Frameworks & Drivers** | 외부 프레임워크, 라이브러리 | gRPC, etcd 클라이언트, WAL 파일 I/O |

**핵심 의존성 규칙**: 의존성은 항상 안쪽(Domain)을 향한다. Domain은 아무것도 import하지 않는다.

---

## 2. 패키지 구조

```
actorbase/
│
├── provider/                  # 사용자와의 계약 패키지
│   ├── actor.go               # Actor, ActorFactory, Context, Message
│   └── store.go               # Store
│
├── internal/                  # 모듈 내 공유 라이브러리 (외부 import 불가)
│   ├── domain/                # [Entities] 핵심 도메인. 의존성 없음.
│   │   ├── partition.go       # Partition, KeyRange
│   │   ├── node.go            # NodeInfo (주소, 상태)
│   │   └── routing.go         # RoutingTable (KeyRange → NodeInfo 매핑)
│   │
│   ├── engine/                # [Use Cases] Actor 실행 엔진. PS가 사용.
│   │   ├── host.go            # ActorHost: Actor 생명주기 관리 (활성화/비활성화/eviction)
│   │   ├── mailbox.go         # 메시지 큐 (채널 기반, Actor당 1개)
│   │   └── scheduler.go       # idle Actor eviction 스케줄러
│   │
│   ├── cluster/               # [Use Cases] 클러스터 멤버십/라우팅. PS·PM·SDK 공용.
│   │   ├── registry.go        # ClusterRegistry 인터페이스 + etcd lease 구현
│   │   └── membership.go      # MembershipWatcher (etcd watch 기반)
│   │
│   ├── rebalance/             # [Use Cases] Split/Migration 조율. PM이 사용.
│   │   ├── splitter.go        # 파티션 분할 로직
│   │   └── migrator.go        # 파티션 이동 로직
│   │
│   ├── wal/                   # [Frameworks] Write-Ahead Log 구현
│   │   └── wal.go             # append, replay
│   │
│   ├── checkpoint/            # [Frameworks] Checkpoint 관리
│   │   └── manager.go         # 주기적 스냅샷 저장/복원 조율
│   │
│   └── transport/             # [Interface Adapters] gRPC 서버/클라이언트 공통
│       ├── server.go
│       └── client.go
│
├── ps/                        # Partition Server daemon
│   ├── server.go              # engine + transport 조합, gRPC 요청 처리
│   └── config.go              # ps.Config
│
├── pm/                        # Partition Manager daemon
│   ├── server.go              # PM gRPC 서버 (split/migrate 요청 수신 및 조율)
│   └── policy/
│       ├── metric.go          # 메트릭 기반 자동 rebalance 정책
│       └── manual.go          # 수동 명령 처리
│
├── sdk/                       # Go SDK (클라이언트 애플리케이션이 import)
│   └── client.go              # PM에서 라우팅 조회, PS로 요청 전송
│
└── cmd/
    ├── ps/main.go             # Partition Server 실행 바이너리
    ├── pm/main.go             # Partition Manager 실행 바이너리
    └── abctl/main.go          # CLI tool: SDK + 운영 기능 (split, migrate, status 등)
```

---

## 3. 의존성 방향

```
[Frameworks & Drivers]
  internal/wal/
  internal/checkpoint/
        │
        ▼
[Interface Adapters]
  internal/transport/
  internal/cluster/  (etcd 구현 포함)
        │
        ▼
[Use Cases]
  internal/engine/
  internal/rebalance/
        │
        ▼
[Entities]
  internal/domain/
```

**규칙:**
- `internal/domain` → 아무것도 import하지 않는다.
- `internal/engine`, `internal/rebalance` → `internal/domain` + `provider` import.
- `internal/cluster`, `internal/transport` → `internal/domain` import.
- `internal/wal`, `internal/checkpoint` → `provider`(Store) import.
- `ps/`, `pm/`, `sdk/` → `internal/*` + `provider` import.
- `cmd/abctl` → `sdk/` + PM gRPC 클라이언트 import.
- 인터페이스는 별도 `port/` 패키지 없이 **사용하는 패키지 안에 정의**한다.

---

## 4. provider/ 패키지

사용자와의 계약 패키지. 사용자가 구현해야 하는 인터페이스와 프레임워크가 제공하는 인터페이스를 함께 정의한다. 주석으로 역할을 구분한다.

### provider/actor.go

```go
// =============================================
// 사용자가 구현해야 하는 인터페이스
// =============================================

// Actor는 하나의 파티션(key range)을 담당하는 비즈니스 로직 단위.
// 파티션당 하나의 인스턴스가 생성되며, 단일 스레드로 실행된다.
// 사용자가 직접 구현한다.
type Actor interface {
    Receive(ctx Context, msg Message) error
    Snapshot() ([]byte, error)  // 체크포인트용 직렬화
    Restore([]byte) error        // 복구용 역직렬화
}

// ActorFactory는 파티션 ID마다 새 Actor 인스턴스를 생성하는 함수.
// 사용자가 직접 구현한다.
type ActorFactory func(partitionID string) Actor

// =============================================
// 프레임워크가 제공하는 인터페이스 (사용만 함)
// =============================================

// Context는 Actor.Receive 호출 시 프레임워크가 주입하는 런타임 기능.
// 사용자는 구현하지 않고 사용만 한다.
type Context interface {
    Reply(v any)
    PartitionID() string
}

// Message는 Actor에 전달되는 요청. 구체적인 타입은 사용자가 정의한다.
type Message = any
```

### provider/store.go

```go
// =============================================
// 사용자가 구현해야 하는 인터페이스
// =============================================

// Store는 Persistent Layer의 추상화.
// Checkpoint와 WAL을 저장할 백엔드(S3, RocksDB 등)를 사용자가 직접 구현한다.
type Store interface {
    Save(ctx context.Context, key string, data []byte) error
    Load(ctx context.Context, key string) ([]byte, error)
    Delete(ctx context.Context, key string) error
}
```

---

## 5. 주요 패키지별 책임

| 패키지 | 책임 |
|---|---|
| `provider` | 사용자와의 인터페이스 계약 정의 |
| `internal/domain` | 파티션, 노드, 라우팅 테이블 등 핵심 도메인 타입 |
| `internal/engine` | Actor 생명주기(활성화/eviction), mailbox, 단일 스레드 실행 보장 |
| `internal/cluster` | etcd lease 기반 멤버십 등록/감지, 라우팅 테이블 저장/조회 |
| `internal/rebalance` | 파티션 split/migration 실행 조율 |
| `internal/wal` | Write-Ahead Log append/replay |
| `internal/checkpoint` | 주기적 Actor 스냅샷 저장/복원 조율 |
| `internal/transport` | gRPC 서버/클라이언트 공통 구현 |
| `ps` | Partition Server 조립 및 기동 |
| `pm` | Partition Manager 조립 및 기동, rebalance 정책 실행 |
| `sdk` | 클라이언트 앱용 Go SDK (라우팅 조회, 요청 전송) |
| `cmd/ps` | PS 실행 바이너리 |
| `cmd/pm` | PM 실행 바이너리 |
| `cmd/abctl` | 운영 CLI (클러스터 상태 조회, split/migrate 요청, 데이터 읽기/쓰기) |

---

## 6. 미결정 사항

| 항목 | 내용 |
|---|---|
| proto 파일 위치 | `proto/` 최상위 별도 디렉토리 vs `internal/transport/` 안 |
| 오류 타입 | 공개 error 타입을 `provider/`에 둘지, 각 패키지에서 정의할지 |
| 로깅 인터페이스 | 공개 Logger 인터페이스 주입 vs `slog` 직접 사용 |
| 메트릭 수집 | Prometheus 직접 의존 vs 추상화 인터페이스 |
| abctl 설정 | 설정 파일 형식 및 위치 |
