# actorbase 요구사항 문서

## 1. 배경 및 동기

### 1.1 현황

S3 호환 분산 저장소의 object metadata를 MongoDB sharded cluster로 운영 중이다.

### 1.2 문제점

**샤딩 유연성 부족**
- MongoDB의 청크 split/migration이 순수하게 데이터 크기 기반으로 동작한다.
- 특정 파티션에 부하가 집중되는 hotspot 상황에 대응하기 어렵다.
- 운영자가 부하 기반으로 파티션을 직접 조정할 수단이 없다.

**동시성 모델의 복잡성**
- 단일 스레드 모델로 작성하면 trivial한 로직이 MongoDB에서는 CAS(Compare-And-Swap)와 트랜잭션으로 복잡해진다.
- 코드 복잡도 증가 및 성능 저하를 유발한다.

**운영 부담**
- MongoDB sharded cluster 운영에 상당한 운영 지식과 툴링이 필요하다.

### 1.3 목표

actorbase는 사용자가 **자신만의 분산 key-value 저장소**를 손쉽게 구축할 수 있도록 프레임워크와 시스템 컴포넌트를 함께 제공한다.

사용자는 비즈니스 로직을 Actor로 구현하고, actorbase가 제공하는 Partition Server(PS)·Partition Manager(PM)·SDK를 배포·활용하면 자신의 데이터 구조가 분산 환경에서 수평 확장하며 동작한다.

---

## 2. 설계 가정

이 절의 가정들은 설계, 구현, 테스트 전반의 기준이 된다. 가정을 벗어나는 환경에서의 동작은 보장하지 않는다.

### 2.1 배포 환경

- **기업 내부 네트워크**에서만 운영된다. 공개 인터넷에 노출되는 환경은 고려하지 않는다.
- 네트워크 경계 보안(방화벽, VPN 등)은 인프라 수준에서 갖춰져 있다고 가정한다.
- 노드 간 통신에 별도의 mTLS나 암호화를 기본 제공하지 않는다. 필요 시 인프라 레벨에서 처리한다.

### 2.2 장애 모델

- **Crash-stop 장애**만 고려한다. 노드는 정상 동작하거나 완전히 멈추는 두 가지 상태만 가진다.
- **비잔틴 장애(Byzantine failure)**는 고려하지 않는다. 악의적이거나 임의의 잘못된 응답을 보내는 노드는 없다고 가정한다.
- 클러스터 내 노드들은 신뢰할 수 있다고 가정한다.

### 2.3 규모

- 노드 수 기준으로 **수백~약 1,000대** 규모를 대상으로 한다.
- 수만 대 이상의 초대형 클러스터는 설계 목표에 포함하지 않는다.
- 이 규모 가정은 etcd lease 기반 멤버십, 단일 PM 인스턴스 등 여러 설계 결정의 근거가 된다.

---

## 3. 공통 개발 규칙

### 3.1 설계

- **Clean Architecture**를 기준으로 설계를 검토한다.
  - 비즈니스 로직(domain)이 인프라(gRPC, etcd, 파일시스템 등)에 의존하지 않도록 의존성 방향을 안쪽으로 유지한다.
  - Use case, Entity, Interface Adapter, Infrastructure 레이어 경계를 의식하며 구조를 잡는다.
- 설계 결정 사항은 반드시 문서로 남긴다. 구현 전에 문서를 먼저 작성하고 검토한다.

### 3.2 구현

- **SOLID 원칙**을 가급적 준수한다.
  - **S** (Single Responsibility): 하나의 타입/함수는 하나의 책임만 가진다.
  - **O** (Open/Closed): 기존 코드 수정 없이 확장 가능하도록 설계한다.
  - **L** (Liskov Substitution): interface를 구현한 타입은 상호 대체 가능해야 한다.
  - **I** (Interface Segregation): 사용하지 않는 메서드를 강제하는 큰 interface보다 작고 구체적인 interface를 선호한다.
  - **D** (Dependency Inversion): 구체 타입이 아닌 interface에 의존한다.

---

## 4. 시스템 개요

### 4.1 핵심 설계 원칙

- **Actor = Partition**: 하나의 Actor가 하나의 파티션(shard)을 담당하며, 특정 key range를 관리한다.
- **단일 스레드 Actor**: Actor 내부는 단일 스레드로 동작하므로 CAS/트랜잭션 없이 일관성을 보장한다.
- **플러그인 방식 Persistent Layer**: WAL과 Checkpoint 저장 백엔드를 Go interface로 추상화하여 사용자가 자신의 환경에 맞게 구현한다.
- **기존 라이브러리 활용**: gRPC, etcd 등 검증된 라이브러리를 기반으로 구현한다.

### 4.2 시스템 구성

actorbase는 세 가지 컴포넌트로 이루어진다.

| 컴포넌트 | 형태 | 역할 |
|---|---|---|
| **Partition Server (PS)** | 실행 바이너리 (daemon) | Actor 인스턴스 호스팅, 데이터 요청 처리 |
| **Partition Manager (PM)** | 실행 바이너리 (daemon) | 클러스터 토폴로지 관리, split/migrate 조율, 라우팅 배포 |
| **SDK** | Go 라이브러리 | 사용자 애플리케이션에 임베드, 요청을 적절한 PS로 라우팅 |

### 4.3 사용자 책임

actorbase를 활용하기 위해 사용자가 해야 할 일:

**구현 (Go 코드)**
- `Actor[Req, Resp]` 인터페이스 구현 — 비즈니스 로직 정의
- `WALStore` 인터페이스 구현 — WAL 저장 백엔드 (예: Redis Stream)
- `CheckpointStore` 인터페이스 구현 — 스냅샷 저장 백엔드 (예: S3)
- `Codec` 인터페이스 구현 또는 기본 제공 `adapter/json` 사용

**배포 (인프라)**
- PS 인스턴스를 노드마다 기동 — `WALStore`, `CheckpointStore`, `ActorFactory`, `Codec` 주입
- PM 인스턴스를 기동 — etcd 주소 설정
- 사용자 애플리케이션에 SDK 임베드 — PM 주소 설정

---

## 5. Actor 모델

### 5.1 Actor 인터페이스

사용자는 아래 Go interface를 구현하여 Actor를 정의한다.

```go
// Actor는 하나의 파티션(key range)을 담당하는 비즈니스 로직 단위.
// 파티션당 하나의 인스턴스가 생성되며, 단일 스레드로 실행된다.
// Req는 수신하는 요청 타입, Resp는 응답 타입이다.
type Actor[Req, Resp any] interface {
    // Receive는 요청을 처리하고 응답과 WAL 엔트리를 반환한다.
    // walEntry가 nil이면 read-only 연산 (WAL 기록 안 함).
    // walEntry가 있으면 프레임워크가 WALStore에 기록한 뒤 응답을 반환한다.
    Receive(ctx Context, req Req) (resp Resp, walEntry []byte, err error)

    // Replay는 WAL 엔트리 하나를 상태에 적용한다. 복구 시 호출.
    Replay(entry []byte) error

    // Snapshot은 현재 상태를 직렬화한다. (체크포인트용)
    Snapshot() ([]byte, error)

    // Restore는 Snapshot 데이터로 상태를 복원한다.
    Restore(data []byte) error

    // Split은 splitKey 기준으로 상위 절반 상태를 직렬화하여 반환하고,
    // 자신의 상태에서 해당 데이터를 제거한다. 파티션 split 시 호출.
    Split(splitKey string) (upperHalf []byte, err error)
}

// ActorFactory는 파티션 ID마다 새 Actor 인스턴스를 생성하는 함수.
type ActorFactory[Req, Resp any] func(partitionID string) Actor[Req, Resp]

// Context는 Actor.Receive 호출 시 프레임워크가 주입하는 런타임 정보.
type Context interface {
    PartitionID() string
    Logger() *slog.Logger
}
```

### 5.2 사용 예시

```go
type BucketRequest struct {
    Op  string // "get" | "put" | "delete"
    Key string
    Meta *ObjectMeta
}

type BucketResponse struct {
    Meta *ObjectMeta
}

type BucketActor struct {
    objects map[string]*ObjectMeta
}

func (a *BucketActor) Receive(ctx provider.Context, req BucketRequest) (BucketResponse, []byte, error) {
    switch req.Op {
    case "get":
        obj, ok := a.objects[req.Key]
        if !ok {
            return BucketResponse{}, nil, provider.ErrNotFound
        }
        return BucketResponse{Meta: obj}, nil, nil // read: walEntry = nil

    case "put":
        a.objects[req.Key] = req.Meta
        entry := marshalPutOp(req) // 사용자 정의 직렬화
        return BucketResponse{}, entry, nil // write: walEntry 반환

    case "delete":
        delete(a.objects, req.Key)
        entry := marshalDeleteOp(req)
        return BucketResponse{}, entry, nil
    }
    return BucketResponse{}, nil, fmt.Errorf("unknown op: %s", req.Op)
}
```

### 5.3 Actor 생명주기

- Actor는 활성 상태일 때 메모리에 상주한다.
- 비활성(idle) 상태가 되면 checkpoint를 저장하고 메모리에서 evict된다.
- 해당 key range로 요청이 들어오면 CheckpointStore에서 상태를 로드하고 WAL을 replay하여 재활성화된다.
- Actor는 write-back 캐시처럼 동작한다.

### 5.4 Actor 실행 모델

- Actor당 단일 goroutine(mailbox 기반) 실행을 보장한다.
- Actor 내부에서 CAS나 트랜잭션 없이 데이터를 직접 수정할 수 있다.
- 하나의 PS가 여러 Actor(파티션)를 호스팅한다.
- write 연산의 WAL 기록은 group commit으로 배치 처리하여 처리량을 향상시킨다.

---

## 6. Persistent Layer

### 6.1 인터페이스 정의

영속성 백엔드는 **WAL**과 **Checkpoint** 두 가지로 분리된 인터페이스로 추상화한다. 성격과 적합한 백엔드가 다르기 때문이다.

```go
// WALStore는 Write-Ahead Log 저장소의 추상화.
// append-only이며 LSN 기반으로 순차 조회한다.
// 예: Redis Stream, Kafka
type WALStore interface {
    Append(ctx context.Context, partitionID string, data []byte) (lsn uint64, err error)
    ReadFrom(ctx context.Context, partitionID string, fromLSN uint64) ([]WALEntry, error)
    TrimBefore(ctx context.Context, partitionID string, lsn uint64) error
}

// CheckpointStore는 Actor 상태 스냅샷 저장소의 추상화.
// 파티션당 하나의 스냅샷을 유지한다.
// 예: S3, 분산 파일시스템
type CheckpointStore interface {
    Save(ctx context.Context, partitionID string, data []byte) error
    Load(ctx context.Context, partitionID string) ([]byte, error)
    Delete(ctx context.Context, partitionID string) error
}
```

### 6.2 저장 대상

| 저장소 | 저장 내용 | 특성 |
|---|---|---|
| WALStore | Checkpoint 이후의 write 연산 로그 | append-only, LSN 기반 순차 조회 |
| CheckpointStore | Actor 상태의 스냅샷 + checkpoint LSN | 파티션당 1개 (덮어쓰기) |

---

## 7. 장애 복구

### 7.1 복구 전략

Checkpoint와 WAL을 함께 사용한다. 둘은 대안이 아닌 상호 보완 관계다.

**복구 흐름:**
1. 노드 장애 감지 (etcd lease 만료)
2. 해당 파티션 접근 시 PS가 CheckpointStore에서 마지막 **Checkpoint** 로드
3. Checkpoint의 LSN 이후의 **WAL replay**
4. Actor가 장애 직전 상태로 완전히 복구됨

이는 PostgreSQL, RocksDB 등 주요 데이터베이스와 동일한 표준 접근 방식이다.

### 7.2 체크포인트 주기

- idle Actor eviction 시 자동으로 checkpoint를 저장한다.
- eviction 기준(idle timeout)과 주기는 PS Config로 설정한다.
- 구체적인 기본값은 구현 단계에서 벤치마크로 결정한다.

---

## 8. 클러스터 관리

### 8.1 클러스터 메타데이터 저장소

**etcd**를 클러스터 메타데이터 저장소로 사용한다.

etcd에 저장되는 정보:
- 노드 등록 정보 (`/actorbase/nodes/{nodeID}`)
- 라우팅 테이블 (`/actorbase/routing`)

### 8.2 클러스터 멤버십

별도의 gossip 라이브러리 없이 **etcd lease**를 활용하여 클러스터 멤버십을 관리한다.

**동작 방식:**
1. PS 기동 시 etcd에 자신의 정보(주소, 포트 등)를 TTL lease와 함께 등록
2. PS는 주기적으로 lease를 갱신(keepalive)
3. PS 장애 시 heartbeat가 끊기면 TTL 만료 후 lease 자동 삭제
4. PM은 etcd watch로 멤버십 변경을 감지하고 RebalancePolicy를 트리거

gossip 방식 대비 **강한 일관성(linearizable)**으로 멤버십 상태를 확인할 수 있으며, 대상 노드 규모(수백~1,000 노드)에서 충분한 성능을 제공한다.

### 8.3 Partition Manager (PM)

파티션 토폴로지 관리를 담당하는 별도의 daemon 프로세스다.

**PM 책임:**
- 운영자 또는 자동화 정책으로부터 파티션 split/migration 요청 수신
- 담당 Partition Server에 실행 명령 전송 및 조율
- etcd의 라우팅 테이블 갱신
- SDK에 라우팅 테이블 변경 push (gRPC streaming)

**PM 가용성:**
- 초기에는 단일 인스턴스로 운영한다.
- PM이 다운되어도 PS는 계속 동작하고 SDK는 캐시된 라우팅으로 요청을 처리한다.
- PM HA는 향후 과제로 미룬다.

---

## 9. 클라이언트 라우팅

### 9.1 라우팅 방식

- SDK는 etcd에 직접 접근하지 않는다.
- **SDK가 PM에 접속하여 WatchRouting gRPC 스트림으로 라우팅 테이블을 구독한다.**
- 라우팅 테이블은 SDK 로컬에 캐시한다. (atomic swap)
- 파티션 토폴로지가 변경되면 PM이 구독 중인 모든 SDK에 즉시 push한다.
- `ErrPartitionMoved` / `ErrPartitionNotOwned` 수신 시 SDK가 재시도하며 캐시 갱신을 기다린다.

PS는 etcd를 직접 watch하여 라우팅 테이블을 수신한다. SDK는 PM을 통해서만 수신한다.

### 9.2 Client API

- **Go SDK만 제공**한다.
- 초기 범위에서는 외부 클라이언트를 위한 gRPC나 HTTP API를 제공하지 않는다.
- SDK는 사용자 애플리케이션과 같은 Go 프로세스에 라이브러리로 임베드된다.

---

## 10. 파티션 Split / Migration

### 10.1 지원 모드

두 가지 모드를 지원한다.

| 모드 | 설명 |
|---|---|
| **자동 (AutoRebalancePolicy)** | PM이 클러스터 이벤트와 부하 메트릭을 기반으로 자동으로 split/migration 트리거 |
| **수동 (ManualPolicy + abctl)** | 운영자가 `abctl` CLI를 통해 명시적으로 split/migrate 명령 발행 |

### 10.2 Split 동작

- splitKey 기준으로 파티션을 두 개로 분리한다.
- 분리 중 해당 파티션 요청은 일시 대기된다.
- 분리 후 두 파티션은 동일 PS에 위치하며, 필요 시 이후 Migration으로 재배치한다.

### 10.3 Migration 동작

- source PS에서 파티션을 evict하고 target PS에서 로드한다.
- Migration 중 해당 파티션은 Draining 상태로 표시되어 SDK가 `ErrPartitionBusy`를 수신한다.
- PS 간 직접 데이터 전송 없이 CheckpointStore를 중간 저장소로 활용한다.

---

## 11. 전체 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│                  사용자 애플리케이션                          │
│  (BucketActor 구현, SDK 임베드)                              │
└───────────────────────────┬─────────────────────────────────┘
                            │ Go SDK
                            │ (key → PS 라우팅)
                            ▼
                   ┌─────────────────┐
                   │      SDK        │◀─── WatchRouting (gRPC stream)
                   │ - 로컬 라우팅   │
                   │   테이블 캐시   │
                   └────────┬────────┘
                            │ gRPC (PartitionService.Send)
                            ▼
┌───────────────────────────────────────────────────────────┐
│                 Partition Server (PS)                      │
│  - ActorHost: Actor 생명주기 관리                          │
│  - mailbox: Actor당 단일 스레드 실행                       │
│  - WALFlusher: WAL group commit                           │
│  - EvictionScheduler: idle Actor eviction                 │
│                                                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐               │
│  │ Actor A  │  │ Actor B  │  │ Actor C  │  ...           │
│  │[a, d)    │  │[d, m)    │  │[m, ∞)    │               │
│  └──────────┘  └──────────┘  └──────────┘               │
└──────────┬──────────────────────────┬─────────────────────┘
           │ WALStore                 │ CheckpointStore
           ▼                          ▼
   ┌──────────────┐          ┌──────────────────┐
   │ WAL Backend  │          │ Checkpoint        │
   │ (예: Redis   │          │ Backend           │
   │  Stream)     │          │ (예: S3)          │
   └──────────────┘          └──────────────────┘

┌────────────────────────────────────────────────────┐
│              Partition Manager (PM)                │
│  - RoutingTableStore: etcd 라우팅 테이블 관리       │
│  - MembershipWatcher: 노드 join/leave 감지         │
│  - Splitter / Migrator: split·migrate 조율         │
│  - WatchRouting: SDK에 라우팅 변경 push (fanout)   │
│  - RebalancePolicy: 자동/수동 rebalance 전략       │
└──────────────────────┬─────────────────────────────┘
      PartitionControlService (gRPC)  │  etcd read/write
      ExecuteSplit / MigrateOut /     │
      PreparePartition                │
                       ▼              ▼
              [PS]            ┌──────────────┐
                              │     etcd     │
                              │ - 노드 레지  │
                              │   스트리     │
                              │ - 라우팅     │
                              │   테이블     │
                              └──────────────┘
                                     ▲
                              PS가 etcd watch로
                              라우팅 테이블 수신
```

---

## 12. 활용 예정 라이브러리

| 라이브러리 | 용도 |
|---|---|
| `google.golang.org/grpc` | 노드 간 통신 (data plane / management plane / control plane) |
| `go.etcd.io/etcd/client/v3` | 클러스터 메타데이터 저장소 및 멤버십 관리 |

---

## 13. 미결정 사항 (구현 단계 결정)

| 항목 | 내용 |
|---|---|
| PM 고가용성 | 다중 PM 인스턴스 구성 방식 |
| WAL 엔트리 포맷 | 사용자가 직렬화하는 WAL 데이터의 권장 포맷 가이드 |
| Actor mailbox 크기 기본값 | 버퍼링 채널 크기. 벤치마크로 결정. |
| WALFlusher 배치 기본값 | flushSize, flushInterval. 벤치마크로 결정. |
| idle eviction 기본값 | idleTimeout, evictInterval 기본값 |
| etcd lease TTL 기본값 | 장애 감지 지연과 네트워크 불안정 내성의 균형 |
| shutdown EvictAll timeout | PS graceful shutdown 시 ActorHost.EvictAll 제한 시간 |
| AutoRebalancePolicy 임계치 | split 트리거 QPS 임계치, 메트릭 수집 방식 |
