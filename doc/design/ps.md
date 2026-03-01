# ps 패키지 설계

Partition Server(PS) 조립 패키지. 내부 컴포넌트(`engine`, `cluster`, `transport`)를 조합하여 PS를 구성하고 기동·종료를 관리한다.

의존성: `internal/engine`, `internal/cluster`, `internal/transport`, `provider`

파일 목록:
- `config.go`            — Config: PS 설정 및 의존성 주입 구조체
- `server.go`            — Server: 컴포넌트 조립 및 생명주기 관리
- `partition_handler.go` — PartitionService gRPC 핸들러 (SDK → PS, data plane)
- `control_handler.go`   — PartitionControlService gRPC 핸들러 (PM → PS, control plane)

---

## 이전 설계 변경 사항

ps 설계 과정에서 `internal/engine` 패키지의 `ActorHost`에 두 개의 메서드가 추가된다.

### internal/engine/host.go — Split, Activate 추가

```go
// Split은 partitionID Actor를 splitKey 기준으로 두 파티션으로 분리한다.
// 내부적으로 Actor.Split(splitKey)를 호출하여 상태를 분리한다.
// 분리 완료 후 두 Actor(원본 하위 절반 + 신규 상위 절반) 모두 ActorHost에 활성 상태로 등록된다.
//
// 실행 순서:
//   1. 원본 Actor의 mailbox에 split 신호를 보내고 처리 대기
//   2. Actor.Split(splitKey) → upperHalf
//   3. 하위 파티션 checkpoint 저장 (원본 Actor의 변경된 상태)
//   4. Factory(newPartitionID)로 신규 Actor 생성
//   5. newActor.Restore(upperHalf)
//   6. 상위 파티션 checkpoint 저장
//   7. 신규 Actor mailbox 생성 후 actors 맵에 등록
func (h *ActorHost[Req, Resp]) Split(ctx context.Context, partitionID, splitKey, newPartitionID string) error

// Activate는 partitionID Actor를 명시적으로 활성화한다.
// 이미 활성화된 경우 no-op.
// migration target PS에서 PreparePartition 처리 시 사용한다.
//
// 실행 순서:
//   CheckpointStore.Load → actor.Restore → WAL replay → mailbox 등록
func (h *ActorHost[Req, Resp]) Activate(ctx context.Context, partitionID string) error
```

> `Split`은 Actor goroutine(mailbox)에서 실행된다. split 신호를 mailbox에 넣어 순서를 보장한다. 일반 Receive 메시지와 동일한 채널을 사용하되 특수 신호 타입으로 구분한다.

---

## config.go

```go
// Config는 PS 생성에 필요한 모든 설정과 의존성을 담는다.
type Config[Req, Resp any] struct {
    // ─── 필수 (사용자 제공) ───────────────────────────────────────

    NodeID   string // 클러스터 내 유일한 PS 식별자
    Addr     string // gRPC 수신 주소 ("host:port"). etcd에 등록되어 다른 노드가 접속에 사용.

    EtcdEndpoints []string // etcd 엔드포인트 목록

    Factory         provider.ActorFactory[Req, Resp]
    Codec           provider.Codec        // SDK와 동일한 구현체를 주입해야 한다
    WALStore        provider.WALStore
    CheckpointStore provider.CheckpointStore

    // ─── 선택 (기본값 있음) ───────────────────────────────────────

    Metrics provider.Metrics // nil이면 no-op 구현체 사용

    // ActorHost 설정
    MailboxSize   int           // 기본값: 구현 단계에서 벤치마크로 결정
    FlushSize     int           // WAL 배치 최대 크기. 기본값: 구현 단계에서 결정
    FlushInterval time.Duration // WAL 배치 최대 대기 시간. 기본값: 구현 단계에서 결정

    // EvictionScheduler 설정
    IdleTimeout   time.Duration // Actor가 이 시간 동안 메시지 없으면 evict. 기본값: 5분
    EvictInterval time.Duration // eviction 검사 주기. 기본값: 1분

    // etcd 설정
    EtcdLeaseTTL time.Duration // 노드 lease TTL. 기본값: 10초
}
```

---

## server.go

```go
// Server는 Partition Server의 진입점.
// Config를 받아 내부 컴포넌트를 조립하고 gRPC 서버를 기동·관리한다.
type Server[Req, Resp any] struct {
    cfg       Config[Req, Resp]
    actorHost *engine.ActorHost[Req, Resp]
    registry  *cluster.NodeRegistry
    rtStore   *cluster.RoutingTableStore
    grpcSrv   *grpc.Server
    routing   atomic.Pointer[domain.RoutingTable] // 현재 라우팅 테이블. lock-free 읽기.
}

// NewServer는 Config를 검증하고 컴포넌트를 조립한다.
// etcd 연결, gRPC 서버 생성까지 수행하지만 Start 호출 전까지 수신은 시작하지 않는다.
func NewServer[Req, Resp any](cfg Config[Req, Resp]) (*Server[Req, Resp], error)

// Start는 PS를 기동한다. ctx 취소 시 graceful shutdown 후 반환한다.
func (s *Server[Req, Resp]) Start(ctx context.Context) error
```

### 컴포넌트 조립 (NewServer)

```
NewServer:
  1. cfg 검증 (필수 필드 누락 확인)
  2. etcd 클라이언트 생성
  3. ActorHost 생성  (engine.NewActorHost)
  4. NodeRegistry 생성  (cluster.NewNodeRegistry)
  5. RoutingTableStore 생성  (cluster.NewRoutingTableStore)
  6. gRPC 서버 생성  (transport.NewGRPCServer)
  7. PartitionService 핸들러 등록
  8. PartitionControlService 핸들러 등록
```

### 기동 순서 (Start)

```
Start:
  1. go registry.Register(ctx, myNode)    // etcd 등록 + keepalive 시작
  2. go s.watchRouting(ctx)               // RoutingTable watch (초기값 즉시 수신)
  3. <초기 RoutingTable 수신 대기>         // 준비 전 요청 수락 방지
  4. grpcSrv.Serve(listener)              // gRPC 수신 시작 (비동기)
  5. go evictionScheduler.Start(ctx)      // idle Actor eviction 시작
  6. <-ctx.Done()
  7. graceful shutdown (아래 참조)
```

> **초기 RoutingTable 수신 대기**: `RoutingTableStore.Watch`는 시작 시 현재 테이블을 즉시 전달한다 (cluster.md 참조). 빈 클러스터(테이블 없음)인 경우에도 Watch가 nil을 전달하는 시점을 기다려 준비 완료로 간주한다. 대기 없이 요청을 수락하면 라우팅 검증이 불가능하다.

### 종료 순서 (graceful shutdown)

```
ctx 취소 시:
  1. grpcSrv.GracefulStop()             // 진행 중인 RPC 완료 대기 후 수신 중단
  2. actorHost.EvictAll(shutdownCtx)    // 모든 Actor checkpoint 저장
  3. registry.Deregister(ctx, nodeID)  // etcd lease 즉시 revoke
```

> `EvictAll`에는 별도 `shutdownCtx` (예: 30초 타임아웃)를 사용한다. 원래 ctx가 이미 취소됐기 때문이다.

### watchRouting (내부)

```go
func (s *Server[Req, Resp]) watchRouting(ctx context.Context) {
    ch := s.rtStore.Watch(ctx)
    for rt := range ch {
        s.routing.Store(rt) // atomic swap. 읽기 측 잠금 불필요.
    }
}
```

---

## partition_handler.go

SDK → PS data plane 핸들러. `PartitionService.Send` RPC를 구현한다.

```go
// partitionHandler는 PartitionService gRPC 핸들러.
type partitionHandler[Req, Resp any] struct {
    host    *engine.ActorHost[Req, Resp]
    routing *atomic.Pointer[domain.RoutingTable] // Server와 공유
    codec   provider.Codec
    nodeID  string
}

// Send는 partitionID의 Actor에 요청을 전달하고 응답을 반환한다.
func (h *partitionHandler[Req, Resp]) Send(
    ctx context.Context,
    req *pb.SendRequest,
) (*pb.SendResponse, error)
```

### Send 처리 흐름

```
Send(req):
  1. routing = h.routing.Load()
  2. entry, ok = routing.LookupByPartition(req.PartitionID)
     └ ok == false → gRPC UNAVAILABLE (ErrPartitionNotOwned)
  3. entry.Node.ID != h.nodeID
     └ true → gRPC UNAVAILABLE (ErrPartitionNotOwned)
  4. entry.PartitionStatus == Draining
     └ true → gRPC RESOURCE_EXHAUSTED (ErrPartitionBusy)
  5. codec.Unmarshal(req.Payload, &userReq)
  6. resp, err = actorHost.Send(ctx, req.PartitionID, userReq)
     └ err != nil → transport.ToGRPCStatus(err)
  7. payload, _ = codec.Marshal(resp)
  8. return &pb.SendResponse{Payload: payload}
```

> 3번에서 노드 ID를 비교하는 이유: 라우팅 테이블에는 파티션이 존재하지만 다른 노드를 가리킬 수 있다. (migration 완료 후 라우팅이 갱신됐으나 SDK 캐시가 아직 오래된 경우) PS가 자신이 담당하지 않는 파티션을 실수로 처리하지 않도록 명시적으로 검증한다.

---

## control_handler.go

PM → PS control plane 핸들러. `PartitionControlService` RPC 세 개를 구현한다.

```go
// controlHandler는 PartitionControlService gRPC 핸들러.
type controlHandler[Req, Resp any] struct {
    host   *engine.ActorHost[Req, Resp]
    nodeID string
}

func (h *controlHandler[Req, Resp]) ExecuteSplit(
    ctx context.Context,
    req *pb.ExecuteSplitRequest,
) (*pb.ExecuteSplitResponse, error)

func (h *controlHandler[Req, Resp]) ExecuteMigrateOut(
    ctx context.Context,
    req *pb.ExecuteMigrateOutRequest,
) (*pb.ExecuteMigrateOutResponse, error)

func (h *controlHandler[Req, Resp]) PreparePartition(
    ctx context.Context,
    req *pb.PreparePartitionRequest,
) (*pb.PreparePartitionResponse, error)
```

### ExecuteSplit 처리 흐름

```
ExecuteSplit(req):
  1. actorHost.Split(ctx, req.PartitionID, req.SplitKey, req.NewPartitionID)
     └ 내부: Actor.Split → 양쪽 checkpoint 저장 → 두 Actor 모두 활성 등록
  2. 에러 없으면 success 반환
```

> split 실행의 모든 복잡성은 `ActorHost.Split`에 캡슐화된다. control_handler는 단순 위임만 한다.

### ExecuteMigrateOut 처리 흐름

```
ExecuteMigrateOut(req):
  1. actorHost.Evict(ctx, req.PartitionID)
     └ 내부: 최종 checkpoint 저장 → Actor evict
  2. 에러 없으면 success 반환
```

> `Evict`는 이미 checkpoint를 저장하므로 별도 처리 없다. Target PS의 `PreparePartition`은 이 checkpoint를 읽어 상태를 복원한다.

### PreparePartition 처리 흐름

```
PreparePartition(req):
  1. actorHost.Activate(ctx, req.PartitionID)
     └ 내부: CheckpointStore.Load → Restore → WAL replay → 등록
  2. 에러 없으면 success 반환
```

> `Activate`는 이미 활성 Actor가 있으면 no-op이므로 PM의 재시도에 멱등적으로 동작한다.

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| 라우팅 테이블 보관 방식 | `atomic.Pointer[domain.RoutingTable]` | 매 요청마다 호출되는 핫패스에서 잠금 없이 읽기 가능 |
| 초기 라우팅 수신 대기 | gRPC Serve 전에 첫 Watch 이벤트 대기 | 라우팅 없이 요청 수락 시 모든 요청이 ErrPartitionNotOwned |
| graceful shutdown 순서 | GracefulStop → EvictAll → Deregister | RPC 완료 후 checkpoint 저장, 그 다음 etcd에서 제거. 역순이면 장애 노드로 오인 가능. |
| 파티션 소유 검증 위치 | partitionHandler (gRPC 핸들러) | engine(ActorHost)은 소유 개념 없이 partitionID를 키로만 관리. 라우팅 검증은 PS 레이어 책임. |
| Split/Activate 위치 | engine.ActorHost 메서드로 추가 | Actor 생명주기 조작(상태 분리, checkpoint)은 engine 책임. PS는 단순 위임. |
| shutdown 타임아웃 | EvictAll에 별도 context 사용 | 기동 ctx가 취소됐으므로 deadline 없는 context를 재사용할 수 없음 |

---

## 알려진 한계

- **Split 중 mailbox 중단**: `ActorHost.Split` 실행 중 split 파티션의 mailbox가 잠시 중단된다. 이 시간 동안 해당 파티션 요청은 대기한다. 중단 시간은 `Actor.Split` 수행 시간 + checkpoint 저장 시간에 비례하며, 구현 후 측정이 필요하다.
- **Deregister 실패 시 lease 잔존**: graceful shutdown에서 `Deregister`가 실패하면 etcd lease가 TTL까지 남아 PM이 해당 노드를 장애로 감지하는 데 지연이 생긴다. TTL은 구현 단계에서 결정한다.
- **EvictAll timeout 기본값 미결정**: shutdown 시 EvictAll에 사용할 context timeout 기본값은 구현 단계에서 Actor checkpoint 저장 성능을 측정한 후 결정한다.
- **PartitionService에 인증 없음**: 현재 설계에서 PartitionService는 인증/인가 없이 모든 클라이언트의 Send를 수락한다. 클러스터 내부망 환경을 가정하며, mTLS 등 인증은 향후 과제.
