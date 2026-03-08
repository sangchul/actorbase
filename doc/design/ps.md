# ps 패키지 설계

Partition Server(PS) 조립 패키지. 내부 컴포넌트(`engine`, `cluster`, `transport`)를 조합하여 PS를 구성하고 기동·종료를 관리한다.

의존성: `internal/engine`, `internal/cluster`, `internal/transport`, `provider`

파일 목록:
- `config.go`            — BaseConfig + TypeConfig[Req,Resp]: PS 설정 구조체
- `server.go`            — ServerBuilder, Register[Req,Resp](), Server: 조립 및 생명주기 관리
- `partition_handler.go` — PartitionService gRPC 핸들러 (SDK → PS, data plane), non-generic
- `control_handler.go`   — PartitionControlService gRPC 핸들러 (PM → PS, control plane), non-generic

---

## 이전 설계 변경 사항

ps 설계 과정에서 `internal/engine` 패키지의 `ActorHost`에 두 개의 메서드가 추가된다.

### internal/engine/host.go — Split, Activate 추가

```go
// Split은 partitionID Actor를 splitKey 기준으로 두 파티션으로 분리한다.
// Actor가 비활성이면 먼저 getOrActivate로 활성화한다.
// 분리 완료 후 두 Actor(원본 하위 절반 + 신규 상위 절반) 모두 ActorHost에 활성 상태로 등록된다.
//
// 실행 순서:
//   1. getOrActivate(partitionID)      // 비활성 시 checkpoint + WAL replay 후 활성화
//   2. mailbox.split(splitKey)         // Actor goroutine 내에서 Actor.Split 호출 → upperHalf
//   3. mailbox.checkpoint()            // 하위 파티션 checkpoint 저장
//   4. saveRawCheckpoint(newPartitionID, lsn=0, upperHalf)  // 상위 파티션 checkpoint 저장
//   5. Activate(newPartitionID)        // CheckpointStore에서 로드하여 활성화
func (h *ActorHost[Req, Resp]) Split(ctx context.Context, partitionID, splitKey, newPartitionID string) error

// Activate는 partitionID Actor를 명시적으로 활성화한다.
// 이미 활성화된 경우 no-op.
// migration target PS에서 PreparePartition 처리 시 사용한다.
//
// 실행 순서:
//   CheckpointStore.Load → actor.Restore → WAL replay → mailbox 등록
func (h *ActorHost[Req, Resp]) Activate(ctx context.Context, partitionID string) error
```

> `Split`은 Actor goroutine(mailbox)에서 실행된다. split 신호를 mailbox에 넣어 순서를 보장한다.

### internal/engine/host.go — WAL partial write 복구

```go
// activate 내부 WAL replay 루프: 마지막 엔트리 실패는 partial write로 간주하고 skip.
for i, e := range entries {
    if err := actor.Replay(e.Data); err != nil {
        if i == len(entries)-1 {
            // crash 중 부분 기록된 마지막 엔트리 → warning 후 skip
            slog.Warn("last WAL entry may be truncated (partial write?), skipping", ...)
            break
        }
        return fmt.Errorf("replay WAL entry LSN=%d: %w", e.LSN, err)
    }
}
```

> SIGKILL 등으로 마지막 WAL 엔트리가 부분 기록되어 JSON이 truncated된 경우에 대한 방어 처리.
> 중간 엔트리 실패는 여전히 에러 반환 (실제 데이터 손상).

---

## config.go

multi-actor-type 지원을 위해 설정이 두 레벨로 분리된다.

```go
// BaseConfig는 PS 인스턴스 전체의 공통 설정을 담는다.
// 모든 actor type이 공유한다.
type BaseConfig struct {
    // ─── 필수 (사용자 제공) ───────────────────────────────────────

    NodeID string // 클러스터 내 유일한 PS 식별자
    Addr   string // gRPC 수신 주소 ("host:port"). etcd에 등록되어 다른 노드가 접속에 사용.

    EtcdEndpoints []string // etcd 엔드포인트 목록

    // ─── 선택 (기본값 있음) ───────────────────────────────────────

    Metrics provider.Metrics

    // ActorHost 설정
    MailboxSize   int           // 기본값: 구현 단계에서 벤치마크로 결정
    FlushSize     int           // WAL 배치 최대 크기
    FlushInterval time.Duration // WAL 배치 최대 대기 시간

    // EvictionScheduler 설정
    IdleTimeout   time.Duration // Actor가 이 시간 동안 메시지 없으면 evict. 기본값: 5분
    EvictInterval time.Duration // eviction 검사 주기. 기본값: 1분

    // CheckpointScheduler 설정
    CheckpointInterval     time.Duration // 주기적 checkpoint 간격
    CheckpointWALThreshold int           // 이 수만큼 WAL entry가 쌓이면 자동 checkpoint

    // etcd 설정
    EtcdLeaseTTL time.Duration // 노드 lease TTL. 기본값: 10초

    // graceful shutdown 설정
    DrainTimeout    time.Duration // 파티션 선이전 최대 대기 시간. 기본값: 60초
    ShutdownTimeout time.Duration // EvictAll 최대 대기 시간. 기본값: 30초
}

// TypeConfig는 특정 actor type에 대한 설정을 담는다.
// ps.Register[Req, Resp]() 에 전달한다.
type TypeConfig[Req, Resp any] struct {
    // ─── 필수 (사용자 제공) ───────────────────────────────────────

    TypeID  string                           // actor type 식별자 ("kv", "bucket", "object" 등)
    Factory provider.ActorFactory[Req, Resp] // 파티션별 Actor 생성 팩토리
    Codec   provider.Codec                   // SDK와 동일한 구현체를 주입해야 한다

    WALStore        provider.WALStore
    CheckpointStore provider.CheckpointStore

    // ─── 선택 (BaseConfig 기본값 사용) ───────────────────────────

    MailboxSize            int
    FlushSize              int
    FlushInterval          time.Duration
    CheckpointWALThreshold int
}
```

---

## server.go

Go 제네릭 제약으로 인해 타입 소거(type erasure) 패턴을 사용한다.
(제네릭 메서드를 non-generic 타입에 정의할 수 없으므로 package-level 함수로 분리)

```go
// actorDispatcher는 engine.ActorHost[Req,Resp]를 타입 소거한 인터페이스.
// Server가 각 actor type의 ActorHost를 map[string]actorDispatcher로 관리한다.
type actorDispatcher interface {
    Send(ctx context.Context, partitionID string, payload []byte) ([]byte, error)
    Evict(ctx context.Context, partitionID string) error
    EvictAll(ctx context.Context) error
    Activate(ctx context.Context, partitionID string) error
    Split(ctx context.Context, partitionID, splitKey, newPartitionID string) error
    StartSchedulers(ctx context.Context, idleTimeout, evictInterval, checkpointInterval time.Duration)
}

// typedDispatcher는 engine.ActorHost[Req,Resp]를 actorDispatcher로 래핑한다.
// Codec을 이용해 []byte ↔ Req/Resp 변환을 처리한다.
type typedDispatcher[Req, Resp any] struct {
    host  *engine.ActorHost[Req, Resp]
    codec provider.Codec
}

// ServerBuilder는 여러 actor type을 등록한 후 Server를 생성한다.
type ServerBuilder struct {
    base        BaseConfig
    dispatchers map[string]actorDispatcher // typeID → dispatcher
}

// NewServerBuilder는 BaseConfig로 ServerBuilder를 생성한다.
func NewServerBuilder(cfg BaseConfig) *ServerBuilder

// Register는 ServerBuilder에 actor type을 등록한다.
// package-level generic 함수. 제네릭 메서드 제약 우회.
func Register[Req, Resp any](b *ServerBuilder, cfg TypeConfig[Req, Resp]) error

// Build는 ServerBuilder 설정을 검증하고 Server를 생성한다.
// actor type이 하나도 등록되지 않으면 에러를 반환한다.
func (b *ServerBuilder) Build() (*Server, error)

// Server는 Partition Server의 진입점.
// 타입별 dispatcher map을 통해 multi-actor-type 요청을 처리한다.
type Server struct {
    cfg         BaseConfig
    dispatchers map[string]actorDispatcher // typeID → dispatcher
    registry    cluster.NodeRegistry
    rtStore     cluster.RoutingTableStore
    grpcSrv     *grpc.Server
    etcdCli     *clientv3.Client
    routing     atomic.Pointer[domain.RoutingTable]
}

// Start는 PS를 기동한다. ctx 취소 시 graceful shutdown 후 반환한다.
func (s *Server) Start(ctx context.Context) error
```

### 사용 예시

```go
// BaseConfig로 builder 생성
builder := ps.NewServerBuilder(ps.BaseConfig{
    NodeID:        "ps-1",
    Addr:          ":8001",
    EtcdEndpoints: []string{"localhost:2379"},
})

// actor type 등록 (여러 개 가능)
ps.Register(builder, ps.TypeConfig[KVRequest, KVResponse]{
    TypeID:          "kv",
    Factory:         func(_ string) provider.Actor[KVRequest, KVResponse] { return &kvActor{} },
    Codec:           adapterjson.New(),
    WALStore:        walStore,
    CheckpointStore: cpStore,
})

// 두 번째 actor type (예: s3_server)
ps.Register(builder, ps.TypeConfig[BucketRequest, BucketResponse]{
    TypeID:          "bucket",
    Factory:         func(_ string) provider.Actor[BucketRequest, BucketResponse] { return &bucketActor{} },
    Codec:           adapterjson.New(),
    WALStore:        walStore,
    CheckpointStore: cpStore,
})

srv, err := builder.Build()
srv.Start(ctx)
```

### 컴포넌트 조립 (NewServerBuilder + Register + Build)

```
NewServerBuilder(base):
  - dispatchers map 초기화

Register[Req, Resp](b, typeCfg):
  1. typeID 중복 검사
  2. engine.NewActorHost[Req, Resp](engineCfg) 생성
  3. typedDispatcher[Req, Resp]{host, codec} 생성
  4. b.dispatchers[typeID] = dispatcher

Build():
  1. dispatchers가 비어있으면 에러
  2. etcd 클라이언트 생성
  3. NodeRegistry, RoutingTableStore 생성
  4. gRPC 서버 생성 (transport.NewGRPCServer)
  5. partitionHandler{dispatchers} 등록
  6. controlHandler{dispatchers} 등록
  7. Server 반환
```

### 기동 순서 (Start)

```
Start:
  1. cluster.WaitForPM(ctx, etcdCli)      // PM이 etcd에 등록될 때까지 대기 (최대 10초)
  2. go registry.Register(ctx, myNode)    // etcd 등록 + keepalive 시작
  3. rtCh = rtStore.Watch(ctx)            // RoutingTable watch 시작
  4. <초기 RoutingTable 수신 대기>         // 준비 전 요청 수락 방지
  5. go s.watchRouting(ctx, rtCh)         // 이후 라우팅 갱신 백그라운드 처리
  6. grpcSrv.Serve(listener)              // gRPC 수신 시작 (비동기)
  7. for each dispatcher:
       go dispatcher.StartSchedulers(ctx, idleTimeout, evictInterval, checkpointInterval)
  8. <-ctx.Done()
  9. graceful shutdown (아래 참조)
```

> **PM 가드**: PS 기동 시 etcd에 PM presence 키(`/actorbase/pm/`)가 없으면 최대 10초 대기.
> **초기 RoutingTable 수신 대기**: Watch 시작 시 현재 테이블을 즉시 전달. 빈 클러스터도 nil을 받는 시점을 기다려 준비 완료로 간주.

### 종료 순서 (graceful shutdown)

```
ctx 취소 시:
  1. drainPartitions(drainCtx)          // 파티션 선이전: 다른 PS로 migrate 요청
                                        // gRPC 서버가 살아있는 동안 실행해야 PM이
                                        // ExecuteMigrateOut을 호출할 수 있다.
  2. grpcSrv.GracefulStop()             // 진행 중인 RPC 완료 대기 후 수신 중단
  3. for each dispatcher:
       dispatcher.EvictAll(shutdownCtx) // drain 실패 파티션 대비 safety checkpoint
  4. registry.Deregister(ctx, nodeID)  // etcd lease 즉시 revoke
```

> `drainPartitions`에는 `DrainTimeout`(기본 60초) 타임아웃을 사용한다.
> `EvictAll`에는 별도 `ShutdownTimeout`(기본 30초)을 사용한다. 원래 ctx가 이미 취소됐기 때문이다.

### drainPartitions (내부)

PS 종료 전 자신의 파티션을 PM에 위임하는 절차.

```
drainPartitions(ctx):
  1. cluster.GetPMAddr(ctx, etcdCli)   // etcd에서 PM 주소 조회
  2. PMClient 생성 (transport.NewPMClient)
  3. PMClient.ListMembers()            // 가용 노드 목록 조회
  4. routing.Load()에서 내 파티션 추출 (entry.Node.ID == myNodeID)
  5. for each 파티션:
       target = targets[round-robin] (나 제외 active 노드)
       PMClient.RequestMigrate(entry.Partition.ActorType, partitionID, target.NodeID)
  6. 실패 파티션: 로그만 남기고 계속. NodeLeft 후 failoverNode가 처리.
```

### watchRouting (내부)

```go
func (s *Server) watchRouting(ctx context.Context) {
    ch := s.rtStore.Watch(ctx)
    for rt := range ch {
        s.routing.Store(rt) // atomic swap. 읽기 측 잠금 불필요.
    }
}
```

---

## partition_handler.go

SDK → PS data plane 핸들러. **non-generic**. `req.ActorType`으로 적절한 dispatcher를 선택한다.

```go
// partitionHandler는 PartitionService gRPC 핸들러.
// 등록된 dispatcher map을 보유하며 req.ActorType으로 dispatcher를 선택한다.
type partitionHandler struct {
    dispatchers map[string]actorDispatcher
    routing     *atomic.Pointer[domain.RoutingTable]
    nodeID      string
}

func (h *partitionHandler) Send(
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
  3. entry.Node.ID != h.nodeID → gRPC UNAVAILABLE
  4. dispatcher, ok = h.dispatchers[req.ActorType]
     └ ok == false → gRPC NOT_FOUND (알 수 없는 actor type)
  5. payload, err = dispatcher.Send(ctx, req.PartitionID, req.Payload)
  6. return &pb.SendResponse{Payload: payload}
```

---

## control_handler.go

PM → PS control plane 핸들러. **non-generic**. `req.ActorType`으로 dispatcher를 선택한다.

```go
// controlHandler는 PartitionControlService gRPC 핸들러.
type controlHandler struct {
    dispatchers map[string]actorDispatcher
    nodeID      string
}

func (h *controlHandler) ExecuteSplit(ctx, req) (*pb.ExecuteSplitResponse, error)
func (h *controlHandler) ExecuteMigrateOut(ctx, req) (*pb.ExecuteMigrateOutResponse, error)
func (h *controlHandler) PreparePartition(ctx, req) (*pb.PreparePartitionResponse, error)
```

각 핸들러는 `req.ActorType`으로 dispatcher를 조회한 후 해당 dispatcher의 메서드를 호출한다.

### ExecuteSplit 처리 흐름

```
ExecuteSplit(req):
  1. dispatcher = h.dispatchers[req.ActorType]
  2. dispatcher.Split(ctx, req.PartitionID, req.SplitKey, req.NewPartitionID)
```

### ExecuteMigrateOut 처리 흐름

```
ExecuteMigrateOut(req):
  1. dispatcher = h.dispatchers[req.ActorType]
  2. dispatcher.Evict(ctx, req.PartitionID)  // 최종 checkpoint 저장 후 evict
```

### PreparePartition 처리 흐름

```
PreparePartition(req):
  1. dispatcher = h.dispatchers[req.ActorType]
  2. dispatcher.Activate(ctx, req.PartitionID)  // CheckpointStore.Load → Restore → WAL replay
```

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| multi-actor-type 지원 방식 | `actorDispatcher` 인터페이스 + `typedDispatcher[Req,Resp]` | Go 제네릭은 generic method를 허용하지 않아 타입 소거 필요 |
| Register 위치 | package-level generic 함수 | generic method 제약 우회. `Register[Req,Resp](b, cfg)` 형태 |
| 설정 분리 | `BaseConfig` (공유) + `TypeConfig[Req,Resp]` (타입별) | 공통 설정과 타입별 설정의 명확한 분리 |
| dispatcher 라우팅 | `req.ActorType` → `dispatchers[actorType]` | proto에 actor_type 필드 추가로 handlers가 non-generic 가능 |
| WAL/Checkpoint 공유 | 경로는 공유, partitionID UUID로 격리 | 동일 WALStore에 타입별 파티션이 공존해도 디렉토리 충돌 없음 |
| 라우팅 테이블 보관 방식 | `atomic.Pointer[domain.RoutingTable]` | 매 요청마다 호출되는 핫패스에서 잠금 없이 읽기 가능 |
| graceful shutdown 순서 | drainPartitions → GracefulStop → EvictAll(타입별) → Deregister | drain 선행으로 정상 파티션 이전 |
| WAL partial write 복구 | 마지막 엔트리 replay 실패 시 skip | SIGKILL 중 truncated 엔트리 방어. 중간 엔트리 실패는 여전히 에러 |
| 파티션 소유 검증 위치 | partitionHandler (gRPC 핸들러) | engine은 소유 개념 없이 partitionID를 키로만 관리 |
| shutdown 타임아웃 | EvictAll에 별도 context 사용 | 기동 ctx가 취소됐으므로 deadline 없는 context를 재사용할 수 없음 |

---

## 알려진 한계

- **Split 중 mailbox 중단**: split 파티션의 mailbox가 잠시 중단된다. 중단 시간은 `Actor.Split` + checkpoint 저장 시간에 비례.
- **Deregister 실패 시 lease 잔존**: etcd lease가 TTL까지 남아 PM이 해당 노드를 장애로 감지하는 데 지연이 생긴다.
- **EvictAll timeout 기본값**: shutdown 시 EvictAll에 별도 context를 사용한다. 기동 ctx가 이미 취소됐기 때문이다.
- **PartitionService에 인증 없음**: 클러스터 내부망 환경을 가정. mTLS 등 인증은 향후 과제.
