# pm 패키지 설계

Partition Manager(PM) 조립 패키지. 클러스터 멤버십 감시, 라우팅 테이블 관리, SDK 라우팅 push, split/migrate 조율을 담당한다.

의존성: `internal/cluster`, `internal/rebalance`, `internal/transport`, `internal/domain`, `provider`

파일 목록:
- `config.go`           — Config: PM 설정 및 의존성 주입 구조체
- `server.go`           — Server: 컴포넌트 조립 및 생명주기 관리
- `manager_handler.go`  — PartitionManagerService gRPC 핸들러 (SDK/abctl → PM)
- `policy/policy.go`    — RebalancePolicy 인터페이스
- `policy/manual.go`    — ManualPolicy: 자동 rebalance 없음
- `policy/auto.go`      — AutoRebalancePolicy: 메트릭 기반 자동 rebalance

---

## config.go

```go
// Config는 PM 생성에 필요한 모든 설정과 의존성을 담는다.
type Config struct {
    // ─── 필수 (사용자 제공) ───────────────────────────────────────

    ListenAddr    string   // gRPC 수신 주소 ("host:port")
    EtcdEndpoints []string // etcd 엔드포인트 목록

    // ─── 선택 (기본값 있음) ───────────────────────────────────────

    Metrics provider.Metrics      // nil이면 no-op 구현체 사용
    Policy  policy.RebalancePolicy // nil이면 ManualPolicy 사용
}
```

> PM은 PS와 달리 etcd에 자신을 노드로 등록하지 않는다. PM은 조율자이며 파티션을 보유하지 않는다.

---

## server.go

```go
// Server는 Partition Manager의 진입점.
// 컴포넌트를 조립하고 gRPC 서버를 기동·관리한다.
type Server struct {
    cfg          Config
    routingStore *cluster.RoutingTableStore
    nodeRegistry *cluster.NodeRegistry
    membership   *cluster.MembershipWatcher
    splitter     *rebalance.Splitter
    migrator     *rebalance.Migrator
    connPool     *transport.ConnPool
    grpcSrv      *grpc.Server

    // 현재 라우팅 테이블. WatchRouting 스트림 신규 연결 시 즉시 전달용.
    routing atomic.Pointer[domain.RoutingTable]

    // WatchRouting 구독자 관리
    subsMu      sync.RWMutex
    subscribers map[string]*subscriber // clientID → subscriber

    // split/migrate 직렬화. PM 단일 인스턴스이지만 동시 RPC 요청 방어.
    opMu sync.Mutex
}

// subscriber는 WatchRouting 스트림 하나의 상태를 담는다.
type subscriber struct {
    latest atomic.Pointer[domain.RoutingTable]
    notify chan struct{} // 버퍼 크기 1. 새 라우팅 테이블 도착 신호.
}

func NewServer(cfg Config) (*Server, error)

// Start는 PM을 기동한다. ctx 취소 시 graceful shutdown 후 반환한다.
func (s *Server) Start(ctx context.Context) error
```

### 컴포넌트 조립 (NewServer)

```
NewServer:
  1. cfg 검증 (필수 필드 누락 확인)
  2. cfg.Policy == nil이면 ManualPolicy로 대체
  3. etcd 클라이언트 생성
  4. NodeRegistry 생성   (cluster.NewNodeRegistry)
  5. MembershipWatcher 생성  (cluster.NewMembershipWatcher)
  6. RoutingTableStore 생성  (cluster.NewRoutingTableStore)
  7. ConnPool 생성  (transport.NewConnPool)
  8. Splitter 생성  (rebalance.NewSplitter)
  9. Migrator 생성  (rebalance.NewMigrator)
 10. gRPC 서버 생성  (transport.NewGRPCServer)
 11. PartitionManagerService 핸들러 등록
```

### 기동 순서 (Start)

```
Start:
  1. currentRT = routingStore.Load()        // 현재 라우팅 테이블 조회
  2. routing.Store(currentRT)               // 초기값 설정
  3. go s.watchRouting(ctx)                 // etcd watch → 구독자 broadcast
  4. go s.watchMembership(ctx)              // 노드 join/leave → Policy 호출
  5. if currentRT == nil:
       go s.bootstrap(ctx)                  // 첫 PS 등록 대기 후 초기 테이블 생성
  6. grpcSrv.Serve(listener)               // gRPC 수신 시작 (비동기)
  7. <-ctx.Done()
  8. graceful shutdown (아래 참조)
```

### 종료 순서 (graceful shutdown)

```
ctx 취소 시:
  1. grpcSrv.GracefulStop()   // 진행 중인 RPC 완료 대기 후 수신 중단
  2. connPool.Close()          // PS 연결 정리
```

> PM은 자신이 보유한 Actor나 상태가 없으므로 PS보다 종료가 단순하다. 모든 상태는 etcd에 있다.

### watchRouting (내부)

etcd 라우팅 테이블 변경을 감지하여 로컬 캐시를 갱신하고 모든 WatchRouting 구독자에게 broadcast한다.

```go
func (s *Server) watchRouting(ctx context.Context) {
    ch := s.routingStore.Watch(ctx)
    for rt := range ch {
        s.routing.Store(rt)
        s.broadcast(rt)
    }
}
```

### broadcast (내부)

```go
// broadcast는 새 라우팅 테이블을 모든 구독자에게 전달한다.
// 느린 구독자는 최신 값으로 덮어쓰인다. (outdated 업데이트는 버린다)
func (s *Server) broadcast(rt *domain.RoutingTable) {
    s.subsMu.RLock()
    defer s.subsMu.RUnlock()
    for _, sub := range s.subscribers {
        sub.latest.Store(rt)
        select {
        case sub.notify <- struct{}{}:
        default: // 이미 신호가 있으면 추가 신호 불필요
        }
    }
}
```

> 구독자는 `notify` 채널에서 신호를 받으면 `latest`를 읽어 전송한다. 빠른 연속 변경 시 중간 값은 건너뛰고 최신 값만 전달된다. SDK 입장에서는 최신 라우팅 테이블만 있으면 충분하다.

### watchMembership (내부)

```go
func (s *Server) watchMembership(ctx context.Context) {
    ch := s.membership.Watch(ctx)
    for event := range ch {
        switch event.Type {
        case cluster.NodeJoined:
            s.cfg.Policy.OnNodeJoined(ctx, event.Node)
        case cluster.NodeLeft:
            s.cfg.Policy.OnNodeLeft(ctx, event.Node)
        }
    }
}
```

### bootstrap (내부)

빈 클러스터(라우팅 테이블 없음) 시 첫 PS 등록을 기다려 초기 라우팅 테이블을 생성한다.

```go
// bootstrap은 첫 번째 PS가 등록될 때까지 기다린 후 초기 라우팅 테이블을 생성한다.
// 초기 파티션: 전체 키 범위 ["", "") → 첫 번째 PS
// etcd CAS(Compare-And-Swap)로 중복 생성을 방어한다.
func (s *Server) bootstrap(ctx context.Context)
```

---

## manager_handler.go

SDK/abctl → PM management plane 핸들러. `PartitionManagerService` RPC 세 개를 구현한다.

```go
// managerHandler는 PartitionManagerService gRPC 핸들러.
type managerHandler struct {
    server *Server
}

// WatchRouting은 연결 즉시 현재 라우팅 테이블을 전송하고,
// 이후 변경 시마다 스트리밍으로 push한다.
func (h *managerHandler) WatchRouting(
    req *pb.WatchRoutingRequest,
    stream pb.PartitionManagerService_WatchRoutingServer,
) error

// RequestSplit은 partitionID를 splitKey 기준으로 분할하도록 요청한다.
// abctl 또는 AutoRebalancePolicy가 호출한다.
func (h *managerHandler) RequestSplit(
    ctx context.Context,
    req *pb.SplitRequest,
) (*pb.SplitResponse, error)

// RequestMigrate는 partitionID를 targetNodeID로 이동시키도록 요청한다.
func (h *managerHandler) RequestMigrate(
    ctx context.Context,
    req *pb.MigrateRequest,
) (*pb.MigrateResponse, error)
```

### WatchRouting 처리 흐름

```
WatchRouting(req, stream):
  1. sub = &subscriber{notify: make(chan struct{}, 1)}
  2. server.subscribe(req.ClientID, sub)
  3. defer server.unsubscribe(req.ClientID)
  4. current = server.routing.Load()
  5. if current != nil: stream.Send(toProto(current))  // 현재 테이블 즉시 전달
  6. loop:
       select {
       case <-sub.notify:
           rt = sub.latest.Load()
           stream.Send(toProto(rt))
       case <-stream.Context().Done():
           return nil  // 클라이언트 연결 종료
       }
```

### RequestSplit 처리 흐름

```
RequestSplit(req):
  1. server.opMu.Lock()
  2. defer server.opMu.Unlock()
  3. newPartitionID, err = server.splitter.Split(ctx, req.PartitionID, req.SplitKey)
     └ Splitter 내부에서 ExecuteSplit RPC → routing table 갱신까지 수행
  4. return &SplitResponse{NewPartitionID: newPartitionID}
```

### RequestMigrate 처리 흐름

```
RequestMigrate(req):
  1. server.opMu.Lock()
  2. defer server.opMu.Unlock()
  3. err = server.migrator.Migrate(ctx, req.PartitionID, req.TargetNodeID)
     └ Migrator 내부에서 Draining 표시 → ExecuteMigrateOut → PreparePartition → routing 갱신까지 수행
  4. return &MigrateResponse{}
```

> `opMu`로 split/migrate를 직렬화한다. 동시 rebalance 연산은 라우팅 테이블 충돌 위험이 있고 검증이 복잡해진다. PM 단일 인스턴스 환경에서는 직렬화 비용이 크지 않다.

---

## policy/policy.go

```go
// RebalancePolicy는 클러스터 이벤트에 반응하여 rebalance를 수행하는 전략 인터페이스.
// Server가 멤버십 이벤트 발생 시 호출한다.
type RebalancePolicy interface {
    // OnNodeJoined는 새 노드가 클러스터에 합류했을 때 호출된다.
    // 파티션 부하를 균등하게 하기 위해 일부 파티션을 새 노드로 migrate할 수 있다.
    OnNodeJoined(ctx context.Context, node domain.NodeInfo)

    // OnNodeLeft는 노드가 클러스터를 떠났을 때 호출된다.
    // 해당 노드의 파티션을 다른 노드로 재배치해야 한다.
    OnNodeLeft(ctx context.Context, node domain.NodeInfo)
}
```

---

## policy/manual.go

```go
// ManualPolicy는 자동 rebalance를 수행하지 않는다.
// split/migrate는 오직 명시적 RPC(RequestSplit, RequestMigrate)로만 발생한다.
// 노드 장애 발생 시 운영자가 직접 abctl로 처리해야 한다.
type ManualPolicy struct{}

func (p *ManualPolicy) OnNodeJoined(ctx context.Context, node domain.NodeInfo) {} // no-op
func (p *ManualPolicy) OnNodeLeft(ctx context.Context, node domain.NodeInfo)  {} // no-op
```

---

## policy/auto.go

```go
// AutoRebalancePolicy는 클러스터 이벤트와 주기적 메트릭 검사를 기반으로
// split/migrate를 자동으로 수행한다.
//
// 동작 방식:
//   - OnNodeJoined: 가장 부하가 높은 파티션들을 새 노드로 migrate하여 균등 분배.
//   - OnNodeLeft:   해당 노드의 파티션을 부하가 낮은 다른 노드로 migrate.
//   - 주기적 검사: split 임계치를 초과한 파티션을 자동으로 split.
//
// 메트릭 수집 방식은 구현 단계에서 결정한다.
// (Prometheus HTTP scraping 또는 PS가 PM에 주기적으로 push하는 방식 중 선택)
type AutoRebalancePolicy struct {
    splitter     *rebalance.Splitter
    migrator     *rebalance.Migrator
    nodeRegistry *cluster.NodeRegistry
    routingStore *cluster.RoutingTableStore

    splitThreshold float64       // 파티션당 요청 수/초 임계치. 초과 시 split 트리거.
    checkInterval  time.Duration // 주기적 메트릭 검사 간격
    opMu           *sync.Mutex   // Server.opMu 공유 (직렬화)
}

func NewAutoRebalancePolicy(
    splitter       *rebalance.Splitter,
    migrator       *rebalance.Migrator,
    nodeRegistry   *cluster.NodeRegistry,
    routingStore   *cluster.RoutingTableStore,
    splitThreshold float64,
    checkInterval  time.Duration,
    opMu           *sync.Mutex,
) *AutoRebalancePolicy

func (p *AutoRebalancePolicy) OnNodeJoined(ctx context.Context, node domain.NodeInfo)
func (p *AutoRebalancePolicy) OnNodeLeft(ctx context.Context, node domain.NodeInfo)

// Start는 주기적 메트릭 검사 루프를 시작한다.
// AutoRebalancePolicy 사용 시 Server.Start 내에서 호출한다.
func (p *AutoRebalancePolicy) Start(ctx context.Context)
```

> **메트릭 수집 방식은 구현 단계 결정 사항.** Prometheus scraping 방식이 가장 단순하다. PS가 Prometheus 메트릭을 HTTP로 노출하면 AutoRebalancePolicy가 주기적으로 조회한다. PS → PM push 방식은 transport 변경이 필요하므로 초기에는 scraping을 우선 검토한다.

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| 라우팅 테이블 broadcast 방식 | `atomic.Pointer` (최신값) + `chan struct{}` (신호) | 빠른 연속 변경 시 중간 값 skip. SDK는 최신 라우팅만 필요. |
| split/migrate 직렬화 | `opMu sync.Mutex` | 단순하고 안전. PM 단일 인스턴스 환경에서 충분. |
| RebalancePolicy 인터페이스 | `OnNodeJoined / OnNodeLeft` | Policy를 교체 가능하게 분리. 초기엔 ManualPolicy로 시작. |
| 초기 라우팅 테이블 생성 | PM bootstrap 시 첫 PS 등록 후 etcd CAS로 생성 | PM이 클러스터 상태를 초기화하는 단일 권한. 중복 생성 방어. |
| AutoRebalancePolicy opMu 공유 | Server.opMu 포인터 전달 | Policy 내부에서도 split/migrate를 직렬화해야 하므로 동일 mutex 공유. |
| Policy.OnNodeLeft에서 Migrate | Policy 내부에서 migrator.Migrate 직접 호출 | RPC 경유 불필요. PM 내부 로직이므로 직접 의존성 주입. |
| PM etcd 등록 없음 | PM은 NodeRegistry에 등록하지 않음 | PM은 파티션 보유자가 아닌 조율자. PS만 등록. |

---

## 알려진 한계

- **PM 단일 인스턴스**: PM이 다운되면 split/migrate/WatchRouting이 불가능하다. PS는 계속 동작하고 SDK는 캐시된 라우팅으로 요청을 처리할 수 있으나, 라우팅 변경은 반영되지 않는다. PM HA는 향후 과제.
- **PM 재시작 중 in-flight 연산**: PM이 split/migrate 도중 재시작되면 rebalance.md의 실패 복구 전략이 적용된다. 단, PM이 재시작 후 자동으로 복구를 수행하지는 않는다. 운영자가 상태를 확인하고 필요 시 수동으로 완료해야 한다.
- **WatchRouting 구독자 누수**: 클라이언트가 비정상 종료하면 stream.Context()가 취소되어 unsubscribe가 호출된다. gRPC keepalive가 비정상 종료를 감지하는 시간만큼 구독자가 잠시 남아있을 수 있다. keepalive 파라미터는 구현 단계에서 결정한다.
- **AutoRebalancePolicy 구현 미완**: 메트릭 수집 방식·임계치·split/migrate 결정 알고리즘은 구현 단계에서 결정한다.
- **ManualPolicy + NodeLeft**: ManualPolicy에서 노드가 영구 장애나면 해당 노드의 파티션은 라우팅 테이블에 여전히 남아있어 SDK가 접근을 시도한다. 운영자가 직접 `abctl migrate`로 파티션을 다른 노드로 옮겨야 한다.
