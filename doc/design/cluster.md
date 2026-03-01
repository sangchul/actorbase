# internal/cluster 패키지 설계

etcd 기반 클러스터 멤버십 및 라우팅 테이블 관리.
PS·PM이 사용한다. SDK는 etcd에 직접 접근하지 않는다.

의존성: `internal/domain`, `go.etcd.io/etcd/client/v3`

파일 목록:
- `registry.go`   — NodeRegistry: etcd lease 기반 노드 등록/조회
- `membership.go` — MembershipWatcher: 노드 join/leave 이벤트 감지
- `routing.go`    — RoutingTableStore: 라우팅 테이블 저장/조회/watch

---

## etcd 키 스키마

```
/actorbase/nodes/{nodeID}   → NodeInfo JSON  (TTL lease)
/actorbase/routing          → RoutingTable JSON (no TTL)
```

| 키 | 쓰는 주체 | 읽는 주체 |
|---|---|---|
| `/actorbase/nodes/{nodeID}` | PS (register/deregister) | PM (ListNodes, MembershipWatcher) |
| `/actorbase/routing` | PM | PS (Watch) |

---

## etcd 확장성 고려

### SDK가 etcd를 직접 watch하지 않는 이유

SDK는 사용자 애플리케이션에 임베드된다. 애플리케이션 인스턴스가 수백 개라면,
SDK 인스턴스도 수백 개이고 etcd watch 커넥션도 수백 개가 된다.

etcd는 클러스터 코디네이션을 위한 강한 일관성 저장소로, 대규모 클라이언트 fanout에 적합한 구조가 아니다.
또한 SDK 사용자가 etcd 접속 설정(엔드포인트, TLS, 인증 등)을 직접 관리해야 하는 부담도 생긴다.

이를 해결하기 위해 PM이 etcd를 단일 watch하고 SDK에 gRPC로 fanout한다:

```
etcd ←── watch(1개) ── PM ── gRPC push ──→ SDK 인스턴스 (수백 개)
```

SDK는 PM 주소만 알면 되며, etcd 클라이언트를 보유하지 않는다.

### PS etcd watch의 확장성 한계 (미해결)

현재 설계에서 PS는 라우팅 테이블 변경을 etcd를 직접 watch하여 감지한다.
PS 노드가 1,000대면 etcd에 1,000개의 watch 커넥션이 생긴다.

SDK와 동일한 문제가 PS에도 존재한다. PM이 PS에도 gRPC로 push하는 방식으로 전환하면
etcd watch를 PM 단일 커넥션으로 줄일 수 있다:

```
etcd ←── watch(1개) ── PM ── gRPC push ──→ PS (1,000대)
                          └── gRPC push ──→ SDK (수백 개)
```

단, 이 방식은 PM이 모든 PS 주소를 알고 커넥션을 관리해야 하므로 PM 구현이 복잡해진다.
(PS 주소는 NodeRegistry에서 조회 가능하므로 불가능한 건 아니다.)

**현재 결정:** 설계 목표 규모(수백~1,000 노드)에서 etcd v3는 수천 개의 watch 커넥션을
지원하므로, 우선 PS는 etcd watch 방식을 유지한다.
실제 운영에서 etcd 부하가 문제가 되면 PM gRPC push 방식으로 전환을 검토한다.

---

## registry.go

```go
// NodeRegistry는 etcd lease를 활용한 노드 등록/조회를 담당한다.
//
// PS 시작 시 Register를 호출하여 자신을 클러스터에 등록한다.
// 내부적으로 lease를 생성하고 ctx가 살아있는 동안 주기적으로 갱신(keepalive)한다.
// lease가 만료되면 etcd가 해당 노드 키를 자동으로 삭제한다.
type NodeRegistry struct {
    client *clientv3.Client
    ttl    time.Duration // lease TTL
}

func NewNodeRegistry(client *clientv3.Client, ttl time.Duration) *NodeRegistry

// Register는 node를 etcd에 등록하고, ctx가 취소될 때까지 lease를 갱신한다.
// lease 갱신은 내부 goroutine에서 처리한다.
// ctx 취소 시 keepalive를 중단하고 반환한다. (lease는 TTL 후 자동 만료)
// graceful shutdown 시에는 Deregister를 별도로 호출한다.
func (r *NodeRegistry) Register(ctx context.Context, node domain.NodeInfo) error

// Deregister는 nodeID의 lease를 즉시 revoke하여 클러스터에서 제거한다.
// graceful shutdown 시 TTL 만료를 기다리지 않고 즉시 제거하는 데 사용한다.
func (r *NodeRegistry) Deregister(ctx context.Context, nodeID string) error

// ListNodes는 현재 등록된 (살아있는) 모든 노드를 반환한다.
// /actorbase/nodes/ 프리픽스를 가진 모든 키를 조회한다.
func (r *NodeRegistry) ListNodes(ctx context.Context) ([]domain.NodeInfo, error)
```

### PS 생명주기와 Register

```
PS 시작:
  go registry.Register(ctx, myNode)  // ctx = PS 전체 생명주기

PS 종료 (graceful):
  registry.Deregister(ctx, myNode.ID)
  cancel()  // Register goroutine 종료
```

---

## membership.go

```go
// NodeEventType은 멤버십 변경 이벤트의 종류.
type NodeEventType int

const (
    NodeJoined NodeEventType = iota // 새 노드가 etcd에 등록됨
    NodeLeft                        // 노드의 lease가 만료되거나 revoke됨
)

// NodeEvent는 클러스터 멤버십 변경 이벤트.
type NodeEvent struct {
    Type NodeEventType
    Node domain.NodeInfo
}

// MembershipWatcher는 etcd watch를 통해 노드 join/leave 이벤트를 감지한다.
// PM이 사용한다. 노드 장애 감지 및 파티션 재배치 트리거에 활용한다.
type MembershipWatcher struct {
    client *clientv3.Client
}

func NewMembershipWatcher(client *clientv3.Client) *MembershipWatcher

// Watch는 노드 join/leave 이벤트 채널을 반환한다.
// etcd watch가 끊겼다가 재연결되면 현재 전체 노드 목록을 다시 읽어
// 놓친 이벤트를 보정한다.
// ctx 취소 시 채널이 닫힌다.
func (w *MembershipWatcher) Watch(ctx context.Context) <-chan NodeEvent
```

### Watch 재연결 보정

etcd watch가 네트워크 장애로 끊어졌다가 재연결되면 이벤트가 유실될 수 있다.
재연결 시 `/actorbase/nodes/` 전체를 스냅샷으로 읽어 이전 상태와 비교 후 delta를 전달한다.

```
reconnect 시:
  current = etcd.Get(/actorbase/nodes/)
  delta   = diff(last_known_nodes, current)
  for each added node:   emit NodeJoined
  for each removed node: emit NodeLeft
  last_known_nodes = current
```

---

## routing.go

```go
// RoutingTableStore는 etcd에서 라우팅 테이블을 저장/조회/감시한다.
//
// PM: 파티션 토폴로지 변경 시 Save 호출
// PS: 시작 시 첫 이벤트 수신, 이후 Watch로 변경 감지
type RoutingTableStore struct {
    client *clientv3.Client
}

func NewRoutingTableStore(client *clientv3.Client) *RoutingTableStore

// Save는 라우팅 테이블을 etcd에 저장한다.
// PM은 단일 인스턴스이므로 동시성 제어 없이 단순 Put을 사용한다.
func (s *RoutingTableStore) Save(ctx context.Context, rt *domain.RoutingTable) error

// Load는 etcd에서 현재 라우팅 테이블을 조회한다.
// 키가 없으면 (nil, nil)을 반환한다. (빈 클러스터)
func (s *RoutingTableStore) Load(ctx context.Context) (*domain.RoutingTable, error)

// Watch는 라우팅 테이블이 변경될 때마다 새 테이블을 전달하는 채널을 반환한다.
// Watch 시작 시 현재 테이블을 즉시 한 번 전달한다. (초기 상태 동기화)
// ctx 취소 시 채널이 닫힌다.
func (s *RoutingTableStore) Watch(ctx context.Context) <-chan *domain.RoutingTable
```

### etcd 직렬화 (내부 DTO)

`domain.RoutingTable`은 필드가 unexported이므로 내부 DTO로 변환하여 JSON 직렬화한다.
JSON을 사용하는 이유: 클러스터 메타데이터 경로로 성능 비중점이며, 사람이 읽을 수 있어 운영 편의성이 높다.

```go
// cluster 패키지 내부에서만 사용
type routingTableDTO struct {
    Version int64           `json:"version"`
    Entries []routeEntryDTO `json:"entries"`
}

type routeEntryDTO struct {
    PartitionID   string `json:"partitionId"`
    KeyRangeStart string `json:"keyRangeStart"`
    KeyRangeEnd   string `json:"keyRangeEnd"` // "" = 상한 없음
    NodeID        string `json:"nodeId"`
    NodeAddress   string `json:"nodeAddress"`
    NodeStatus    int    `json:"nodeStatus"`
}
```

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| SDK 라우팅 수신 | PM gRPC push | etcd watch 커넥션 수 제한. SDK 사용자가 etcd 설정 불필요. |
| PS 라우팅 수신 | etcd watch (현재) | 목표 규모에서 허용 가능. 부하 문제 시 PM gRPC push로 전환 검토. |
| 노드 등록 방식 | etcd lease + keepalive | 노드 장애 시 TTL 만료로 자동 제거. 별도 gossip 라이브러리 불필요. |
| RoutingTableStore 동시성 제어 | 없음 (단순 Put) | PM 단일 인스턴스 가정. PM HA 구현 시 etcd CAS transaction 추가. |
| RoutingTable 직렬화 | JSON (내부 DTO) | 운영 가시성. 핫패스 아님. |
| Watch 재연결 보정 | 스냅샷 diff | etcd watch reconnect 시 이벤트 유실 방지. |
| Watch 초기 동기화 | Watch 시작 시 현재 테이블 즉시 전달 | PS 시작 시 Load + Watch 이중 호출 불필요. |

---

## 알려진 한계

- **PS etcd watch 확장성**: 노드 1,000대 = etcd watch 1,000개. 현재 규모에서는 허용 가능하나, 규모가 커지면 PM이 PS에도 gRPC push하는 방식으로 전환이 필요하다.
- **RoutingTable 크기**: 파티션 수가 매우 많아지면 etcd 단일 키의 JSON이 커진다. 현재 설계 목표 규모에서는 허용 가능할 것으로 예상하나, 구현 후 측정이 필요하다.
- **PM HA 미지원**: RoutingTableStore.Save에 동시성 제어가 없으므로 PM 단일 인스턴스일 때만 안전하다. PM HA는 향후 과제.
- **NodeLeft 이벤트 지연**: lease TTL 동안 장애 노드가 살아있는 것으로 간주된다. TTL 값은 구현 단계에서 결정한다.
