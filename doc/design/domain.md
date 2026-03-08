# internal/domain 패키지 설계

핵심 도메인 타입 정의. 외부 패키지에 의존하지 않는다.

파일 목록:
- `partition.go` — KeyRange, Partition
- `node.go` — NodeInfo, NodeStatus
- `routing.go` — RoutingTable, RouteEntry

---

## partition.go

```go
// KeyRange는 [Start, End) 형태의 반열린 구간으로 파티션이 담당하는 키 범위를 나타낸다.
// End가 빈 문자열("")이면 상한이 없음을 의미한다 (Start 이상 모든 키).
type KeyRange struct {
    Start string // inclusive
    End   string // exclusive; "" = 상한 없음
}

// Contains는 key가 이 KeyRange에 속하는지 반환한다.
func (r KeyRange) Contains(key string) bool

// Overlaps는 두 KeyRange가 겹치는 영역이 있는지 반환한다.
// split/migration 전 범위 충돌 검증에 사용한다.
func (r KeyRange) Overlaps(other KeyRange) bool

// Partition은 클러스터 내 하나의 파티션 단위.
// 유일한 ID, 담당 키 범위, 그리고 Actor 타입을 가진다.
type Partition struct {
    ID        string
    ActorType string // 이 파티션을 담당하는 Actor 타입 ("kv", "bucket", "object" 등)
    KeyRange  KeyRange
}
```

### Contains 동작

```
End == "" : key >= Start
End != "" : key >= Start && key < End
```

### Overlaps 동작

두 구간 `[a, b)`, `[c, d)` 가 겹치는 조건:
- `a < d && c < b` (단, `""` 는 +∞ 로 취급)

---

## node.go

```go
// NodeStatus는 노드의 현재 상태를 나타낸다.
type NodeStatus int

const (
    // NodeStatusActive: 노드가 정상 동작 중이며 요청을 수락한다.
    NodeStatusActive NodeStatus = iota

    // NodeStatusDraining: 파티션 migration 진행 중.
    // 새 요청은 받지만, 곧 파티션을 반납할 예정이다.
    NodeStatusDraining
)

// NodeInfo는 클러스터 내 하나의 노드에 대한 메타데이터.
type NodeInfo struct {
    ID      string     // 클러스터 내 유일한 노드 식별자
    Address string     // gRPC 접속 주소 ("host:port")
    Status  NodeStatus
}
```

---

## routing.go

```go
// RouteEntry는 파티션과 그 파티션을 호스팅하는 노드의 쌍.
type RouteEntry struct {
    Partition Partition
    Node      NodeInfo
}

// RoutingTable은 파티션→노드 매핑 테이블.
//
// 내부 구조:
//   - byType     : actorType → 해당 타입의 RouteEntry 슬라이스 (KeyRange.Start 기준 정렬)
//   - partitions : partitionID → RouteEntry map  → 파티션 기반 Lookup O(1)
//
// actorType별로 독립적인 키 공간을 가진다.
// "bucket" 파티션과 "object" 파티션이 동일 키 범위를 가져도 충돌하지 않는다.
type RoutingTable struct {
    version    int64
    byType     map[string][]RouteEntry // actorType → sorted entries
    partitions map[string]RouteEntry   // partitionID → entry
}

// NewRoutingTable은 RoutingTable을 생성한다.
// entries를 actorType별로 분리하여 KeyRange.Start 기준으로 정렬하고,
// 동일 actorType 내에 겹치는 범위가 없는지 검증한다.
// 겹치는 범위가 있거나 중복 partitionID가 있으면 에러를 반환한다.
// 서로 다른 actorType 간에는 키 범위 중복이 허용된다.
func NewRoutingTable(version int64, entries []RouteEntry) (*RoutingTable, error)

// Version은 단조 증가하는 버전 번호를 반환한다.
func (rt *RoutingTable) Version() int64

// Entries는 모든 actorType의 RouteEntry 슬라이스의 복사본을 반환한다.
func (rt *RoutingTable) Entries() []RouteEntry

// EntriesByType은 특정 actorType의 RouteEntry 슬라이스를 반환한다.
// 해당 actorType이 없으면 nil을 반환한다.
func (rt *RoutingTable) EntriesByType(actorType string) []RouteEntry

// ActorTypes는 이 라우팅 테이블에 등록된 모든 actorType 목록을 반환한다.
func (rt *RoutingTable) ActorTypes() []string

// Lookup은 actorType과 key를 포함하는 파티션의 RouteEntry를 반환한다.
// 해당 actorType의 정렬된 entries를 이진 탐색한다. O(log n).
// 해당하는 파티션이 없으면 (zero value, false)를 반환한다.
func (rt *RoutingTable) Lookup(actorType, key string) (RouteEntry, bool)

// LookupByPartition은 partitionID에 해당하는 RouteEntry를 반환한다.
// 내부 map을 통해 O(1)로 조회한다.
func (rt *RoutingTable) LookupByPartition(partitionID string) (RouteEntry, bool)
```

### Lookup 이진 탐색 알고리즘

1. `byType[actorType]`에서 해당 타입의 entries 슬라이스를 가져온다.
2. `sort.Search`로 `Start <= key` 를 만족하는 마지막 entry를 찾는다.
3. 해당 entry의 `KeyRange.Contains(key)` 를 검증한다.
4. 검증 통과 시 반환, 실패 시 `(zero value, false)` 반환.

### NewRoutingTable 처리 순서

1. entries를 actorType별로 분리
2. 각 actorType의 entries를 `KeyRange.Start` 기준으로 정렬 (`slices.SortFunc`)
3. 동일 actorType 내 인접 entry 간 `Overlaps` 검사 → 겹치면 에러
4. partitionID 중복 검사 (전체 actorType 통합) → 중복이면 에러
5. `byType` map 및 `partitions` map 구축

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| 외부 의존성 | 없음 | 도메인 레이어는 어떤 것도 import하지 않는다 |
| KeyRange 상한 없음 표현 | `End == ""` | 단순하고 관례적. bool 필드나 포인터보다 struct가 작음 |
| 값 타입 | KeyRange, Partition, NodeInfo, RouteEntry 모두 struct 값 타입 | 작은 크기, 불변성 의도 |
| RoutingTable 필드 | unexported (`byType`, `partitions`, `version`) | 생성자 강제를 통해 내부 불변식 보호 |
| actorType별 키 공간 분리 | `byType map[string][]RouteEntry` | 서로 다른 Actor 타입이 독립적인 키 공간을 가짐. 중복 허용. |
| key Lookup 복잡도 | O(log n) 이진 탐색 | 매 요청마다 호출되는 핫패스 |
| partition Lookup 복잡도 | O(1) map 조회 | migration 조율 등 관리 경로에서도 빈번히 호출 가능 |
| 불변식 검증 시점 | NewRoutingTable 생성 시 1회 | 런타임 Lookup마다 검증하지 않아도 됨 |
| 스레드 안전성 | 없음 | RoutingTable은 교체(atomic swap) 방식으로 갱신한다. 잠금은 사용 측 책임 |

---

## 알려진 한계

- `byType`과 `partitions` 맵은 생성 이후 변경할 수 없다. 라우팅 테이블 갱신은 새 `RoutingTable` 인스턴스로 교체하는 방식으로만 가능하다.
- 파티션 수가 수백만 규모가 되면 이진 탐색보다 인터벌 트리가 유리할 수 있으나, 현재 설계 목표(수백~약 1,000 노드)에서는 불필요하다.
