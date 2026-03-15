# internal/domain 패키지 설계

핵심 도메인 타입 정의. 외부 패키지에 의존하지 않는다.

파일 목록:
- `partition.go` — KeyRange, Partition
- `node.go` — NodeInfo, NodeStatus
- `routing.go` — RoutingTable, RouteEntry

---

## partition.go

```go
// KeyRange는 [Start, End) 형태의 반열린 구간.
// End == "" 이면 상한 없음 (Start 이상 모든 키).
type KeyRange struct {
    Start string // inclusive
    End   string // exclusive; "" = 상한 없음
}

func (r KeyRange) Contains(key string) bool  // key가 이 범위에 속하는지
func (r KeyRange) Overlaps(other KeyRange) bool  // 두 범위가 겹치는지

type Partition struct {
    ID        string
    ActorType string // "kv", "bucket", "object" 등
    KeyRange  KeyRange
}
```

**Contains**: `End == ""` 이면 `key >= Start`, 아니면 `key >= Start && key < End`

**Overlaps**: `[a, b)`, `[c, d)` 가 겹치는 조건: `a < d && c < b` (`""` 는 +∞ 로 취급)

---

## node.go

```go
type NodeStatus int
const (
    NodeStatusActive   NodeStatus = iota // 정상 동작 중
    NodeStatusDraining                   // migration 진행 중
)

type NodeInfo struct {
    ID      string
    Address string // gRPC 접속 주소 ("host:port")
    Status  NodeStatus
}
```

---

## routing.go

`RoutingTable`은 파티션→노드 매핑 테이블. actorType별로 독립적인 키 공간을 가진다. "bucket"과 "object" 파티션이 동일 키 범위를 가져도 충돌하지 않는다.

내부 구조:
- `byType map[string][]RouteEntry` — actorType → KeyRange.Start 기준 정렬된 entries
- `partitions map[string]RouteEntry` — partitionID → entry (O(1) 조회)

```go
type RouteEntry struct {
    Partition Partition
    Node      NodeInfo
}

func NewRoutingTable(version int64, entries []RouteEntry) (*RoutingTable, error)

func (rt *RoutingTable) Version() int64
func (rt *RoutingTable) Entries() []RouteEntry
func (rt *RoutingTable) EntriesByType(actorType string) []RouteEntry
func (rt *RoutingTable) ActorTypes() []string
func (rt *RoutingTable) Lookup(actorType, key string) (RouteEntry, bool)               // O(log n)
func (rt *RoutingTable) LookupByPartition(partitionID string) (RouteEntry, bool)        // O(1)
func (rt *RoutingTable) PartitionsInRange(actorType, startKey, endKey string) []RouteEntry // O(n)
```

### Lookup 알고리즘

`byType[actorType]`의 정렬된 슬라이스에서 이진 탐색으로 `Start <= key`를 만족하는 마지막 entry를 찾고, `KeyRange.Contains(key)`로 검증한다.

### PartitionsInRange 알고리즘

`[startKey, endKey)` 범위와 겹치는 모든 파티션을 반환한다. SDK Scan의 fan-out 대상 파티션 목록 계산에 사용된다.

1. 이진 탐색으로 `Start > startKey`를 만족하는 첫 entry 위치 `idx` 를 구하고 `idx-1`로 조정 (startKey를 포함하는 파티션 시작점)
2. `idx`부터 순차 탐색하며 `KeyRange.Start >= endKey` (endKey != "")이면 중단
3. `KeyRange.End <= startKey` 인 항목은 skip (겹치지 않음)
4. 해당 범위와 겹치는 항목을 `KeyRange.Start` 순서로 반환

### NewRoutingTable 처리 순서

1. entries를 actorType별로 분리
2. 각 actorType의 entries를 `KeyRange.Start` 기준 정렬
3. 동일 actorType 내 인접 entry 간 `Overlaps` 검사 → 겹치면 에러
4. partitionID 중복 검사 (전체 actorType 통합) → 중복이면 에러
5. `byType` 및 `partitions` 맵 구축

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| 외부 의존성 | 없음 | 도메인 레이어는 어떤 것도 import하지 않는다 |
| KeyRange 상한 없음 표현 | `End == ""` | 단순하고 관례적. bool 필드나 포인터보다 struct가 작음 |
| 값 타입 | KeyRange, Partition, NodeInfo, RouteEntry 모두 struct 값 타입 | 작은 크기, 불변성 의도 |
| RoutingTable 필드 | unexported | 생성자 강제를 통해 내부 불변식 보호 |
| actorType별 키 공간 분리 | `byType map[string][]RouteEntry` | 서로 다른 Actor 타입이 독립적인 키 공간을 가짐 |
| key Lookup 복잡도 | O(log n) 이진 탐색 | 매 요청마다 호출되는 핫패스 |
| partition Lookup 복잡도 | O(1) map 조회 | migration 조율 등 관리 경로에서도 빈번히 호출 |
| 불변식 검증 시점 | NewRoutingTable 생성 시 1회 | 런타임 Lookup마다 검증하지 않아도 됨 |
| 스레드 안전성 | 없음 | RoutingTable은 atomic swap 방식으로 갱신. 잠금은 사용 측 책임 |
