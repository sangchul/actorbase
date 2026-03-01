# internal/rebalance 패키지 설계

파티션 Split·Migration 조율. PM이 사용한다.

의존성: `internal/domain`, `internal/cluster`, `internal/transport`, `provider`

파일 목록:
- `splitter.go` — 파티션 split 조율
- `migrator.go` — 파티션 migration 조율

---

## 이전 설계 변경 사항

rebalance 설계 과정에서 세 패키지의 설계가 변경된다.

### 1. provider/actor.go — Split 메서드 추가

```go
type Actor[Req, Resp any] interface {
    Receive(ctx Context, req Req) (Resp, []byte, error)
    Replay(entry []byte) error
    Snapshot() ([]byte, error)
    Restore([]byte) error
    // Split은 splitKey 기준으로 상위 절반의 상태를 직렬화하여 반환하고,
    // 자신의 상태에서 해당 데이터를 제거한다.
    // engine이 split 실행 시 호출한다. Actor가 직접 상태를 분리한다.
    Split(splitKey string) (upperHalf []byte, err error)
}
```

### 2. internal/domain/routing.go — PartitionStatus 추가

노드 전체가 아닌 특정 파티션 단위로 상태를 관리하기 위해 `PartitionStatus`를 추가한다.

```go
// PartitionStatus는 파티션의 현재 운영 상태.
type PartitionStatus int

const (
    // PartitionStatusActive: 정상 운영 중. 요청 수락.
    PartitionStatusActive PartitionStatus = iota

    // PartitionStatusDraining: migration 진행 중. 새 요청 거부(ErrPartitionBusy).
    PartitionStatusDraining
)

// RouteEntry에 PartitionStatus 추가
type RouteEntry struct {
    Partition       Partition
    Node            NodeInfo
    PartitionStatus PartitionStatus // 추가
}
```

### 3. internal/transport/proto — PreparePartition RPC 추가

migration 시 PM이 target PS에게 파티션 로드를 명령하는 RPC.

```protobuf
service PartitionControlService {
    rpc ExecuteSplit(ExecuteSplitRequest) returns (ExecuteSplitResponse);
    rpc ExecuteMigrateOut(ExecuteMigrateOutRequest) returns (ExecuteMigrateOutResponse);
    // 추가: PM이 target PS에게 파티션을 CheckpointStore에서 로드하도록 명령
    rpc PreparePartition(PreparePartitionRequest) returns (PreparePartitionResponse);
}

message PreparePartitionRequest {
    string partition_id    = 1;
    string key_range_start = 2; // target PS가 올바른 파티션인지 검증용
    string key_range_end   = 3;
}

message PreparePartitionResponse {}
```

---

## Split

### 흐름

```
[PM: Splitter]                    [Source PS]
      │
      │ 1. 검증 (partitionID 존재, splitKey가 KeyRange 내)
      │ 2. newPartitionID 생성
      │
      │── ExecuteSplit ──────────────────────▶│
      │   (partitionID, splitKey,             │ 3. 파티션 busy 표시
      │    newPartitionID)                    │ 4. Actor.Split(splitKey)
      │                                       │    → upperHalf []byte
      │                                       │    → actor는 하위 절반만 유지
      │                                       │ 5. 하위 파티션 checkpoint 저장
      │                                       │ 6. 새 Actor 생성 + Restore(upperHalf)
      │                                       │ 7. 상위 파티션 checkpoint 저장
      │                                       │ 8. 두 파티션 모두 Active
      │◀── success ───────────────────────────│
      │
      │ 9. RoutingTable 갱신
      │    - 기존: [oldStart, oldEnd) → sourceNode
      │    + 분리: [oldStart, splitKey) → sourceNode (Active)
      │    +       [splitKey, oldEnd)  → sourceNode (Active)
      │ 10. etcd에 저장
```

### splitter.go

```go
// Splitter는 파티션 split을 조율한다.
type Splitter struct {
    routingStore *cluster.RoutingTableStore
    connPool     *transport.ConnPool
}

func NewSplitter(
    routingStore *cluster.RoutingTableStore,
    connPool     *transport.ConnPool,
) *Splitter

// Split은 partitionID를 splitKey 기준으로 둘로 나눈다.
// 새로 생성된 파티션(상위 절반)의 ID를 반환한다.
// split 후 두 파티션은 기존과 동일한 노드에 위치한다.
// 필요 시 호출자(PM policy)가 이후 Migrate를 호출한다.
func (s *Splitter) Split(ctx context.Context, partitionID, splitKey string) (newPartitionID string, err error)
```

#### 검증 조건

- `partitionID`가 현재 라우팅 테이블에 존재해야 한다.
- `splitKey`가 파티션의 `KeyRange` 내에 있어야 한다. (`Start < splitKey < End` 또는 `Start < splitKey` when `End == ""`)
- `splitKey`가 `Start`와 같으면 안 된다. (빈 하위 파티션 방지)

---

## Migration

### 흐름

```
[PM: Migrator]           [Source PS]              [Target PS]
      │
      │ 1. 검증
      │ 2. RoutingTable에서 해당 파티션 Draining으로 갱신 → etcd 저장
      │    (SDK는 ErrPartitionBusy 수신 후 대기)
      │
      │── ExecuteMigrateOut ───────▶│
      │                             │ 3. Actor 최종 checkpoint 저장
      │                             │ 4. Actor evict
      │◀── success ─────────────────│
      │
      │── PreparePartition ──────────────────────▶│
      │                                           │ 5. CheckpointStore에서 로드
      │                                           │    + WAL replay
      │◀── success ────────────────────────────────│
      │
      │ 6. RoutingTable 갱신
      │    파티션 → targetNode (Active)
      │    etcd 저장 → PS·SDK에 전파
```

### migrator.go

```go
// Migrator는 파티션 migration을 조율한다.
type Migrator struct {
    routingStore *cluster.RoutingTableStore
    nodeRegistry *cluster.NodeRegistry
    connPool     *transport.ConnPool
}

func NewMigrator(
    routingStore *cluster.RoutingTableStore,
    nodeRegistry *cluster.NodeRegistry,
    connPool     *transport.ConnPool,
) *Migrator

// Migrate는 partitionID를 targetNodeID로 이동시킨다.
func (m *Migrator) Migrate(ctx context.Context, partitionID, targetNodeID string) error
```

#### 검증 조건

- `partitionID`가 라우팅 테이블에 존재해야 한다.
- `targetNodeID`가 NodeRegistry에 존재하고 Active 상태여야 한다.
- source 노드와 target 노드가 달라야 한다.
- 파티션이 이미 Draining 상태이면 에러 반환. (중복 migration 방지)

### 실패 복구 전략

```
실패 시점                     복구 방법
─────────────────────────────────────────────────────────────────
ExecuteMigrateOut 전          RoutingTable을 Active로 되돌린다.
                              (etcd 저장 안 됐으므로 revert)

ExecuteMigrateOut 성공 후,    PreparePartition 재시도.
PreparePartition 실패         Checkpoint는 CheckpointStore에 남아있으므로
                              재시도 가능. N회 실패 시 RoutingTable을
                              source로 되돌린다. (source PS는 다음 요청
                              수신 시 checkpoint에서 재활성화)

PreparePartition 성공 후,     etcd 저장 재시도.
etcd 저장 실패
```

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| Actor 상태 분리 | `Actor.Split(splitKey)` 메서드 | Actor가 자신의 데이터 구조를 직접 분리. full snapshot 복사 후 필터링 방식보다 정확하고 효율적. |
| PartitionStatus 추가 | `RouteEntry`에 추가 | 노드 단위가 아닌 파티션 단위로 Draining 표시 필요. 다른 파티션은 영향 없이 계속 동작. |
| Migration 중 상태 전달 | CheckpointStore 경유 | Source PS → Target PS 직접 전송 대신 공유 저장소 활용. PS 간 직접 통신 불필요. |
| Split 후 파티션 위치 | 두 파티션 모두 source 노드 | Split과 Migration을 분리. 필요 시 PM policy가 split 후 migrate 호출. |
| PreparePartition | 동기 RPC (PM이 응답 대기) | Target PS가 준비 완료를 PM에 알리는 가장 단순한 방식. |
| 중복 migration 방지 | Draining 상태 검사 | 동일 파티션에 동시 migration 요청이 들어오면 즉시 에러 반환. |

---

## 알려진 한계

- **Split 중 다운타임**: Actor가 busy 상태인 동안 (`Actor.Split` 실행부터 양쪽 checkpoint 저장까지) 해당 파티션에 대한 요청이 블로킹된다. 이 시간은 짧아야 하지만 구현 후 측정이 필요하다.
- **Migration 중 지연**: 파티션이 Draining 상태인 동안 SDK는 `ErrPartitionBusy`를 수신하고 대기한다. 대기 전략(retry interval, max wait)은 SDK 설계에서 결정한다.
- **WAL trim 타이밍**: migration 완료 후 source의 WAL은 더 이상 필요 없다. 명시적인 cleanup이 없으면 WALStore에 계속 남는다. PM이 migration 완료 후 WALStore.TrimBefore를 호출하는 방식을 구현 시 검토한다.
- **대규모 파티션 PreparePartition 타임아웃**: 파티션 상태가 크면 checkpoint 로드 + WAL replay에 시간이 걸린다. PM의 PreparePartition 호출 timeout을 충분히 크게 설정해야 한다. 구체적인 값은 구현 후 결정한다.
