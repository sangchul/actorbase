# internal/rebalance 패키지 설계

파티션 Split·Migration 조율. PM이 사용한다.

의존성: `internal/domain`, `internal/cluster`, `internal/transport`, `provider`

파일 목록:
- `splitter.go` — 파티션 split 조율
- `migrator.go` — 파티션 migration 조율

---

## Split

### 흐름

```
[PM: Splitter]                    [Source PS]
      │
      │ 1. 검증 (partitionID 존재, splitKey 제공 시 KeyRange 내인지 확인)
      │ 2. newPartitionID 생성
      │
      │── ExecuteSplit ──────────────────────▶│
      │   (partitionID, splitKey,             │ 3. 파티션 busy 표시
      │    keyRangeStart, keyRangeEnd,        │ 4. split key 결정:
      │    newPartitionID, actorType)         │    splitKey != "" → 그대로 사용
      │                                       │    splitKey == "" → SplitHint() 또는 midpoint
      │                                       │ 5. Actor.Split(결정된 key) → upperHalf
      │                                       │ 6. 하위 파티션 checkpoint 저장
      │                                       │ 7. 상위 파티션 checkpoint 저장
      │                                       │ 8. 두 파티션 모두 Active
      │◀── ExecuteSplitResponse(split_key) ───│
      │
      │ 9. RoutingTable 갱신 (PS가 반환한 split_key 사용)
      │    [oldStart, split_key) → sourceNode (Active)
      │    [split_key, oldEnd)  → sourceNode (Active)
      │ 10. etcd에 저장
```

### Splitter API

```go
func (s *Splitter) Split(ctx context.Context, actorType, partitionID, splitKey string) (newPartitionID string, err error)
```

**검증 조건:**
- `partitionID`가 현재 라우팅 테이블에 존재
- `splitKey`가 명시적으로 제공된 경우에만:
  - `splitKey > partition.KeyRange.Start` (빈 하위 파티션 방지)
  - `splitKey < partition.KeyRange.End` (또는 End가 ""이면 제약 없음)
- `splitKey == ""`이면 PS가 SplitHinter 또는 midpoint로 결정 (검증 skip)

---

## Migration

### 흐름

```
[PM: Migrator]           [Source PS]              [Target PS]
      │
      │ 1. 검증
      │ 2. 파티션 → Draining 상태로 갱신 (etcd 저장)
      │    (SDK: ErrPartitionBusy 수신 → 재시도 대기)
      │
      │── ExecuteMigrateOut ───────▶│
      │                             │ 3. Actor 최종 checkpoint
      │                             │ 4. Actor evict
      │◀── success ─────────────────│
      │
      │── PreparePartition ──────────────────────▶│
      │                                           │ 5. CheckpointStore에서 로드
      │                                           │    + WAL replay
      │◀── success ────────────────────────────────│
      │
      │ 6. 파티션 → targetNode (Active) (etcd 저장)
```

### Migrator API

```go
// 정상 migration: source PS가 살아있을 때
func (m *Migrator) Migrate(ctx context.Context, actorType, partitionID, targetNodeID string) error

// 장애 복구: source PS가 죽었을 때. ExecuteMigrateOut을 건너뛴다.
func (m *Migrator) Failover(ctx context.Context, actorType, partitionID, targetNodeID string) error
```

**검증 조건:**
- `partitionID`가 라우팅 테이블에 존재
- `targetNodeID`가 NodeRegistry에 존재하고 Active 상태
- source 노드와 target 노드가 다름
- 파티션이 이미 Draining 상태이면 에러 (중복 migration 방지)

---

## Failover (장애 복구)

source PS가 죽었을 때 PM이 자동으로 파티션을 다른 PS로 복구하는 경로. Migrate와 달리 ExecuteMigrateOut을 건너뛴다.

```
[PM: Migrator.Failover]          [Target PS]
      │
      │ 1. 검증 + 파티션 → Draining
      │    [ExecuteMigrateOut 건너뜀 — source PS 죽음]
      │
      │── PreparePartition ──────────────────────▶│
      │                                           │ 2. CheckpointStore에서 로드
      │                                           │    + WAL replay
      │◀── success ────────────────────────────────│
      │
      │ 3. 파티션 → targetNode (Active) (etcd 저장)
```

> source의 마지막 checkpoint 이후 WAL replay 범위는 WALStore 구현에 따라 다르다.
> fs WAL이면 WAL 파일이 공유 경로에 있어야 target PS가 읽을 수 있다.
> networked WAL(Redis Streams 등)이면 target PS가 동일 store에서 WAL을 읽어 완전 복원한다.

### 실패 복구 전략

| 실패 시점 | 복구 방법 |
|---|---|
| ExecuteMigrateOut 전 | RoutingTable을 Active로 되돌린다 |
| ExecuteMigrateOut 성공 후, PreparePartition 실패 | PreparePartition 재시도. N회 실패 시 RoutingTable을 source로 revert |
| PreparePartition 성공 후, etcd 저장 실패 | etcd 저장 재시도 |

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| Actor 상태 분리 | `Actor.Split(splitKey)` 메서드 | Actor가 자신의 데이터 구조를 직접 분리. full snapshot 복사 후 필터링보다 정확. |
| PartitionStatus | `RouteEntry`에 추가 | 파티션 단위 Draining 표시 필요. 다른 파티션은 영향 없이 계속 동작. |
| Migration 상태 전달 | CheckpointStore 경유 | PS 간 직접 전송 대신 공유 저장소 활용. |
| Split 후 파티션 위치 | 두 파티션 모두 source 노드 | Split과 Migration을 분리. 필요 시 PM policy가 split 후 migrate 호출. |
| 중복 migration 방지 | Draining 상태 검사 | 동일 파티션에 동시 migration 요청 시 즉시 에러 반환. |

---

## 알려진 한계

- **Split 중 다운타임**: Actor.Split 실행부터 양쪽 checkpoint 저장까지 해당 파티션 요청이 블로킹된다.
- **Migration 중 지연**: Draining 상태인 동안 SDK는 ErrPartitionBusy를 수신하고 재시도한다.
- **WAL trim 타이밍**: migration 완료 후 source의 WAL이 명시적 cleanup 없이 WALStore에 남는다.
