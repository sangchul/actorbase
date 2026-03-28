# 향후 개선 계획

## Checkpoint 효율 개선

### 현재 구조

actorbase는 **full snapshot + WAL** 2-tier 복구 모델을 사용한다.

```
Checkpoint 생성: Actor.Snapshot() → [8byte LSN][full state] → CheckpointStore.Save()
복원: CheckpointStore.Load() → Restore(state) → WALStore.ReadFrom(lsn+1) → Replay
트리거: 주기적(scheduler) / WAL 누적 임계값 / eviction / split / migration / shutdown
```

- checkpoint 사이의 모든 변경은 WAL에 기록됨
- checkpoint는 WAL replay 시간을 제한하기 위한 **주기적 baseline**
- 변경 없는 파티션은 checkpoint를 건너뜀 (`walsSinceCheckpoint == 0`)

### Incremental Snapshot이 불필요한 이유

**WAL이 이미 incremental record 역할을 한다.** checkpoint + WAL replay로 임의 시점 복원이 가능하므로, checkpoint에 delta layer를 추가하면 3-tier (base checkpoint + incremental checkpoint + WAL) 구조가 되어 복잡도 대비 이득이 적다.

| 구조 | 복구 경로 | 복잡도 |
|------|-----------|--------|
| 현재 (2-tier) | checkpoint → WAL replay | 단순 |
| Incremental (3-tier) | base → deltas → WAL replay | base refresh 주기, delta 손상 복구, 다중 버전 관리 필요 |

추가로:
- WAL entry는 opaque bytes → framework가 delta를 이해/생성할 수 없어 Actor에 새 인터페이스 필요
- WAL compaction도 동일한 이유로 Actor 로직에 의존 → incremental과 비슷한 복잡도
- `Snapshot()`/`Restore()` 계약이 단순한 것이 actorbase의 장점 — Actor 구현 부담을 최소화

### Split이 올바른 전략인 이유

actorbase는 **자동 파티셔닝 시스템**이다. 파티션이 커지면 split하는 것이 시스템의 근본 설계 원칙이며, checkpoint 비용 문제도 이 원칙 안에서 해결하는 것이 일관적이다.

- 파티션 크기가 bounded → checkpoint 비용도 bounded
- Kafka partition, DynamoDB adaptive capacity 등 분산 시스템의 정석적 접근
- split threshold를 적절히 설정하면 파티션당 checkpoint는 수 MB ~ 수십 MB 수준
  - 이 범위에서 full snapshot은 충분히 빠름 (JSON 직렬화 수십 MB ≈ 수십 ms)

### 개선 방안

현재 구조를 유지하면서 checkpoint 효율을 높이는 경로:

| 개선 | 효과 | 난이도 | 비고 |
|------|------|--------|------|
| **size 기반 split 트리거** | value 크기 편차 대응 | 낮음 | 현재 policy는 key count/RPS만 사용. `Countable`에 `ByteSize()` 추가하거나 stats에 snapshot_bytes 수집 |
| **압축** | checkpoint I/O 감소 | 낮음 | adapter 레벨 또는 Actor 내부에서 gzip/snappy 적용 |
| **binary 직렬화** | 직렬화 속도 + 크기 감소 | 낮음 | Actor 구현자 선택 (JSON → protobuf/msgpack). framework 변경 불필요 |
| **checkpoint 빈도 동적 조절** | 대형 파티션 부하 감소 | 중간 | 파티션 크기에 따라 checkpoint 간격 자동 조절. 복원 시 WAL replay 증가 트레이드오프 |

#### size 기반 split 트리거

현재 `ThresholdPolicy`의 split 판단 기준:
- `RPS > rps_threshold` (절대 RPS)
- `KeyCount > key_threshold` (절대 키 수)

키 수는 적지만 value가 큰 파티션(예: 대용량 blob)은 split 트리거가 걸리지 않아 checkpoint 비용이 계속 커질 수 있다. 해결 방안:

- **방안 A**: `Countable` 인터페이스에 `ByteSize() int64` 추가 → `PartitionStats`에 `ByteSize` 필드 → policy에서 `byte_threshold` 기반 split 판단
- **방안 B**: checkpoint 저장 시 크기를 `PartitionStats`에 기록 → Actor 인터페이스 변경 없이 engine 레벨에서 수집

#### 압축

Actor 구현자가 `Snapshot()`에서 압축된 bytes를 반환하거나, CheckpointStore adapter 레벨에서 투명하게 압축할 수 있다. framework 변경 불필요.

```go
// Actor 내부에서 직접 압축
func (a *myActor) Snapshot() ([]byte, error) {
    raw, _ := json.Marshal(a.state)
    return snappy.Encode(nil, raw), nil
}

// 또는 CheckpointStore wrapper로 투명 압축
type CompressedCheckpointStore struct {
    inner provider.CheckpointStore
}
```

---

## TiKV 비교 및 부족 기능 분석

actorbase에 Redis Streams WAL + S3 CheckpointStore를 사용한 경우, TiKV와 비교하여 부족한 기능과 설계 차이를 분석한다.

### 기능 비교

| 영역 | TiKV | actorbase (Redis WAL + S3 CP) |
|------|------|-------------------------------|
| **복제** | Raft 3-way 동기 복제 | 복제 없음. Redis가 WAL 공유 스토리지 역할 |
| **파티션 내 일관성** | Serializable | Serializable (single-threaded mailbox) |
| **분산 트랜잭션** | Percolator 2PC + TSO | 없음 (단일 파티션 원자성만) |
| **읽기 경로** | Snapshot isolation (follower read 가능) | mailbox 경유 (쓰기와 동일 경로) |
| **Range scan** | 단일 스냅샷 기반 일관된 읽기 | fan-out 병렬, 파티션별 독립 스냅샷 (시점 불일치 가능) |
| **Partition merge** | Region merge 지원 | 없음 (split만) |
| **장애 복구** | Raft log → 자동 복구 (데이터 손실 0) | checkpoint(S3) + WAL(Redis) replay. 데이터 손실 ~0 |
| **복구 다운타임** | Raft follower 즉시 승격 (~초) | failover 감지 + checkpoint 로드 + WAL replay (~수십 초) |
| **쓰기 지연** | 3-copy 합의 필요 (높음) | 단일 복사본 (낮음) |
| **MVCC** | RocksDB 기반 다중 버전 | 없음 |

### 부족한 기능 (우선순위 순)

#### 1. 복제 (Replication) — 가용성

가장 큰 차이. Redis WAL + S3 조합으로 **내구성(durability)은 해결**되지만, **가용성(availability)**에 차이가 있다.

- 현재: 파티션당 단일 복사본. 장비 장애 시 checkpoint(S3) + WAL(Redis) 모두 살아있으므로 데이터 손실 ~0
- **하지만**: 복원까지 다운타임 존재 (failover 감지 ~10초 + checkpoint 로드 + WAL replay)
- TiKV: Raft follower가 즉시 승격 → 다운타임 최소

읽기 전용 replica를 추가하면 가용성 개선 가능하나 난이도 높음.

#### 2. 분산 트랜잭션 — 설계 철학 차이

actorbase는 Actor 모델 → 파티션 단위 직렬화가 핵심. cross-partition 트랜잭션은 Actor 모델과 근본적으로 충돌한다.

필요 시 Saga 패턴이나 애플리케이션 레벨 보상 트랜잭션으로 해결. 이것은 "부족"이 아니라 설계 선택.

#### 3. Partition merge — 운영 편의

split만 있고 merge 없음 → 트래픽 감소 시 파티션 수가 줄지 않는다.
소형 파티션이 많아지면 메타데이터 오버헤드 + etcd 부하 증가.

#### 4. Read-only fast path — 성능

모든 읽기가 mailbox 경유 → 쓰기와 경쟁. read-only 요청은 actor 상태만 읽고 WAL 없음 → 별도 fast path 가능.

#### 5. Range scan 일관성 — 정확성

현재: 파티션별 독립 스냅샷 → cross-partition 시점 불일치 가능.
TiKV: TSO(Timestamp Oracle) 기반 단일 글로벌 스냅샷.

#### 6. MVCC — 동시성

현재: single-threaded → 자연스러운 직렬화지만 처리량 제한.
MVCC로 읽기/쓰기 분리하면 처리량 향상 가능하나 Actor 모델의 단순성과 상충.

### 결론

actorbase와 TiKV는 다른 문제를 푼다:
- **TiKV**: 범용 분산 KV. 강한 일관성, 분산 트랜잭션, MVCC가 핵심
- **actorbase**: Actor 기반 상태 관리 플랫폼. 단순성, 사용자 정의 로직 주입, 자동 파티셔닝이 핵심

Redis WAL + S3 조합으로 **내구성 문제는 해결**됨. 실용적으로 추가할 만한 기능:

| 기능 | 효과 | 난이도 |
|------|------|--------|
| **Partition merge** | 소형 파티션 통합, etcd 부하 감소 | 중간 |
| **Read-only fast path** | 읽기 처리량 향상, 쓰기 경쟁 제거 | 낮음 |
| **읽기 전용 replica** | 장애 시 다운타임 감소, 읽기 확장 | 높음 |
