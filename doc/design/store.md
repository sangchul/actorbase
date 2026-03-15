# WALStore / CheckpointStore 구현 가이드

provider 인터페이스와 실제 스토리지 백엔드의 적합성 분석.
구현 시 고려해야 할 impedance mismatch와 해결 패턴을 정리한다.

---

## 인터페이스 요약

```go
type WALStore interface {
    AppendBatch(ctx context.Context, partitionID string, data [][]byte) (lsns []uint64, err error)
    ReadFrom(ctx context.Context, partitionID string, fromLSN uint64) ([]WALEntry, error)
    TrimBefore(ctx context.Context, partitionID string, lsn uint64) error
}

type CheckpointStore interface {
    Save(ctx context.Context, partitionID string, data []byte) error
    Load(ctx context.Context, partitionID string) ([]byte, error)
    Delete(ctx context.Context, partitionID string) error
}
```

---

## WALStore ↔ Redis Streams

### AppendBatch

Redis Streams의 `XADD`는 Stream ID(`<millisecondsTime>-<sequenceNumber>` 형태 문자열)를 반환한다. 인터페이스가 요구하는 `uint64 LSN`과 타입이 다르다.

- LSN을 별도로 발급하려면 `INCR wal:{partitionID}:lsn`과 `XADD`를 조합해야 한다.
- 두 연산이 원자적이지 않으므로 `MULTI/EXEC`(트랜잭션)로 묶어야 한다.
- `AppendBatch`는 "에러 시 전체 실패"를 요구하므로, 트랜잭션 실패 시 롤백 처리가 필요하다.

→ 구현 가능하지만 LSN 관리 레이어를 별도로 설계해야 한다.

### ReadFrom

`XRANGE {stream} {startID} +`가 자연스럽게 대응된다. 단, `fromLSN → Stream ID` 변환이 필요하다. LSN↔StreamID 매핑을 일관되게 정하면 구현은 무난하다.

### TrimBefore

`XTRIM {stream} MINID {id}` (Redis 6.2+)로 구현 가능. 단 기본 동작이 근사값 삭제이므로 정확한 경계 보장을 위해 정확 모드를 명시해야 한다.

### 종합

**큰 impedance mismatch가 있다.** LSN 발급이 Redis에 내장되지 않아 LSN 관리 레이어를 별도로 구현해야 하고, 원자성 보장이 번거롭다.

**설계 대안:** Redis Streams 대신 `LIST` + 별도 counter 조합이 더 단순할 수 있다. 또는 Stream ID 자체를 LSN으로 활용하는 구조(uint64 캐스팅)로 인터페이스 사용 측의 가정을 완화하는 방법도 있다.

---

## WALStore ↔ Kafka

Kafka offset이 LSN에 가까운 개념이라 Redis Streams보다 자연스럽다.

- `AppendBatch` → `producer.SendMessages([]Message{...})`. offset은 ProduceResponse에서 얻는다.
- 파티션당 하나의 Kafka topic(또는 하나의 topic + partitionID를 key로 샤딩)이 필요하다.
- `TrimBefore` → Kafka retention 정책(`log.retention.ms`, `log.start.offset`)으로 구현. 단 Kafka의 retention은 시간/크기 기반이 일반적이라 LSN 기반 trim은 AdminClient로 `DeleteRecords`를 호출해야 한다.

→ AppendBatch/ReadFrom은 자연스럽다. TrimBefore가 다소 어색하다.

---

## CheckpointStore ↔ HDFS

3개 메서드 모두 자연스럽게 대응된다.

| 메서드 | HDFS 구현 |
|---|---|
| `Save(partitionID, data)` | `/ckpt/{partitionID}.tmp`에 쓴 후 rename |
| `Load(partitionID)` | `hdfs.ReadFile("/ckpt/{partitionID}")` |
| `Delete(partitionID)` | `hdfs.Remove("/ckpt/{partitionID}")` |

**주의:** HDFS는 파일 overwrite를 기본적으로 허용하지 않는다. `Save`를 직접 overwrite로 구현하면 `Remove` 후 `Create` 사이에 장애가 나면 파일이 사라진다.

**해결 — write-then-rename 패턴:**

```
1. /ckpt/{partitionID}.tmp 에 쓰기
2. /ckpt/{partitionID}.tmp → /ckpt/{partitionID} rename
```

HDFS `rename`은 원자적이므로 장애가 나도 이전 체크포인트가 보존된다. 이 패턴 하나만 지키면 구현이 깔끔하게 맞는다.

---

## CheckpointStore ↔ S3 (또는 GCS)

인터페이스와 완벽하게 맞는다.

| 메서드 | S3 구현 |
|---|---|
| `Save(partitionID, data)` | `PutObject(key="{partitionID}", body=data)` |
| `Load(partitionID)` | `GetObject(key="{partitionID}")` |
| `Delete(partitionID)` | `DeleteObject(key="{partitionID}")` |

S3는 overwrite가 네이티브하게 지원되고, 각 연산이 원자적이다. HDFS의 write-then-rename 같은 우회 패턴이 필요 없다.

---

## 적합성 요약

| 조합 | 인터페이스 적합성 | 구현 난이도 | 비고 |
|---|---|---|---|
| WALStore ↔ Redis Streams | **미스매치** — LSN이 Stream ID와 다름 | 중-상 | LSN 관리 레이어 필요, 원자성 처리 번거로움 |
| WALStore ↔ Kafka | 자연스러움 — offset이 LSN에 가까움 | 중 | TrimBefore가 다소 어색 |
| CheckpointStore ↔ HDFS | **잘 맞음** | 낮음 | write-then-rename 패턴 필수 |
| CheckpointStore ↔ S3/GCS | **완벽하게 맞음** | 매우 낮음 | 추가 패턴 불필요 |

---

## 인터페이스 설계 관련 메모

WALStore에서 `uint64 LSN`을 직접 반환하도록 설계한 것이 Redis Streams와의 마찰의 근본 원인이다. LSN을 WALStore 내부에서 발급하고 외부에는 opaque token으로 노출했다면 더 유연했을 것이다.

현재 구조에서도 Redis Streams 구현은 가능하다. LSN 카운터 관리 레이어를 명확히 정의하면 된다. 단, 이 레이어가 올바르게 구현되지 않으면 LSN 단조 증가 보장이 깨질 수 있으므로 주의가 필요하다.

---

## LSN이 uint64인 이유 — Redis Stream ID로 대체 불가한 이유

LSN은 단순 식별자가 아니라 engine 내부에서 세 가지 역할을 한다:

1. **Checkpoint 바이너리 포맷에 내장**: `saveCheckpoint`가 snapshot 앞에 8바이트(big-endian uint64)로 LSN을 기록한다. 복구 시 `fromLSN+1`부터 WAL을 replay한다 — `+1` 산술이 필요하다.

2. **에러 센티널**: mailbox 내부에서 `lsn=0`이 "WAL flush 실패"를 의미한다. `walConfirmedCh chan uint64`로 flusher → mailbox에 피드백하며 0이면 actor를 evict한다.

3. **대소 비교**: `ReadFrom(fromLSN)`, `TrimBefore(lsn)`에서 `<` 비교로 범위를 필터링한다.

Redis Stream ID(`"1234567890123-0"` 형태 문자열)는 이 세 가지를 모두 충족하지 못한다 — lexicographic 정렬이 숫자 정렬과 다르고, 산술 불가, 고정 크기 인코딩 불가.

**결론:** uint64 LSN은 FS 구현에 최적화된 설계가 인터페이스에 노출된 것이다. Redis Streams 구현체는 내부적으로 `INCR`로 별도 uint64 카운터를 발급하고, Stream ID와의 매핑을 관리하는 방식으로 인터페이스를 맞춰야 한다.
