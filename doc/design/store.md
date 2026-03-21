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

**구현 완료**: `adapter/redis` 패키지. 상세 설계는 `adapter-redis.md` 참고.

### 핵심 설계 결정: 명시적 uint64 ID 사용

기존 분석은 auto-generated ID(`*`)를 전제해 "큰 mismatch"라 결론 냈지만,
**XADD에 uint64 LSN을 명시적 ID로 지정하면** impedance mismatch가 거의 없다.

```
XADD key 42 d {data}   → Redis ID = "42-0"  (fromLSN 42 그대로 사용)
XRANGE key 1 +         → "1-0" 이상 항목 반환  ← ReadFrom
XTRIM key MINID = 10   → ID < "10-0" 정확히 삭제  ← TrimBefore
```

- **AppendBatch**: `INCR` (LSN 발급) → 파이프라인 `XADD×n`. Lua/MULTI-EXEC 불필요.
  - "원자성 없이 괜찮은가?" → actor 연산이 멱등(idempotent)하면 partial write의 double-apply는 무해.
  - Lua로 묶어도 "Redis 성공 + 응답 유실" 시나리오에서 동일하게 double-apply가 발생하므로 Lua는 이 문제를 해결하지 못한다.
- **ReadFrom**: 단일 `XRANGE` 명령.
- **TrimBefore**: 단일 `XTRIM MINID = {lsn}` 명령 (exact mode).

### 적합성 재평가

auto ID 대신 명시적 uint64 ID를 사용하면 Redis Streams가 WALStore에 **자연스럽게 맞는다**. Sorted Set + Hash 조합보다 키 수가 적고, ReadFrom/TrimBefore가 단일 명령으로 해결되며, 메모리 효율도 우수하다.

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

**구현 완료**: `adapter/s3` 패키지. aws-sdk-go-v2 기반, MinIO 등 S3-compatible 서비스도 endpoint override로 지원.

인터페이스와 완벽하게 맞는다.

| 메서드 | S3 구현 |
|---|---|
| `Save(partitionID, data)` | `PutObject(key="{prefix}/{partitionID}", body=data, ContentLength=len)` |
| `Load(partitionID)` | `GetObject(key="{prefix}/{partitionID}")`, NoSuchKey → `(nil, nil)` |
| `Delete(partitionID)` | `DeleteObject(key="{prefix}/{partitionID}")`, 없는 키도 성공 |

S3는 overwrite가 네이티브하게 지원되고, 각 연산이 원자적이다. HDFS의 write-then-rename 같은 우회 패턴이 필요 없다.

### 오브젝트 키 구조

```
{prefix}/{partitionID}   (prefix="" 이면 partitionID만)
```

fs의 `{baseDir}/{partitionID}.snapshot`에 대응한다.

### NoSuchKey 판별

타입 어설션 대신 코드 문자열 비교를 사용하여 S3-compatible 서비스 호환성을 확보한다.

```go
var apiErr smithy.APIError
if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey" {
    return nil, nil
}
```

### 사용 방법

```go
// AWS S3
cfg, _ := config.LoadDefaultConfig(ctx, config.WithRegion("ap-northeast-2"))
client := s3.NewFromConfig(cfg)
store := s3adapter.NewCheckpointStore(client, "my-bucket", "checkpoint")

// MinIO (또는 S3-compatible)
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithBaseEndpoint("http://localhost:9000"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, "")),
)
client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
store := s3adapter.NewCheckpointStore(client, "my-bucket", "checkpoint")
```

`kv_server`에서는 `-checkpoint-backend s3 -s3-endpoint ... -s3-bucket ... -s3-prefix ...` 플래그로 선택한다. endpoint를 지정하지 않으면 AWS SDK 기본 credential 체인(환경변수 → `~/.aws/credentials` → IAM role)을 사용한다.

---

## 적합성 요약

| 조합 | 인터페이스 적합성 | 구현 난이도 | 비고 |
|---|---|---|---|
| WALStore ↔ Redis Streams | **잘 맞음** — 명시적 uint64 ID 사용 시 | 낮음 | `adapter/redis` 구현 완료. 상세: `adapter-redis.md` |
| WALStore ↔ Kafka | 자연스러움 — offset이 LSN에 가까움 | 중 | TrimBefore가 다소 어색 |
| CheckpointStore ↔ HDFS | **잘 맞음** | 낮음 | write-then-rename 패턴 필수 |
| CheckpointStore ↔ S3/GCS | **완벽하게 맞음** | 매우 낮음 | `adapter/s3` 구현 완료. MinIO 호환. |

---

## 인터페이스 설계 관련 메모

`uint64 LSN`을 직접 반환하는 설계가 Redis Streams와 마찰할 것으로 예상됐으나,
**명시적 ID 방식**으로 구현하면 마찰이 없다. `INCR`으로 uint64 LSN을 발급하고,
그 값을 Stream ID의 ms 파트로 사용(`XADD key {lsn} d {data}`)하면
LSN ↔ Stream ID 변환 레이어 없이 인터페이스를 충족한다.

---

## LSN이 uint64인 이유 — Redis Stream ID로 대체 불가한 이유

LSN은 단순 식별자가 아니라 engine 내부에서 세 가지 역할을 한다:

1. **Checkpoint 바이너리 포맷에 내장**: `saveCheckpoint`가 snapshot 앞에 8바이트(big-endian uint64)로 LSN을 기록한다. 복구 시 `fromLSN+1`부터 WAL을 replay한다 — `+1` 산술이 필요하다.

2. **에러 센티널**: mailbox 내부에서 `lsn=0`이 "WAL flush 실패"를 의미한다. `walConfirmedCh chan uint64`로 flusher → mailbox에 피드백하며 0이면 actor를 evict한다.

3. **대소 비교**: `ReadFrom(fromLSN)`, `TrimBefore(lsn)`에서 `<` 비교로 범위를 필터링한다.

Redis Stream ID(`"1234567890123-0"` 형태 문자열)는 이 세 가지를 모두 충족하지 못한다 — lexicographic 정렬이 숫자 정렬과 다르고, 산술 불가, 고정 크기 인코딩 불가.

**결론:** auto-generated Stream ID(`"1234567890123-0"`)를 LSN으로 쓰면 이 세 가지가 충족되지 않는다. 그러나 `INCR`으로 uint64 LSN을 발급하고 이를 XADD의 명시적 ID로 사용하면 별도 매핑 없이 인터페이스를 충족한다 — `adapter/redis`의 구현 방식이다.
