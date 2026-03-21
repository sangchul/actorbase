# adapter/redis — Redis WALStore 구현 가이드

`adapter/fs`의 파일시스템 WALStore 대신 Redis를 백엔드로 사용하는 구현체.
PS를 여러 서버에 분산 배포할 때 WAL을 공유 스토리지 없이 네트워크로 접근할 수 있다.

---

## 왜 Redis Streams인가

WAL은 append-only sequential log다. Redis에는 이 개념에 1:1로 대응하는 자료구조인 **Streams**가 있다.

`store.md`의 기존 분석은 auto-generated ID(`*`)를 전제해 "큰 mismatch"라 결론 냈지만,
**XADD에 uint64 LSN을 명시적 ID로 지정하면** impedance mismatch가 거의 사라진다.

```
XADD key 42 d {data}   → Redis ID = "42-0"  (ms=42, seq=0)
XRANGE key 1 +         → "1-0" 이상 항목 순서대로 반환
XTRIM key MINID = 10   → ID < "10-0" 항목 정확히 삭제 (TrimBefore와 1:1 대응)
```

Sorted Set + Hash 대안과 비교:

| 항목 | Sorted Set + Hash | **Redis Streams** |
|---|---|---|
| 파티션당 키 수 | 3개 (lsn, idx, data) | **2개** (lsn, stream) |
| AppendBatch | INCR + ZADD + HSET | **INCR + XADD×n** |
| ReadFrom | ZRANGEBYSCORE → HMGET (2회) | **단일 XRANGE** |
| TrimBefore | ZRANGEBYSCORE + ZREMRANGEBYSCORE + HDEL | **단일 XTRIM MINID =** |
| 메모리 효율 | SkipList + Hash 노드 개별 할당 | **listpack/radix tree — 더 compact** |

---

## 데이터 모델

파티션당 2개 키를 사용한다.

```
{wal:{partitionID}}:lsn    → String  — INCR 카운터 (현재까지 발급된 최대 LSN)
{wal:{partitionID}}:stream → Stream  — XADD {lsn} d {binary_data}
```

`{wal:{partitionID}}` **해시태그**를 사용해 두 키가 Redis Cluster에서 동일 슬롯에 배치되도록 한다.

### Stream ID ↔ LSN 변환

LSN N을 Stream ID로 사용할 때 Redis는 `"N-0"` 형태로 저장한다.
XRANGE/XTRIM에 정수 N을 그대로 전달하면 Redis가 `N-0`으로 해석하므로 별도 변환 불필요.
Go 측에서 `XMessage.ID`를 파싱할 때만 `strings.SplitN(id, "-", 2)[0]`으로 정수 부분을 추출한다.

---

## 각 메서드 구현

### AppendBatch

```
1. INCR {lsn_key} by n  → end_lsn 획득, start_lsn = end_lsn - n + 1
2. 파이프라인:
   XADD {stream_key} {start_lsn}   d data[0]
   XADD {stream_key} {start_lsn+1} d data[1]
   ...
```

**Lua 스크립트가 필요하지 않다.**

non-atomic 두 단계 사이에 연결이 끊기는 경우:
- INCR만 성공하고 XADD가 전혀 안 된 경우 → LSN 번호 일부 낭비. ReadFrom이 비어있는 LSN을 자연스럽게 건너뛰므로 correctness에 무해하다.
- XADD 일부만 성공한 경우 → 클라이언트가 에러를 받고 재시도 → double-apply 가능성 있음. **그러나 actor 연산이 멱등(idempotent)하면 무해하다.** double-apply는 WALStore 레이어가 아닌 actor 설계 레이어의 책임이다.

Lua로 묶어도 "Redis 실행 성공 + 응답 유실" 케이스에서 동일하게 double-apply가 발생하므로 Lua는 이 문제를 해결하지 못한다.

### ReadFrom

```
XRANGE {stream_key} {fromLSN} +
```

`fromLSN`을 정수 문자열로 전달하면 Redis가 `fromLSN-0` 이상의 항목을 순서대로 반환한다.

### TrimBefore

```
XTRIM {stream_key} MINID = {lsn}
```

`=`(exact mode)를 지정해 근사 삭제가 아닌 정확한 경계를 보장한다.
파티션이 존재하지 않으면 no-op (에러 없음).

---

## 패키지 구조

```
adapter/redis/
├── wal.go        — WALStore 구현체
└── wal_test.go   — miniredis 기반 단위 테스트
```

### WALStore 구조체

```go
package redis

type WALStore struct {
    client redis.UniversalClient  // 단일/Sentinel/Cluster 모두 지원
    prefix string                 // 키 네임스페이스, 기본값 "wal"
}

func NewWALStore(client redis.UniversalClient, prefix string) *WALStore
```

`redis.UniversalClient`를 받으면 호출자가 단일 인스턴스, Sentinel, Cluster 중 선택할 수 있다.

---

## 의존성

```
github.com/redis/go-redis/v9       — Redis 클라이언트
github.com/alicebob/miniredis/v2   — 인메모리 Redis (테스트 전용)
```

`miniredis`는 Lua를 포함한 대부분의 Redis 명령을 지원하므로 외부 Redis 없이 단위 테스트 실행이 가능하다.

---

## 사용 예시

```go
import (
    "github.com/redis/go-redis/v9"
    adapterredis "github.com/sangchul/actorbase/adapter/redis"
)

rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
walStore := adapterredis.NewWALStore(rdb, "wal")

ps.Register(builder, ps.TypeConfig[KVRequest, KVResponse]{
    TypeID:   "kv",
    WALStore: walStore,
    // ...
})
```

---

## 운영 고려사항

- **Redis AOF 또는 RDB 활성화 권장**: Redis 재시작 시 WAL 데이터 유지를 위해 영속성 설정이 필요하다.
- **메모리 관리**: TrimBefore는 checkpoint 완료 후 호출되며, 이전 WAL 항목을 정리한다. 주기적으로 호출하지 않으면 Stream이 무한히 증가한다.
- **Redis Cluster**: 해시태그 덕분에 동일 파티션의 두 키가 같은 슬롯에 배치된다.
