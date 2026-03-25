# internal/engine 패키지 설계

Actor 실행 엔진. Partition Server(PS)가 사용한다.
`provider`, `internal/domain` 패키지만 의존한다.

파일 목록:
- `host.go`      — ActorHost: Actor 생명주기 (활성화/eviction/checkpoint)
- `mailbox.go`   — mailbox: Actor당 단일 스레드 실행 보장
- `flusher.go`   — WALFlusher: WAL group commit
- `scheduler.go` — EvictionScheduler, CheckpointScheduler
- `stats.go`     — PartitionStats, rpsCounter

---

## 설계 배경: WAL Group Commit

Actor는 단일 goroutine으로 실행된다. 이 goroutine에서 WAL IO를 직접 수행하면 IO 대기 시간만큼 Actor가 멈춰 처리량이 떨어진다.

이를 해결하기 위해 **group commit** 패턴을 적용한다.

```
Actor goroutine                  WALFlusher goroutine

Receive(req) →
  (resp, walEntry, err)
        │
  walEntry != nil?──No──→ caller에게 즉시 응답
        │ Yes
        ▼
  WALFlusher.Submit()    ←── Actor는 여기서 반환. 다음 메시지 처리 시작.
        │
        │  (배치 누적: flushSize 또는 flushInterval 기준)
        │
        ▼
  WALStore.AppendBatch() (partitionID별 그룹화)
        │
  성공 → 각 reply(lsn, nil)  →  caller에게 응답
  실패 → 각 reply(0, err)
```

Actor goroutine은 WAL IO를 기다리지 않는다. WALFlusher가 여러 Actor의 WAL entry를 모아 배치로 처리하므로 IO 횟수가 줄어든다.

---

## 설계 배경: 주기적 Checkpoint

WAL이 무한히 누적되면 PS 재시작 시 replay 비용이 증가한다. 이를 방지하기 위해 주기적·WAL 누적 기반 checkpoint를 수행한다.

```
Checkpoint = Export("") 저장 + WAL trim  (Actor는 메모리에 유지)
Evict      = Checkpoint + Actor 메모리에서 제거
```

**일관성 보장 (Drain-then-Snapshot)**: mailbox goroutine은 WAL entry를 제출 후 바로 다음 메시지를 처리한다. Checkpoint 시점에 "제출됐지만 아직 flush되지 않은" 상태가 있을 수 있으므로:

1. inCh(새 메시지 수신) 일시 중단
2. pendingWAL == 0이 될 때까지 대기
3. Actor.Export("") → CheckpointStore.Save (LSN prefix 포함)
4. WALStore.TrimBefore(confirmedLSN)
5. inCh 재개

**Checkpoint 트리거:**

| 트리거 | 조건 | 주체 |
|---|---|---|
| 시간 기반 | interval마다 | CheckpointScheduler |
| WAL 누적 기반 | N개 WAL entry 확인마다 | mailbox 내부 카운터 |

---

## ActorHost 공개 API

| 메서드 | 설명 |
|---|---|
| `Send(ctx, partitionID, req)` | Actor에 요청 전달. 비활성이면 먼저 활성화. |
| `Activate(ctx, partitionID)` | 명시적 활성화. 이미 활성이면 no-op. |
| `Checkpoint(ctx, partitionID)` | checkpoint 수행. Actor는 메모리에 유지. |
| `Evict(ctx, partitionID)` | checkpoint 후 메모리에서 제거. |
| `EvictAll(ctx)` | 모든 활성 Actor를 evict. 서버 종료 시 사용. |
| `Split(ctx, partitionID, splitKey, keyRangeStart, keyRangeEnd, newPartitionID) (string, error)` | 파티션 split. splitKey=""이면 SplitHint() 또는 midpoint 사용. 실제 사용된 splitKey 반환. |
| `Merge(ctx, lowerPartitionID, upperPartitionID) error` | 인접 파티션 merge. upper의 상태를 lower에 흡수. upper는 evict + checkpoint/WAL 삭제. |
| `KeyRangeMidpoint(start, end string) string` | 두 문자열의 바이트 레벨 산술 평균. SplitHinter 미구현 Actor의 split key 결정에 사용. |
| `IdleActors(idleSince)` | 마지막 메시지가 idleSince 이전인 파티션 목록. |
| `ActivePartitions()` | 현재 활성화된 모든 파티션 목록. |
| `GetStats()` | 활성 Actor 전체의 통계(`[]PartitionStats`) 반환. PM Auto Balancer가 사용. |

### 활성화 흐름

동시에 동일 파티션 활성화 요청이 오면 하나만 실행하고 나머지는 대기한다. `actorEntry.ready` 채널로 "활성화 진행 중" 상태를 표시하고 완료 시 닫는다.

```
getOrActivate(partitionID):
  actors 맵에 이미 있으면 → ready 채널 대기 후 반환
  없으면 → placeholder 등록 후 doActivate:
    1. CheckpointStore.Load → LSN(8B big-endian prefix) + snapshotData 분리
    2. Factory(partitionID)로 Actor 생성 후 Import(snapshotData)
    3. WALStore.ReadFrom(checkpointLSN+1) → WAL replay
       └ 마지막 엔트리 replay 실패 → partial write로 간주하고 skip (warning)
       └ 중간 엔트리 실패 → 에러 반환 (실제 데이터 손상)
    4. mailbox 생성 및 goroutine 시작
    5. ready 채널 닫기
  doActivate 실패 시: actors 맵에서 제거 + ready 닫기
```

> **CheckpointStore 저장 형식**: snapshot 앞에 checkpoint LSN 8바이트(big-endian)를 prepend하여 저장한다. 사용자는 이 형식을 알 필요 없다.

### Split 처리 흐름

```
Split(partitionID, splitKey, keyRangeStart, keyRangeEnd, newPartitionID):
  1. getOrActivate(partitionID)
  2. mailbox에 split 신호 전달 (splitKey, keyRangeStart, keyRangeEnd 포함)
     mailbox goroutine 내에서 split key 결정:
       a. splitKey != "" → 그대로 사용 (호출자가 명시)
       b. splitKey == "" && actor가 SplitHinter 구현 → SplitHint() 호출
       c. splitKey == "" (또는 SplitHint() == "") → KeyRangeMidpoint(start, end)
     결정된 splitKey로 Actor.Export(splitKey) → upperHalf
  3. mailbox.checkpoint()           // 하위 파티션 checkpoint
  4. saveRawCheckpoint(newPartitionID, lsn=0, upperHalf)
  5. Activate(newPartitionID)       // 상위 파티션 활성화
  6. 실제 사용된 splitKey 반환
```

### Merge 처리 흐름

```
Merge(lowerPartitionID, upperPartitionID):
  1. getOrActivate(upperPartitionID)
  2. upperEntry.mailbox.export(ctx, "", "", "") → upperData   // 전체 상태
  3. upper actor 삭제 (actors 맵에서 제거 + mailbox.close)
  4. CheckpointStore.Delete(upperPartitionID) + WALStore.TrimBefore(upper, max)
  5. getOrActivate(lowerPartitionID)
  6. lowerEntry.mailbox.importData(ctx, upperData)            // lower에 합침
  7. lowerEntry.mailbox.checkpoint(ctx)                       // merged 상태 저장
```

---

## mailbox / WALFlusher / Scheduler 개요

**mailbox**: Actor당 하나. 단일 goroutine이 inCh에서 순차적으로 메시지를 처리하여 단일 스레드 실행을 보장한다. checkpoint 요청 수신 시 inCh를 일시 중단하고 pendingWAL == 0 후 Export("")을 찍는다. 각 mailbox는 `rpsCounter`(60초 슬라이딩 윈도우)와 `keyCount atomic.Int64`를 보유하여 stats를 실시간 추적한다. split 신호 수신 시 splitKey 결정(SplitHinter → midpoint fallback)을 mailbox goroutine 내에서 수행하므로 actor 상태에 별도 동기화 없이 안전하게 접근한다.

**WALFlusher**: 공유 goroutine 하나. submitCh로 각 mailbox의 WAL entry를 수신·배치 누적 후, partitionID별로 그룹화하여 WALStore.AppendBatch를 파티션당 1회 호출한다. 결과를 `reply func(lsn uint64, err error)` 클로저로 각 mailbox에 피드백한다. 클로저 사용으로 WALFlusher 자체는 제네릭이 불필요하다.

**EvictionScheduler**: 주기적으로 IdleActors를 조회하여 Evict 호출.

**CheckpointScheduler**: 주기적으로 ActivePartitions를 조회하여 Checkpoint 호출.

## stats.go

```go
// PartitionStats는 파티션 하나의 런타임 통계.
type PartitionStats struct {
    PartitionID string
    KeyCount    int64   // -1이면 actor가 Countable 미구현
    RPS         float64 // 60초 슬라이딩 윈도우 평균
}

// rpsCounter는 60초 슬라이딩 윈도우 RPS 카운터.
// 초 단위 버킷 60개를 사용하며 goroutine-safe하다.
// 별도 goroutine 없이 inc() 호출 시점에 버킷을 갱신한다.
type rpsCounter struct { ... }
func (r *rpsCounter) inc()                     // Receive 성공 시 호출
func (r *rpsCounter) rps(window int) float64  // 최근 window초 평균 RPS
```

mailbox에서의 stats 갱신 흐름:
- `safeReceive` 성공 → `rps.inc()`
- WAL flush 확인 후 → actor가 `Countable`이면 `keyCount.Store(actor.KeyCount())`

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| WAL 기록 방식 | Group commit (WALFlusher 배치) | Actor 단일 스레드에서 IO 제거 → 처리량 향상 |
| `Receive` 반환값 | `(Resp, []byte, error)` | walEntry nil이면 read-only. WAL 기록 여부를 Actor가 결정. |
| Checkpoint 일관성 | Drain-then-Snapshot | inCh 중단 + pendingWAL==0 대기 후 스냅샷. 상태-WAL 불일치 방지. |
| Checkpoint 트리거 | 시간 기반 + WAL 누적 기반 | 둘 중 먼저 도달하는 조건으로 트리거. WAL 무한 증가 방지. |
| checkpointFn 주입 | ActorHost가 클로저로 mailbox에 주입 | Export("")은 mailbox goroutine 내에서 호출해야 thread-safe. |
| WALFlusher 타입 소거 | `reply func(lsn, err)` 클로저 | WALFlusher를 제네릭으로 만들지 않아도 됨. |
| panic 처리 | `safeReceive`에서 recover → ErrActorPanicked | Actor 오류가 전체 PS를 다운시키지 않음. |
| WAL flush 실패 시 | onWALError 콜백으로 actors 맵에서 제거 | 메모리 상태-WAL 불일치 방지. checkpoint 없이 evict. |
| WAL partial write 복구 | 마지막 엔트리 replay 실패 시 skip | SIGKILL 중 truncated 엔트리 방어. 중간 실패는 여전히 에러. |

---

## 알려진 한계

- **Checkpoint 중 지연**: checkpoint 신호 수신 시 inCh가 일시 중단된다. drain 대기 + Snapshot + Save 시간만큼 해당 파티션 요청이 대기한다. 파티션 상태가 매우 크면 pause time이 길어질 수 있다.
- **WAL flush 실패 시 데이터 유실**: 실패 배치의 Actor를 evict하면 마지막 checkpoint 이후 데이터를 잃는다.
- **단일 파티션 병렬 처리량 상한**: actor goroutine이 단일 스레드이므로 파티션당 처리량은 goroutine 포화 시 ~370K ops/s(M2 Pro 기준). 이 이상은 파티션 분할이 필요하다.

---

## 처리량 벤치마크 (`BenchmarkActorHost_*`)

`internal/engine/bench_test.go`에 수록. nopActor(비즈니스 로직 없음)를 사용하여 engine 자체의 overhead를 측정한다. 실제 Actor는 추가 처리 비용이 있으므로 이 수치가 상한이다.

**환경:** Apple M2 Pro, GOMAXPROCS=10, Go 1.26

---

### 1. Read — WAL 없는 읽기 처리량 (mailbox 상한)

```
BenchmarkActorHost_Read     ~1,086,000 ops/s   (~921 ns/op)
```

`walEntry=nil` 경로: actor.Receive 완료 즉시 caller에게 응답. 추가 channel op나 goroutine wake-up 없이 순수 mailbox 왕복 비용만 포함된다. 이 수치가 엔진의 처리 속도 상한이다.

**해석:** actor 비즈니스 로직이 1µs라면 ~1M ops/s, 10µs라면 ~100K ops/s.

---

### 2. Write + MemWAL — 순차 단일 발신자의 쓰기 처리량

```
BenchmarkActorHost_Write_MemWAL     ~200 ops/s   (FlushInterval=5ms)
```

group commit 경로(actor → flusher → reply)를 통과하지만 WAL IO 자체는 메모리다. 처리량은 WAL 속도가 아니라 **FlushInterval**에 의해 결정된다.

단일 발신자 시: `throughput ≈ 1 / FlushInterval`

5ms FlushInterval이면 caller는 최대 5ms를 기다려야 하므로 최대 200 ops/s. 이는 낮아보이지만 의도된 동작이다 — 그룹에 묶일 발신자가 없으면 batch 이득이 없다. 실제 서비스에서는 항상 다수의 동시 발신자가 존재한다.

---

### 3. Write + SlowWAL — group commit 확장성 (핵심)

배치당 500µs 지연(일반 SSD fsync 수준)의 WAL, FlushInterval=10ms, GOMAXPROCS=10.

`b.SetParallelism(N)`은 N×GOMAXPROCS goroutine을 생성하므로 실제 goroutine 수는 ×10.

```
concurrency= 1 (  10 goroutine)     ~    999 ops/s   (1.0×)
concurrency= 4 (  40 goroutine)     ~  3,998 ops/s   (4.0×)  ← 선형 확장
concurrency=16 ( 160 goroutine)     ~ 15,980 ops/s  (16.0×)  ← 선형 확장
concurrency=64 ( 640 goroutine)     ~369,545 ops/s  (370×)   ← actor goroutine 포화
```

**group commit 확장 원리:**

concurrency=1~16 구간에서 처리량이 concurrency에 정비례한다. 이것이 group commit의 핵심 효과다.

```
concurrency=1:  10 goroutine이 각자 flusher를 깨움 → 배치당 ~10개 entry
concurrency=4:  40 goroutine → 배치당 ~40개 entry
concurrency=16: 160 goroutine → 배치당 ~160개 entry

WAL IO 횟수: 같음. 처리한 entry 수: 각각 10배, 40배, 160배.
```

Actor goroutine은 WAL flush를 기다리지 않는다. 160개의 caller가 각자 replyCh를 들고 기다리는 동안 actor goroutine은 계속 다음 메시지를 처리한다. flusher가 10ms마다 발화하여 쌓인 160개를 한 번의 AppendBatch로 처리하고, 160개의 replyCh에 일괄 응답한다.

**concurrency=64 (640 goroutine):** 선형 확장이 무너지며 ~370K ops/s로 수렴한다. 이 시점에서 actor goroutine 자체가 병목이다 — mailbox가 항상 포화 상태여서 actor는 쉬지 않고 돌지만, 단일 goroutine의 처리 속도 한계에 도달한 것이다. Read benchmark(~1M ops/s)보다 낮은 이유는 WAL 경로의 추가 channel op(submitCh 전송, walConfirmedCh 수신) 때문이다.

---

### 4. FlushInterval — latency vs throughput 트레이드오프

WAL 지연 500µs, concurrency=16 (160 goroutine), FlushSize=256.

```
interval= 1ms    ~145,191 ops/s
interval=10ms    ~ 15,974 ops/s   (1ms 대비 ~9× 감소, interval 10× 증가)
interval=50ms    ~  3,182 ops/s   (10ms 대비 ~5× 감소, interval 5× 증가)
```

**처리량 ∝ 1/FlushInterval** (고정 concurrency에서): 각 caller의 처리량은 FlushInterval당 1 op이므로, 160 caller × (1000ms/interval) = 이론 처리량.

- `interval=1ms`: 이론 160,000 ops/s, 실제 145,000 (~91% 효율). 짧은 interval에서는 flusher의 스케줄링 overhead 비중이 커진다.
- `interval=10ms`: 이론 16,000 ops/s, 실제 16,000 (~100% 효율).
- `interval=50ms`: 이론 3,200 ops/s, 실제 3,200 (~100% 효율).

**FlushInterval 선택 기준:**

| 환경 | 권장 FlushInterval | 이유 |
|---|---|---|
| 인메모리 WAL (테스트) | 1~5ms | disk 없음, 짧아도 무방 |
| SSD (NVMe, fsync ~100µs) | 1~5ms | 짧은 interval도 disk가 소화 가능 |
| SSD (SATA, fsync ~500µs) | 5~10ms | 기본값 10ms가 적합 |
| HDD / 네트워크 WAL (fsync ~5ms) | 20~50ms | 큰 배치로 IO 횟수 절감 |
| 저지연 우선 (쓰기 처리량 희생 가능) | 1ms | 각 op 최대 1ms 대기 |

WAL IO 지연보다 FlushInterval이 작으면 flusher가 매번 1~2개 entry만 처리하게 되어 group commit 이득이 없다. **FlushInterval ≥ 예상 fsync 지연**을 지키는 것이 기본 원칙이다.
