# internal/engine 패키지 설계

Actor 실행 엔진. Partition Server(PS)가 사용한다.
`provider`, `internal/domain` 패키지만 의존한다.

파일 목록:
- `host.go`      — ActorHost: Actor 생명주기 (활성화/eviction/checkpoint)
- `mailbox.go`   — mailbox: Actor당 단일 스레드 실행 보장
- `flusher.go`   — WALFlusher: WAL group commit
- `scheduler.go` — EvictionScheduler, CheckpointScheduler

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

> **검증 필요:** 구현 후 벤치마크로 확인한다.

---

## 설계 배경: 주기적 Checkpoint

WAL이 무한히 누적되면 PS 재시작 시 replay 비용이 증가한다. 이를 방지하기 위해 주기적·WAL 누적 기반 checkpoint를 수행한다.

```
Checkpoint = Snapshot 저장 + WAL trim  (Actor는 메모리에 유지)
Evict      = Checkpoint + Actor 메모리에서 제거
```

**일관성 보장 (Drain-then-Snapshot)**: mailbox goroutine은 WAL entry를 제출 후 바로 다음 메시지를 처리한다. Checkpoint 시점에 "제출됐지만 아직 flush되지 않은" 상태가 있을 수 있으므로:

1. inCh(새 메시지 수신) 일시 중단
2. pendingWAL == 0이 될 때까지 대기
3. Actor.Snapshot() → CheckpointStore.Save (LSN prefix 포함)
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
| `Split(ctx, partitionID, splitKey, newPartitionID)` | 파티션 split. 비활성이면 먼저 활성화. |
| `IdleActors(idleSince)` | 마지막 메시지가 idleSince 이전인 파티션 목록. |
| `ActivePartitions()` | 현재 활성화된 모든 파티션 목록. |

### 활성화 흐름

동시에 동일 파티션 활성화 요청이 오면 하나만 실행하고 나머지는 대기한다. `actorEntry.ready` 채널로 "활성화 진행 중" 상태를 표시하고 완료 시 닫는다.

```
getOrActivate(partitionID):
  actors 맵에 이미 있으면 → ready 채널 대기 후 반환
  없으면 → placeholder 등록 후 doActivate:
    1. CheckpointStore.Load → LSN(8B big-endian prefix) + snapshotData 분리
    2. Factory(partitionID)로 Actor 생성 후 Restore(snapshotData)
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
Split(partitionID, splitKey, newPartitionID):
  1. getOrActivate(partitionID)
  2. mailbox에 split 신호 → Actor.Split(splitKey) → upperHalf
  3. mailbox.checkpoint()           // 하위 파티션 checkpoint
  4. saveRawCheckpoint(newPartitionID, lsn=0, upperHalf)
  5. Activate(newPartitionID)       // 상위 파티션 활성화
```

---

## mailbox / WALFlusher / Scheduler 개요

**mailbox**: Actor당 하나. 단일 goroutine이 inCh에서 순차적으로 메시지를 처리하여 단일 스레드 실행을 보장한다. checkpoint 요청 수신 시 inCh를 일시 중단하고 pendingWAL == 0 후 Snapshot을 찍는다.

**WALFlusher**: 공유 goroutine 하나. submitCh로 각 mailbox의 WAL entry를 수신·배치 누적 후, partitionID별로 그룹화하여 WALStore.AppendBatch를 파티션당 1회 호출한다. 결과를 `reply func(lsn uint64, err error)` 클로저로 각 mailbox에 피드백한다. 클로저 사용으로 WALFlusher 자체는 제네릭이 불필요하다.

**EvictionScheduler**: 주기적으로 IdleActors를 조회하여 Evict 호출.

**CheckpointScheduler**: 주기적으로 ActivePartitions를 조회하여 Checkpoint 호출.

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| WAL 기록 방식 | Group commit (WALFlusher 배치) | Actor 단일 스레드에서 IO 제거 → 처리량 향상 |
| `Receive` 반환값 | `(Resp, []byte, error)` | walEntry nil이면 read-only. WAL 기록 여부를 Actor가 결정. |
| Checkpoint 일관성 | Drain-then-Snapshot | inCh 중단 + pendingWAL==0 대기 후 스냅샷. 상태-WAL 불일치 방지. |
| Checkpoint 트리거 | 시간 기반 + WAL 누적 기반 | 둘 중 먼저 도달하는 조건으로 트리거. WAL 무한 증가 방지. |
| checkpointFn 주입 | ActorHost가 클로저로 mailbox에 주입 | Snapshot은 mailbox goroutine 내에서 호출해야 thread-safe. |
| WALFlusher 타입 소거 | `reply func(lsn, err)` 클로저 | WALFlusher를 제네릭으로 만들지 않아도 됨. |
| panic 처리 | `safeReceive`에서 recover → ErrActorPanicked | Actor 오류가 전체 PS를 다운시키지 않음. |
| WAL flush 실패 시 | onWALError 콜백으로 actors 맵에서 제거 | 메모리 상태-WAL 불일치 방지. checkpoint 없이 evict. |
| WAL partial write 복구 | 마지막 엔트리 replay 실패 시 skip | SIGKILL 중 truncated 엔트리 방어. 중간 실패는 여전히 에러. |

---

## 알려진 한계

- **Checkpoint 중 지연**: checkpoint 신호 수신 시 inCh가 일시 중단된다. drain 대기 + Snapshot + Save 시간만큼 해당 파티션 요청이 대기한다. 파티션 상태가 매우 크면 pause time이 길어질 수 있다.
- **WAL flush 실패 시 데이터 유실**: 실패 배치의 Actor를 evict하면 마지막 checkpoint 이후 데이터를 잃는다.
- **Group commit 성능 가설 미검증**: 구현 후 벤치마크로 확인 필요.
