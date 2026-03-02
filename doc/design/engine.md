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

이를 해결하기 위해 PostgreSQL WAL에서 쓰는 **group commit** 패턴을 적용한다.

```
Actor goroutine                  WALFlusher goroutine

Receive(req) →
  (resp, walEntry, err)
        │
  walEntry != nil?──No──→ replyCh에 즉시 전달
        │ Yes
        ▼
  WALFlusher.Submit()    ←── Actor는 여기서 반환. 다음 메시지 처리 시작.
        │
        │  (배치 누적: flushSize 또는 flushInterval 기준)
        │
        ▼
  WALStore.Append() (배치)
        │
  성공 → 각 reply(lsn, nil)
  실패 → 각 reply(0, err)
```

Actor goroutine은 WAL IO를 기다리지 않는다. WALFlusher가 여러 Actor의 WAL entry를 모아 배치로 처리하므로 IO 횟수가 줄어든다. 응답 지연은 flushInterval만큼 증가하지만, 단일 스레드 IO 대비 처리량이 크게 향상될 것으로 예상한다.

> **검증 필요:** 이 가설은 구현 후 벤치마크로 확인한다.

---

## 설계 배경: 주기적 Checkpoint

활성 Actor(지속적으로 요청을 받는 파티션)는 Eviction이 발생하지 않으므로 WAL이 무한히 누적된다. WAL이 길어질수록 PS 재시작 시 Replay 비용이 증가한다. 이를 방지하기 위해 주기적 또는 WAL 누적 기반으로 Checkpoint를 수행한다.

**Checkpoint vs Evict:**

```
Checkpoint = Snapshot 저장 + WAL trim  (Actor는 메모리에 유지)
Evict      = Checkpoint + Actor 메모리에서 제거
```

**일관성 보장 (Drain-then-Snapshot):**

mailbox goroutine은 WALFlusher에게 entry를 제출하고 즉시 다음 메시지를 처리한다. Checkpoint 시점에 Actor의 메모리 상태가 "아직 WAL에 기록되지 않은" 상태를 포함할 수 있다. 이를 방지하기 위해 다음 순서를 따른다:

```
1. 새 메시지 수신 중단  (inCh 일시 정지)
2. WALFlusher에 제출된 pending entry가 모두 확인될 때까지 대기
   (pendingWAL 카운터가 0이 될 때까지)
3. 이 시점 Actor 상태 == 마지막 확인된 WAL 상태
4. Actor.Snapshot() 호출 → LSN prepend → CheckpointStore.Save
5. WALStore.TrimBefore(confirmedLSN) 호출
6. 새 메시지 수신 재개
```

**Checkpoint 트리거 두 가지:**

| 트리거 | 조건 | 주체 |
|---|---|---|
| 시간 기반 | interval마다 | `CheckpointScheduler` |
| WAL 누적 기반 | N개 WAL entry 확인마다 | mailbox goroutine 내부 카운터 |

---

## provider/actor.go 인터페이스

engine 설계에 따라 provider/actor.go의 `Actor` 인터페이스가 확정된다.

```go
type Actor[Req, Resp any] interface {
    // Receive는 요청을 처리하고 응답과 WAL 데이터를 반환한다.
    // walEntry가 nil이면 read-only 연산이며 WAL에 기록하지 않는다.
    // walEntry가 nil이 아니면 engine이 WALStore에 기록한 뒤 응답을 반환한다.
    Receive(ctx Context, req Req) (resp Resp, walEntry []byte, err error)

    // Replay는 WAL entry 하나를 Actor 상태에 적용한다.
    // 복구 시 마지막 checkpoint 이후의 WAL entries를 순서대로 적용하는 데 사용한다.
    Replay(entry []byte) error

    // Snapshot은 현재 Actor 상태를 직렬화하여 반환한다. (checkpoint용)
    Snapshot() ([]byte, error)

    // Restore는 Snapshot 데이터로 Actor 상태를 복원한다.
    Restore(data []byte) error

    // Split은 splitKey 기준으로 상위 절반의 상태를 직렬화하여 반환하고,
    // 자신의 상태에서 해당 데이터를 제거한다.
    Split(splitKey string) (upperHalf []byte, err error)
}
```

### Actor 사용 예시

```go
func (a *BucketActor) Receive(ctx provider.Context, req BucketRequest) (BucketResponse, []byte, error) {
    switch req.Op {
    case "get":
        obj, ok := a.objects[req.Key]
        if !ok {
            return BucketResponse{}, nil, provider.ErrNotFound
        }
        return BucketResponse{Meta: obj}, nil, nil  // read: walEntry = nil

    case "put":
        a.objects[req.Key] = req.Meta
        entry := marshalPutOp(req)
        return BucketResponse{}, entry, nil          // write: walEntry 반환

    case "delete":
        delete(a.objects, req.Key)
        entry := marshalDeleteOp(req)
        return BucketResponse{}, entry, nil
    }
    return BucketResponse{}, nil, fmt.Errorf("unknown op: %s", req.Op)
}

func (a *BucketActor) Replay(entry []byte) error {
    op := unmarshalOp(entry)
    switch op.Type {
    case "put":
        a.objects[op.Key] = op.Meta
    case "delete":
        delete(a.objects, op.Key)
    }
    return nil
}
```

---

## host.go

```go
// Config는 ActorHost 생성에 필요한 의존성을 담는다.
type Config[Req, Resp any] struct {
    Factory              provider.ActorFactory[Req, Resp]
    CheckpointStore      provider.CheckpointStore
    WALStore             provider.WALStore
    MailboxSize          int           // mailbox 버퍼 크기. 기본값은 구현 시 결정.
    FlushSize            int           // WAL 배치 최대 크기
    FlushInterval        time.Duration // WAL 배치 최대 대기 시간
    CheckpointInterval   time.Duration // 주기적 checkpoint 간격. CheckpointScheduler가 사용.
    CheckpointWALThreshold int         // 이 수만큼 WAL entry가 쌓이면 자동 checkpoint.
    Metrics              provider.Metrics
}

// ActorHost는 파티션 서버 내 모든 Actor의 생명주기를 관리한다.
type ActorHost[Req, Resp any] struct {
    cfg     Config[Req, Resp]
    actors  map[string]*actorEntry[Req, Resp] // partitionID → actorEntry
    mu      sync.Mutex
    flusher *WALFlusher
}

type actorEntry[Req, Resp any] struct {
    actor        provider.Actor[Req, Resp]
    mailbox      *mailbox[Req, Resp]
    lastMsg      atomic.Value  // time.Time; EvictionScheduler가 사용
    confirmedLSN atomic.Uint64 // WALFlusher가 확인한 마지막 LSN; CheckpointScheduler가 참조
}

func NewActorHost[Req, Resp any](cfg Config[Req, Resp]) *ActorHost[Req, Resp]

// Send는 partitionID의 Actor에 req를 전달하고 응답을 기다린다.
// Actor가 비활성이면 activate를 먼저 호출한다.
func (h *ActorHost[Req, Resp]) Send(ctx context.Context, partitionID string, req Req) (Resp, error)

// Checkpoint는 partitionID의 Actor 상태를 CheckpointStore에 저장하고 WAL을 trim한다.
// Actor는 메모리에 유지된다.
// 내부적으로 mailbox에 checkpoint 신호를 보내고 완료를 기다린다.
func (h *ActorHost[Req, Resp]) Checkpoint(ctx context.Context, partitionID string) error

// Evict는 partitionID의 Actor를 메모리에서 제거한다.
// 내부적으로 Checkpoint를 먼저 수행한 뒤 Actor를 actors 맵에서 삭제한다.
func (h *ActorHost[Req, Resp]) Evict(ctx context.Context, partitionID string) error

// EvictAll은 모든 활성 Actor를 evict한다. 서버 종료 시 호출한다.
func (h *ActorHost[Req, Resp]) EvictAll(ctx context.Context) error

// IdleActors는 lastMsg가 idleSince 이전인 partitionID 목록을 반환한다.
// EvictionScheduler가 주기적으로 호출한다.
func (h *ActorHost[Req, Resp]) IdleActors(idleSince time.Time) []string

// ActivePartitions는 현재 활성화된 모든 partitionID 목록을 반환한다.
// CheckpointScheduler가 주기적으로 호출한다.
func (h *ActorHost[Req, Resp]) ActivePartitions() []string
```

### 활성화 흐름 (activate, 비공개)

```
activate(partitionID):
  1. CheckpointStore.Load(partitionID)
       → (snapshotData, checkpointLSN) 언패킹
  2. actor = Factory(partitionID)
  3. actor.Restore(snapshotData)          // 스냅샷 복원
  4. WALStore.ReadFrom(partitionID, checkpointLSN+1)
  5. for entry in walEntries:
         actor.Replay(entry.Data)         // WAL replay
  6. mailbox 생성 및 goroutine 시작
  7. actors 맵에 등록
```

> **CheckpointStore LSN 저장 방식:** engine이 `Snapshot()` 반환값 앞에 checkpoint LSN(8 bytes, big-endian)을 prepend하여 저장한다. 사용자는 이 구조를 알 필요 없다.

---

## mailbox.go

```go
// envelope은 Actor에 전달할 메시지 하나.
type envelope[Req, Resp any] struct {
    ctx     context.Context
    req     Req
    replyCh chan<- result[Resp]
}

// result는 Actor 처리 완료 후 caller에게 전달되는 최종 결과.
type result[Resp any] struct {
    resp Resp
    err  error
}

// actorOutput은 Actor.Receive 후 WALFlusher에 전달하는 페이로드.
type actorOutput[Resp any] struct {
    partitionID string
    walEntry    []byte
    reply       func(lsn uint64, err error) // WAL flush 결과를 caller와 mailbox 양쪽에 전달
}

// checkpointReq는 외부에서 mailbox에 checkpoint를 요청할 때 사용하는 구조체.
type checkpointReq struct {
    done chan<- error
}

// mailbox는 하나의 Actor에 대한 메시지 큐.
// 단일 goroutine(run)이 메시지를 순차 처리하여 Actor의 단일 스레드 실행을 보장한다.
type mailbox[Req, Resp any] struct {
    inCh           chan envelope[Req, Resp]
    outCh          chan<- actorOutput[Resp] // WALFlusher의 수신 채널
    walConfirmedCh chan uint64             // WALFlusher가 LSN 확인 후 여기로 알림
    checkpointCh   chan checkpointReq      // 외부 checkpoint 요청
    cancel         context.CancelFunc
}

func newMailbox[Req, Resp any](
    inSize         int,
    outCh          chan<- actorOutput[Resp],
    checkpointFn   func(ctx context.Context, lsn uint64) error, // Snapshot+Save+Trim 수행
    walThreshold   int,
) *mailbox[Req, Resp]

// send는 req를 inCh에 넣고 result를 기다린다.
// ctx 만료 시 즉시 provider.ErrTimeout 반환.
func (m *mailbox[Req, Resp]) send(ctx context.Context, req Req) (Resp, error)

// checkpoint는 mailbox에 checkpoint 요청을 보내고 완료를 기다린다.
func (m *mailbox[Req, Resp]) checkpoint(ctx context.Context) error

// close는 mailbox goroutine을 종료한다.
func (m *mailbox[Req, Resp]) close()
```

### mailbox goroutine (run) 처리 흐름

```go
func (m *mailbox[Req, Resp]) run(actor provider.Actor[Req, Resp], partitionID string) {
    pendingWAL := 0           // WALFlusher에 제출했지만 아직 확인 안 된 write 수
    confirmedLSN := uint64(0) // WALFlusher가 마지막으로 확인한 LSN
    walsSinceCheckpoint := 0  // 마지막 checkpoint 이후 확인된 WAL entry 수

    var checkpointDone chan<- error // 현재 진행 중인 checkpoint 완료 채널 (없으면 nil)

    for {
        // checkpoint 대기 중이면 새 메시지 수신 중단 (nil 채널은 영원히 블로킹)
        var inCh <-chan envelope[Req, Resp]
        if checkpointDone == nil {
            inCh = m.inCh
        }

        select {
        case env, ok := <-inCh:
            if !ok {
                return
            }
            resp, walEntry, err := safeReceive(actor, env) // panic recover 포함

            if walEntry == nil || err != nil {
                env.replyCh <- result[Resp]{resp: resp, err: err}
                continue
            }

            // write: WALFlusher에 위임
            pendingWAL++
            reply := func(lsn uint64, flushErr error) {
                if flushErr != nil {
                    env.replyCh <- result[Resp]{err: flushErr}
                } else {
                    env.replyCh <- result[Resp]{resp: resp}
                }
                m.walConfirmedCh <- lsn // mailbox goroutine에 LSN 알림
            }
            m.outCh <- actorOutput[Resp]{
                partitionID: partitionID,
                walEntry:    walEntry,
                reply:       reply,
            }

        case lsn := <-m.walConfirmedCh:
            confirmedLSN = lsn
            pendingWAL--
            walsSinceCheckpoint++

            // checkpoint 완료 조건: drain 완료 (pendingWAL == 0)
            if pendingWAL == 0 && checkpointDone != nil {
                checkpointDone <- m.checkpointFn(confirmedLSN)
                checkpointDone = nil
                walsSinceCheckpoint = 0
            }

            // WAL 누적 기반 자동 checkpoint 트리거
            if checkpointDone == nil && walsSinceCheckpoint >= m.walThreshold {
                if pendingWAL == 0 {
                    m.checkpointFn(confirmedLSN) // 즉시 수행
                    walsSinceCheckpoint = 0
                } else {
                    checkpointDone = make(chan error, 1) // 내부 트리거: 완료 통보 불필요
                }
            }

        case req := <-m.checkpointCh:
            if pendingWAL == 0 {
                req.done <- m.checkpointFn(confirmedLSN)
                walsSinceCheckpoint = 0
            } else {
                checkpointDone = req.done // drain 완료 시 수행
            }
        }
    }
}
```

> `safeReceive`는 `Actor.Receive` 호출을 `recover`로 감싸 panic 발생 시 `provider.ErrActorPanicked`를 반환한다.

> `checkpointFn`은 `ActorHost`가 mailbox 생성 시 주입하는 클로저. `actor.Snapshot()` 호출, LSN prepend, `CheckpointStore.Save`, `WALStore.TrimBefore`를 순서대로 수행한다. mailbox goroutine 내에서 호출되므로 `actor.Snapshot()`이 thread-safe하게 실행된다.

---

## flusher.go

```go
// walPending은 WAL flush 대기 중인 항목 하나.
type walPending struct {
    partitionID string
    entry       []byte
    reply       func(lsn uint64, err error)
}

// WALFlusher는 여러 Actor의 WAL entry를 모아 배치로 WALStore에 기록한다.
// - submitCh로 walPending을 수신한다.
// - flushSize 또는 flushInterval 기준으로 배치를 flush한다.
// - flush 성공 시 각 reply(lsn, nil), 실패 시 각 reply(0, err) 호출.
type WALFlusher struct {
    walStore      provider.WALStore
    submitCh      chan walPending
    flushSize     int
    flushInterval time.Duration
}

func NewWALFlusher(
    walStore      provider.WALStore,
    flushSize     int,
    flushInterval time.Duration,
) *WALFlusher

// Submit은 WAL 기록 대기 항목을 추가한다.
// mailbox goroutine에서 호출한다.
func (f *WALFlusher) Submit(p walPending)

// Start는 flush 루프를 시작한다. ctx 취소 시 종료.
func (f *WALFlusher) Start(ctx context.Context)
```

### flush 루프 동작

```
loop:
  1. submitCh에서 pending 수집
     - flushSize에 도달하거나
     - flushInterval 타이머 만료 시 flush 트리거
  2. pending을 partitionID로 그룹화
  3. 각 partitionID별 WALStore.Append 호출 → lsn 수신
  4. 전체 성공 → 모든 reply(lsn, nil) 호출
     부분 실패 → 실패한 partitionID의 reply(0, err) 호출
                  성공한 partitionID의 reply(lsn, nil) 호출
```

---

## scheduler.go

```go
// EvictionScheduler는 idle Actor를 주기적으로 evict한다.
type EvictionScheduler[Req, Resp any] struct {
    host        *ActorHost[Req, Resp]
    idleTimeout time.Duration // 이 시간 동안 메시지가 없으면 evict
    interval    time.Duration // 검사 주기
}

func NewEvictionScheduler[Req, Resp any](
    host        *ActorHost[Req, Resp],
    idleTimeout time.Duration,
    interval    time.Duration,
) *EvictionScheduler[Req, Resp]

// Start는 eviction 루프를 시작한다. ctx 취소 시 종료.
func (s *EvictionScheduler[Req, Resp]) Start(ctx context.Context)

// CheckpointScheduler는 활성 Actor를 주기적으로 checkpoint한다.
// idle Actor는 EvictionScheduler가 처리하므로 여기서는 제외해도 무방하나,
// 중복 checkpoint가 발생해도 무해하므로 전체 활성 Actor에 적용한다.
type CheckpointScheduler[Req, Resp any] struct {
    host     *ActorHost[Req, Resp]
    interval time.Duration // checkpoint 주기
}

func NewCheckpointScheduler[Req, Resp any](
    host     *ActorHost[Req, Resp],
    interval time.Duration,
) *CheckpointScheduler[Req, Resp]

// Start는 checkpoint 루프를 시작한다. ctx 취소 시 종료.
func (s *CheckpointScheduler[Req, Resp]) Start(ctx context.Context)
```

### eviction 루프 동작

```
interval마다:
  1. host.IdleActors(now - idleTimeout) 호출
  2. 각 partitionID에 대해 host.Evict 호출
     (Evict 내부: Checkpoint 수행 → Actor 메모리 제거)
```

### checkpoint 루프 동작

```
interval마다:
  1. host.ActivePartitions() 호출
  2. 각 partitionID에 대해 host.Checkpoint 호출
     (Checkpoint 내부: mailbox에 checkpoint 신호 → drain → Snapshot → WAL trim)
```

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| WAL 기록 방식 | Group commit (WALFlusher 배치) | Actor 단일 스레드에서 IO 제거 → 처리량 향상 |
| `Receive` 반환값 | `(Resp, []byte, error)` | Actor가 WAL 데이터를 반환. nil이면 read-only |
| `Replay` 메서드 | `Replay([]byte) error` | WAL replay와 외부 요청 처리를 명확히 분리 |
| Checkpoint LSN 저장 | engine이 snapshot 앞에 LSN 8 bytes prepend | 사용자 인터페이스(CheckpointStore) 변경 없이 구현 |
| actorOutput reply 시그니처 | `reply func(lsn uint64, err error)` | WALFlusher가 LSN을 mailbox goroutine에 피드백 |
| Checkpoint 일관성 | Drain-then-Snapshot | mailbox inCh 중단 + pendingWAL==0 대기 후 스냅샷. Actor 상태와 WAL 상태 정확히 일치. |
| Checkpoint 트리거 | 시간 기반(CheckpointScheduler) + WAL 누적 기반(mailbox 내부) | 둘 중 먼저 도달하는 조건으로 트리거. WAL 무한 증가 방지. |
| checkpointFn 주입 | ActorHost가 클로저로 mailbox에 주입 | Snapshot은 mailbox goroutine 내에서 호출해야 thread-safe. 저장 로직은 ActorHost가 소유. |
| actorOutput 타입 소거 | `reply func(lsn uint64, err error)` 클로저 사용 | WALFlusher를 제네릭으로 만들지 않아도 됨 |
| panic 처리 | `safeReceive`에서 recover → ErrActorPanicked | Actor 오류가 전체 PS를 다운시키지 않음 |
| WAL flush 실패 시 | 해당 batch의 reply(0, err) 호출 후 Actor evict | 이미 변경된 메모리 상태와 WAL 불일치 방지 |

---

## 알려진 한계

- **Checkpoint 중 지연**: checkpoint 신호가 오면 inCh가 일시 중단된다. 이 시간(drain 대기 + Snapshot + CheckpointStore 저장)만큼 해당 파티션 요청이 대기한다. 중단 시간은 CheckpointStore 저장 성능에 의존하며, 구현 후 측정이 필요하다. partition state가 매우 커질 경우 pause time이 길어질 수 있으며, 이 경우 incremental checkpoint(변경분만 저장 후 백그라운드 병합) 또는 copy-on-write 방식(drain 후 상태 복사본을 별도 goroutine에서 저장)을 검토한다.
- **WAL flush 실패 시 Actor evict**: 실패한 배치의 Actor를 evict하면 마지막 checkpoint 상태로 돌아간다. 해당 배치에서 처리된 write 연산은 유실된다. 더 정교한 재시도 처리는 구현 후 검토한다.
- **Group commit 성능 가설 미검증**: WALFlusher 배칭이 단일 스레드 IO 대비 처리량을 실질적으로 향상시키는지는 구현 후 벤치마크로 확인한다.
- **mailbox 버퍼 크기**: 기본값 미결정. 구현 단계에서 벤치마크로 결정한다.
- **CheckpointInterval 기본값**: 구현 단계에서 CheckpointStore 저장 성능과 WAL 누적 속도를 측정한 후 결정한다.
