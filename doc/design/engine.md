# internal/engine 패키지 설계

Actor 실행 엔진. Partition Server(PS)가 사용한다.
`provider`, `internal/domain` 패키지만 의존한다.

파일 목록:
- `host.go`      — ActorHost: Actor 생명주기 (활성화/eviction)
- `mailbox.go`   — mailbox: Actor당 단일 스레드 실행 보장
- `flusher.go`   — WALFlusher: WAL group commit
- `scheduler.go` — EvictionScheduler: idle Actor eviction

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
  성공 → 각 reply(nil)
  실패 → 각 reply(err)
```

Actor goroutine은 WAL IO를 기다리지 않는다. WALFlusher가 여러 Actor의 WAL entry를 모아 배치로 처리하므로 IO 횟수가 줄어든다. 응답 지연은 flushInterval만큼 증가하지만, 단일 스레드 IO 대비 처리량이 크게 향상될 것으로 예상한다.

> **검증 필요:** 이 가설은 구현 후 벤치마크로 확인한다.

---

## provider/actor.go 인터페이스 변경

engine 설계에 따라 provider/actor.go의 `Actor` 인터페이스가 변경된다.

```go
type Actor[Req, Resp any] interface {
    // Receive는 요청을 처리하고 응답과 WAL 데이터를 반환한다.
    // walEntry가 nil이면 read-only 연산이며 WAL에 기록하지 않는다.
    // walEntry가 nil이 아니면 engine이 WALStore에 기록한 뒤 응답을 반환한다.
    Receive(ctx Context, req Req) (resp Resp, walEntry []byte, err error)

    // Replay는 WAL entry 하나를 Actor 상태에 적용한다.
    // 복구 시 마지막 checkpoint 이후의 WAL entries를 순서대로 적용하는 데 사용한다.
    // Receive와 달리 외부에 응답을 반환하지 않는다.
    Replay(entry []byte) error

    // Snapshot은 현재 Actor 상태를 직렬화하여 반환한다. (체크포인트용)
    Snapshot() ([]byte, error)

    // Restore는 Snapshot 데이터로 Actor 상태를 복원한다. (체크포인트 로드)
    Restore(data []byte) error
}
```

### Actor 사용 예시 (변경 후)

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
        entry := marshalPutOp(req)  // 사용자가 정의한 직렬화
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
    Factory         provider.ActorFactory[Req, Resp]
    CheckpointStore provider.CheckpointStore
    WALStore        provider.WALStore
    MailboxSize     int           // mailbox 버퍼 크기. 기본값은 구현 시 결정.
    FlushSize       int           // WAL 배치 최대 크기
    FlushInterval   time.Duration // WAL 배치 최대 대기 시간
    Metrics         provider.Metrics
}

// ActorHost는 파티션 서버 내 모든 Actor의 생명주기를 관리한다.
// - Send: 요청을 Actor에 전달. Actor가 비활성이면 먼저 활성화.
// - Evict: 특정 Actor를 메모리에서 제거. 제거 전 checkpoint 저장.
type ActorHost[Req, Resp any] struct {
    cfg     Config[Req, Resp]
    actors  map[string]*actorEntry[Req, Resp] // partitionID → actorEntry
    mu      sync.Mutex
    flusher *WALFlusher
}

type actorEntry[Req, Resp any] struct {
    actor   provider.Actor[Req, Resp]
    mailbox *mailbox[Req, Resp]
    lastMsg atomic.Value // time.Time; WALFlusher reply 후 갱신
}

func NewActorHost[Req, Resp any](cfg Config[Req, Resp]) *ActorHost[Req, Resp]

// Send는 partitionID의 Actor에 req를 전달하고 응답을 기다린다.
// Actor가 비활성이면 activate를 먼저 호출한다.
// O(1) map 조회 후 mailbox로 디스패치.
func (h *ActorHost[Req, Resp]) Send(ctx context.Context, partitionID string, req Req) (Resp, error)

// Evict는 partitionID의 Actor를 메모리에서 제거한다.
// 제거 전 snapshot을 찍고 CheckpointStore에 저장한다.
// WALStore에서 checkpoint LSN 이전 항목을 trim한다.
func (h *ActorHost[Req, Resp]) Evict(ctx context.Context, partitionID string) error

// EvictAll은 모든 활성 Actor를 evict한다. 서버 종료 시 호출한다.
func (h *ActorHost[Req, Resp]) EvictAll(ctx context.Context) error

// IdleActors는 lastMsg가 idleSince 이전인 partitionID 목록을 반환한다.
// EvictionScheduler가 주기적으로 호출한다.
func (h *ActorHost[Req, Resp]) IdleActors(idleSince time.Time) []string
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
    reply       func(error) // WAL flush 결과를 caller에게 전달하는 클로저
}

// mailbox는 하나의 Actor에 대한 메시지 큐.
// 단일 goroutine(run)이 메시지를 순차 처리하여 Actor의 단일 스레드 실행을 보장한다.
type mailbox[Req, Resp any] struct {
    inCh    chan envelope[Req, Resp]
    outCh   chan<- actorOutput[Resp] // WALFlusher의 수신 채널
    cancel  context.CancelFunc
}

func newMailbox[Req, Resp any](
    inSize int,
    outCh  chan<- actorOutput[Resp],
) *mailbox[Req, Resp]

// send는 req를 inCh에 넣고 result를 기다린다.
// ctx 만료 시 즉시 provider.ErrTimeout 반환.
func (m *mailbox[Req, Resp]) send(ctx context.Context, req Req) (Resp, error)

// close는 mailbox goroutine을 종료한다.
func (m *mailbox[Req, Resp]) close()
```

### mailbox goroutine (run) 처리 흐름

```go
func (m *mailbox[Req, Resp]) run(actor provider.Actor[Req, Resp], partitionID string) {
    for env := range m.inCh {
        resp, walEntry, err := safeReceive(actor, env)  // panic recover 포함

        if walEntry == nil || err != nil {
            // read-only 또는 에러: 즉시 reply
            env.replyCh <- result[Resp]{resp: resp, err: err}
            continue
        }

        // write: WALFlusher에 위임. Actor goroutine은 블로킹 없이 계속 진행.
        reply := func(flushErr error) {
            if flushErr != nil {
                env.replyCh <- result[Resp]{err: flushErr}
            } else {
                env.replyCh <- result[Resp]{resp: resp}
            }
        }
        m.outCh <- actorOutput[Resp]{
            partitionID: partitionID,
            walEntry:    walEntry,
            reply:       reply,
        }
    }
}
```

> `safeReceive`는 `Actor.Receive` 호출을 `recover`로 감싸 panic 발생 시 `provider.ErrActorPanicked`를 반환한다.

---

## flusher.go

```go
// walPending은 WAL flush 대기 중인 항목 하나.
type walPending struct {
    partitionID string
    entry       []byte
    reply       func(error)
}

// WALFlusher는 여러 Actor의 WAL entry를 모아 배치로 WALStore에 기록한다.
// - submitCh로 walPending을 수신한다.
// - flushSize 또는 flushInterval 기준으로 배치를 flush한다.
// - flush 성공 시 각 reply(nil), 실패 시 각 reply(err) 호출.
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
  3. 각 partitionID별 WALStore.Append 호출
  4. 전체 성공 → 모든 reply(nil) 호출
     부분 실패 → 실패한 partitionID의 reply(err) 호출
                  성공한 partitionID의 reply(nil) 호출
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
```

### eviction 루프 동작

```
interval마다:
  1. host.IdleActors(now - idleTimeout) 호출
  2. 각 partitionID에 대해 host.Evict 호출
     (Evict 내부에서 Snapshot 저장 + WAL trim)
```

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| WAL 기록 방식 | Group commit (WALFlusher 배치) | Actor 단일 스레드에서 IO 제거 → 처리량 향상 |
| `Receive` 반환값 | `(Resp, []byte, error)` | Actor가 WAL 데이터를 반환. nil이면 read-only |
| `Replay` 메서드 추가 | `Replay([]byte) error` | WAL replay와 외부 요청 처리를 명확히 분리 |
| Checkpoint LSN 저장 | engine이 snapshot 앞에 LSN 8 bytes prepend | 사용자 인터페이스(CheckpointStore) 변경 없이 구현 |
| actorOutput 타입 소거 | `reply func(error)` 클로저 사용 | WALFlusher를 제네릭으로 만들지 않아도 됨 |
| panic 처리 | `safeReceive`에서 recover → ErrActorPanicked | Actor 오류가 전체 PS를 다운시키지 않음 |
| WAL flush 실패 시 | 해당 batch의 reply(err) 호출 후 Actor evict | 이미 변경된 메모리 상태와 WAL 불일치 방지 |

---

## 알려진 한계

- **WAL flush 실패 시 Actor evict**: 실패한 배치의 Actor를 evict하면 마지막 checkpoint 상태로 돌아간다. 해당 배치에서 처리된 write 연산은 유실된다. flush 실패를 재시도하거나 더 정교하게 처리하는 방안은 구현 후 검토한다.
- **Group commit 성능 가설 미검증**: WALFlusher 배칭이 단일 스레드 IO 대비 처리량을 실질적으로 향상시키는지는 구현 후 벤치마크로 확인한다.
- **mailbox 버퍼 크기**: 기본값 미결정. 너무 작으면 Actor goroutine 대기 증가, 너무 크면 메모리 낭비. 구현 단계에서 벤치마크로 결정한다.
