# provider 패키지 설계

사용자와의 계약 패키지. 사용자가 구현해야 하는 인터페이스와 프레임워크가 제공하는 인터페이스를 정의한다.

파일 목록:
- `actor.go` — Actor, ActorFactory, Context
- `wal.go` — WALStore, WALEntry
- `checkpoint.go` — CheckpointStore
- `metrics.go` — Metrics, Counter, Gauge, Histogram
- `error.go` — 공개 에러 타입

---

## actor.go

```go
// =============================================
// 사용자가 구현해야 하는 인터페이스
// =============================================

// Actor는 하나의 파티션(key range)을 담당하는 비즈니스 로직 단위.
// 파티션당 하나의 인스턴스가 생성되며, 단일 스레드로 실행된다.
// Req는 이 Actor가 수신하는 요청 타입, Resp는 응답 타입이다.
// 사용자가 직접 구현한다.
type Actor[Req, Resp any] interface {
    Receive(ctx Context, req Req) (Resp, error)
    Snapshot() ([]byte, error)  // 체크포인트용 직렬화
    Restore([]byte) error        // 복구용 역직렬화
}

// ActorFactory는 파티션 ID마다 새 Actor 인스턴스를 생성하는 함수.
// 사용자가 직접 구현한다.
type ActorFactory[Req, Resp any] func(partitionID string) Actor[Req, Resp]

// =============================================
// 프레임워크가 제공하는 인터페이스 (사용만 함)
// =============================================

// Context는 Actor.Receive 호출 시 프레임워크가 주입하는 런타임 정보.
// 사용자는 구현하지 않고 사용만 한다.
type Context interface {
    PartitionID() string
    Logger() *slog.Logger  // 파티션 컨텍스트(partition-id, node-id 등)가 담긴 로거
}
```

### 사용 예시

```go
type BucketRequest struct {
    Op   string // "get" | "put" | "delete"
    Key  string
    Meta *ObjectMeta
}

type BucketResponse struct {
    Meta *ObjectMeta
}

type BucketActor struct {
    objects map[string]*ObjectMeta
}

func (a *BucketActor) Receive(ctx provider.Context, req BucketRequest) (BucketResponse, error) {
    switch req.Op {
    case "get":
        obj, ok := a.objects[req.Key]
        if !ok {
            return BucketResponse{}, provider.ErrNotFound
        }
        return BucketResponse{Meta: obj}, nil
    case "put":
        a.objects[req.Key] = req.Meta
        return BucketResponse{}, nil
    case "delete":
        delete(a.objects, req.Key)
        return BucketResponse{}, nil
    }
    return BucketResponse{}, fmt.Errorf("unknown op: %s", req.Op)
}
```

### 알려진 한계

- `Req`는 단일 타입이므로, Get/Put/Delete처럼 여러 종류의 연산을 표현하려면 사용자가 직접 discriminator 필드(예: `Op string`)나 내부 variant 패턴을 사용해야 한다. 컴파일 타임에 연산 종류를 강제하지는 않는다.

---

## wal.go

```go
// =============================================
// 사용자가 구현해야 하는 인터페이스
// =============================================

// WALEntry는 WAL의 단일 레코드.
type WALEntry struct {
    LSN  uint64 // Log Sequence Number. 단조 증가.
    Data []byte
}

// WALStore는 Write-Ahead Log 저장소의 추상화.
// append-only이며 LSN 기반으로 순차 조회한다.
// 사용자가 직접 구현한다. (Redis Stream, Kafka 등)
type WALStore interface {
    // Append는 파티션의 WAL에 엔트리를 추가하고 부여된 LSN을 반환한다.
    Append(ctx context.Context, partitionID string, data []byte) (lsn uint64, err error)

    // ReadFrom은 fromLSN 이후의 모든 WAL 엔트리를 순서대로 반환한다.
    // 복구 시 checkpoint LSN 이후부터 replay하는 데 사용한다.
    ReadFrom(ctx context.Context, partitionID string, fromLSN uint64) ([]WALEntry, error)

    // TrimBefore는 lsn 미만의 오래된 WAL 엔트리를 삭제한다.
    // checkpoint 완료 후 불필요한 엔트리를 정리하는 데 사용한다.
    TrimBefore(ctx context.Context, partitionID string, lsn uint64) error
}
```

### Redis Stream 구현 매핑

| WALStore 메서드 | Redis Stream 명령 |
|---|---|
| `Append` | `XADD` (Stream ID → LSN) |
| `ReadFrom` | `XRANGE {key} {fromLSN} +` |
| `TrimBefore` | `XTRIM {key} MINID {lsn}` |

### 알려진 한계

- `ReadFrom`이 `[]WALEntry`를 한 번에 반환하므로, WAL이 매우 길 경우 메모리 부담이 생길 수 있다. 향후 iterator나 channel 방식으로 교체를 검토한다.

---

## checkpoint.go

```go
// =============================================
// 사용자가 구현해야 하는 인터페이스
// =============================================

// CheckpointStore는 Actor 상태 스냅샷 저장소의 추상화.
// 파티션 ID를 키로 사용하며, 파티션당 하나의 스냅샷을 유지한다.
// 사용자가 직접 구현한다. (S3, RocksDB, 분산 파일시스템 등)
type CheckpointStore interface {
    Save(ctx context.Context, partitionID string, data []byte) error
    Load(ctx context.Context, partitionID string) ([]byte, error)
    Delete(ctx context.Context, partitionID string) error
}
```

---

## metrics.go

```go
// =============================================
// 사용자가 구현해야 하는 인터페이스
// =============================================

// Metrics는 메트릭 수집 백엔드의 추상화.
// 사용자가 직접 구현하거나, adapter/prometheus 기본 구현체를 사용한다.
type Metrics interface {
    Counter(name string, labels ...string) Counter
    Gauge(name string, labels ...string) Gauge
    Histogram(name string, labels ...string) Histogram
}

// Counter는 단조 증가하는 값. (요청 수, 에러 수 등)
type Counter interface {
    Inc(labelValues ...string)
    Add(n float64, labelValues ...string)
}

// Gauge는 증감 가능한 값. (현재 활성 Actor 수, 큐 길이 등)
type Gauge interface {
    Set(v float64, labelValues ...string)
    Inc(labelValues ...string)
    Dec(labelValues ...string)
}

// Histogram은 분포를 측정하는 값. (요청 latency, 메시지 크기 등)
type Histogram interface {
    Observe(v float64, labelValues ...string)
}
```

### 알려진 한계

- `labels`(이름)와 `labelValues`(값)의 매칭을 컴파일 타임에 검증하지 않는다. 순서가 맞지 않아도 컴파일 에러가 발생하지 않으므로, 구현 시 주의가 필요하다. 향후 타입 안전한 label 설계로 교체를 검토한다.

---

## error.go

```go
var (
    // ErrNotFound: Actor가 해당 키를 찾지 못함.
    // Actor.Receive 내부에서 반환하며, SDK가 호출자에게 전달한다.
    ErrNotFound = errors.New("not found")

    // ErrPartitionMoved: 파티션이 다른 노드로 이동됨.
    // 라우팅 테이블 자체가 outdated인 상태.
    // SDK가 PM에서 새 라우팅 테이블을 받아 재시도해야 한다.
    ErrPartitionMoved = errors.New("partition moved")

    // ErrPartitionNotOwned: 요청을 받은 PS가 해당 파티션을 담당하지 않음.
    // 라우팅 테이블의 일시적 불일치 상태.
    // SDK가 재시도해야 한다.
    ErrPartitionNotOwned = errors.New("partition not owned")

    // ErrPartitionBusy: 파티션 split/migration 진행 중.
    // 잠시 후 재시도해야 한다.
    ErrPartitionBusy = errors.New("partition busy")

    // ErrTimeout: 요청 처리 시간 초과.
    ErrTimeout = errors.New("timeout")

    // ErrActorPanicked: Actor.Receive 실행 중 panic 발생.
    // 프레임워크가 recover 후 이 에러를 반환한다.
    ErrActorPanicked = errors.New("actor panicked")
)
```

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| Actor 타입 파라미터 | 제네릭 `Actor[Req, Resp any]` | 요청/응답 타입을 컴파일 타임에 강제 |
| Receive 반환값 | `(Resp, error)` | Reply 구조체 불필요. 에러는 무조건 SDK에 전달. |
| WAL/Checkpoint 분리 | 별도 인터페이스 | 성격이 달라 백엔드 선택지가 다름 (Redis Stream vs S3 등) |
| WALStore.ReadFrom 반환 | `[]WALEntry` slice | 단순하게 시작. 대용량 WAL 시 iterator로 교체 검토. |
| Metrics label 방식 | 가변 인자 `string` | 단순하게 시작. 타입 안전성은 추후 보완. |
| 로깅 | `log/slog` 직접 사용 | Go 1.21+ 표준. Context에서 파티션 정보 포함 로거 제공. |
