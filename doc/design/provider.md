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
    // engine이 split 실행 시 호출한다.
    Split(splitKey string) (upperHalf []byte, err error)
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

func (a *BucketActor) Receive(ctx provider.Context, req BucketRequest) (BucketResponse, []byte, error) {
    switch req.Op {
    case "get":
        obj, ok := a.objects[req.Key]
        if !ok {
            return BucketResponse{}, nil, provider.ErrNotFound
        }
        return BucketResponse{Meta: obj}, nil, nil // read: walEntry = nil
    case "put":
        a.objects[req.Key] = req.Meta
        entry := marshalPutOp(req)
        return BucketResponse{}, entry, nil        // write: walEntry 반환
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

### 선택적 인터페이스

```go
// Countable은 Actor가 선택적으로 구현하는 인터페이스.
// 구현하면 PM의 Auto Balancer가 key count를 split 판단에 활용한다.
// 구현하지 않으면 `abctl stats`의 KEY-COUNT 컬럼이 "n/a"로 표시된다.
type Countable interface {
    KeyCount() int64
}

// SplitHinter는 Actor가 선택적으로 구현하는 인터페이스.
// 구현하면 Actor가 내부 상태(hotspot 추적 등)를 기반으로 split 위치를 직접 제안한다.
// 구현하지 않으면 engine이 파티션 key range의 midpoint를 자동으로 계산한다.
//
// split key 결정 우선순위:
//   1. 호출자가 명시한 key (abctl split <id> <key> 등 수동 명령)
//   2. SplitHint() 반환값 (구현한 경우)
//   3. KeyRangeMidpoint(keyRangeStart, keyRangeEnd) (midpoint fallback)
//
// SplitHint()는 mailbox goroutine 내에서 호출되므로 별도 동기화가 불필요하다.
// 반환값이 ""이면 다음 우선순위 fallback으로 넘어간다.
type SplitHinter interface {
    SplitHint() string
}
```

**SplitHinter 구현 예시 (object_actor — hotspot 기반 split):**

```go
// objectActor는 routing key별 누적 접근 수를 추적하여
// 가장 자주 접근되는 key를 split 위치로 제안한다.
// split하면 hotspot key 이상이 상위 파티션으로 이동하여 부하가 분산된다.
type objectActor struct {
    objects  map[string]objectMeta
    accessCt map[string]int64  // routing key별 누적 접근 수
}

func (a *objectActor) Receive(_ provider.Context, req ObjectRequest) (ObjectResponse, []byte, error) {
    k := objKey(req.Bucket, req.Key)
    a.accessCt[k]++ // mailbox goroutine 내 단일 스레드 → 동기화 불필요
    // ...
}

func (a *objectActor) SplitHint() string {
    var hotKey string
    var maxCt int64
    for k, ct := range a.accessCt {
        if ct > maxCt {
            maxCt = ct
            hotKey = k
        }
    }
    return hotKey // ""이면 engine이 midpoint 사용
}
```

bucket과 같이 접근 패턴이 고른 Actor는 `SplitHinter`를 구현하지 않아도 된다. engine이 파티션 key range의 midpoint로 split key를 결정한다.

### WAL 기록과 멱등성 요구사항

**Actor의 쓰기 연산은 멱등(idempotent)하게 설계해야 한다.**

WAL에 기록된 연산이 두 번 적용되는 상황이 발생할 수 있기 때문이다.

**double-apply가 발생하는 경로:**

```
1. Actor.Receive 처리 → walEntry 반환
2. engine이 WALStore에 성공적으로 기록
3. 네트워크 장애로 응답이 클라이언트에 도달하지 않음
4. 클라이언트: 에러로 간주하고 재시도
5. engine: 에러 전파 → actor evict
6. actor 재활성화: WAL replay → 2에서 기록한 walEntry 적용 (1회)
7. 클라이언트 재시도 처리 → 동일 연산 다시 적용 (2회)
```

WAL replay는 정상 경로이고, 클라이언트 재시도도 정상 경로다. 두 경로가 독립적으로 실행되면서 같은 연산이 두 번 적용된다.

**멱등 연산 예시 (안전):**
- `set(key, value)` — 같은 값을 두 번 써도 결과 동일
- `delete(key)` — 없는 키를 삭제해도 결과 동일
- `put_if_absent(key, value)` — 이미 있으면 무시

**비멱등 연산 예시 (위험):**
- `increment(key)` — 두 번 적용되면 2 증가
- `append(key, value)` — 두 번 적용되면 두 번 추가

**비멱등 연산이 필요한 경우:**
클라이언트가 요청에 고유한 sequence number 또는 idempotency key를 포함하고, Actor가 처리 전 중복 여부를 확인하는 방식으로 구현한다. deduplication 상태 자체도 WAL에 기록해야 한다.

> 이 제약은 WALStore 구현 방식과 무관하다. Lua 스크립트 등으로 WAL 쓰기를 원자적으로 만들어도, "WAL 쓰기 성공 + 응답 유실" 시나리오는 동일하게 발생한다.

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
    // AppendBatch는 파티션의 WAL에 여러 엔트리를 원자적으로 추가하고
    // 각 엔트리에 부여된 LSN을 순서대로 반환한다.
    // 에러 시 전체 실패(부분 성공 없음).
    AppendBatch(ctx context.Context, partitionID string, data [][]byte) (lsns []uint64, err error)

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
| `AppendBatch` | `XADD` × N (Pipeline) |
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

## balance.go

사용자가 구현하는 분배 정책 인터페이스와 관련 타입을 정의한다.
PM이 이 인터페이스를 통해 split/migrate/failover 판단을 위임한다.

```go
// =============================================
// 사용자가 구현해야 하는 인터페이스
// =============================================

// BalancePolicy는 클러스터 분배 정책의 추상화.
// PM이 주기적으로 Evaluate를 호출하고, 노드 이벤트 시 OnNodeJoined / OnNodeLeft를 호출한다.
// 반환된 BalanceAction 목록을 PM이 순서대로 실행한다.
type BalancePolicy interface {
    // Evaluate는 check_interval마다 호출된다. 현재 클러스터 상태를 보고 액션을 반환한다.
    Evaluate(ctx context.Context, stats ClusterStats) []BalanceAction

    // OnNodeJoined는 새 노드가 클러스터에 합류할 때 호출된다.
    OnNodeJoined(ctx context.Context, node NodeInfo, stats ClusterStats) []BalanceAction

    // OnNodeLeft는 노드가 클러스터에서 이탈할 때 호출된다. 정상 이탈과 장애 모두 포함.
    // 정상 이탈(graceful): drainPartitions가 이미 파티션을 이전했으므로, 보통 no-op.
    // 장애(failure): 해당 노드의 파티션에 ActionFailover를 반환해야 한다.
    OnNodeLeft(ctx context.Context, node NodeInfo, reason NodeLeaveReason, stats ClusterStats) []BalanceAction
}

// =============================================
// 관련 타입
// =============================================

type NodeStatus string
const (
    NodeStatusActive   NodeStatus = "active"
    NodeStatusDraining NodeStatus = "draining"
)

type NodeInfo struct {
    ID     string
    Addr   string
    Status NodeStatus
}

// NodeLeaveReason은 노드 이탈 원인.
// etcd watch로는 정상/장애를 구분할 수 없으므로 항상 NodeLeaveFailure로 전달된다.
// 정상 이탈 감지는 OnNodeLeft 시점에 라우팅 테이블에서 파티션이 없음을 확인하는 방식으로 한다.
type NodeLeaveReason string
const (
    NodeLeaveGraceful NodeLeaveReason = "graceful"
    NodeLeaveFailure  NodeLeaveReason = "failure"
)

type PartitionInfo struct {
    PartitionID   string
    ActorType     string
    KeyRangeStart string
    KeyRangeEnd   string
    KeyCount      int64   // -1이면 n/a (Countable 미구현)
    RPS           float64
}

type NodeStats struct {
    Node       NodeInfo
    Reachable  bool     // false이면 GetStats 실패 (장애 노드)
    NodeRPS    float64
    Partitions []PartitionInfo
}

type ClusterStats struct {
    Nodes []NodeStats
}

type BalanceActionType string
const (
    ActionSplit    BalanceActionType = "split"
    ActionMigrate  BalanceActionType = "migrate"
    ActionFailover BalanceActionType = "failover"  // source PS가 죽었을 때 사용
)

type BalanceAction struct {
    Type        BalanceActionType
    ActorType   string
    PartitionID string
    TargetNode  string  // ActionMigrate / ActionFailover 시 사용
}
// ActionSplit 시 split key는 Policy가 결정하지 않는다.
// PS가 SplitHinter 또는 midpoint fallback으로 결정한다.
```

### 기본 제공 구현체

`pm/policy` 패키지에서 두 가지 구현체를 제공한다.

| 구현체 | 설명 |
|---|---|
| `NoopBalancePolicy` | 모든 메서드가 nil 반환. `pm.Config.BalancePolicy`의 기본값. |
| `ThresholdPolicy` | RPS·key count 절대 임계값 기반 정책. `algorithm: threshold`. |
| `RelativePolicy` | 클러스터 평균 RPS 대비 배수 기반 정책. `algorithm: relative`. |

사용자는 `provider.BalancePolicy`를 직접 구현하여 `pm.Config{BalancePolicy: myPolicy}`로 주입할 수 있다.

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
| Receive 반환값 | `(Resp, []byte, error)` | walEntry nil이면 read-only. engine이 WAL 기록 후 응답 전달. |
| Replay 메서드 | `Replay([]byte) error` 추가 | WAL replay와 외부 요청 처리를 명확히 분리. |
| Split 메서드 | `Split(splitKey string) ([]byte, error)` 추가 | Actor가 자신의 상태를 직접 분리. engine이 split 실행 시 호출. |
| WAL/Checkpoint 분리 | 별도 인터페이스 | 성격이 달라 백엔드 선택지가 다름 (Redis Stream vs S3 등) |
| WALStore.ReadFrom 반환 | `[]WALEntry` slice | 단순하게 시작. 대용량 WAL 시 iterator로 교체 검토. |
| Metrics label 방식 | 가변 인자 `string` | 단순하게 시작. 타입 안전성은 추후 보완. |
| 로깅 | `log/slog` 직접 사용 | Go 1.21+ 표준. Context에서 파티션 정보 포함 로거 제공. |
