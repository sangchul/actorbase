# sdk 패키지 설계

Go SDK. 사용자 애플리케이션이 import하여 actorbase 클러스터에 요청을 전송한다.
PS·PM과 달리 gRPC 서버를 갖지 않는 순수 클라이언트 라이브러리다.

의존성: `internal/transport`, `internal/domain`, `provider`

파일 목록:
- `config.go` — Config: SDK 설정 및 의존성 주입 구조체
- `client.go` — Client: 라우팅 조회 및 요청 전송

---

## config.go

```go
// Config는 Client 생성에 필요한 설정을 담는다.
type Config[Req, Resp any] struct {
    // ─── 필수 (사용자 제공) ───────────────────────────────────────

    PMAddr string         // PM gRPC 주소 ("host:port")
    Codec  provider.Codec // PS와 동일한 구현체를 주입해야 한다

    // ─── 선택 (기본값 있음) ───────────────────────────────────────

    ClientID      string        // 디버깅·로깅용 식별자. 기본값: hostname
    MaxRetries    int           // 재시도 최대 횟수. 기본값: 3
    RetryInterval time.Duration // 재시도 전 대기 시간. 기본값: 100ms
}
```

---

## client.go

```go
// Client는 actorbase 클러스터에 요청을 전송하는 SDK 진입점.
//
// 내부적으로:
//   - PM WatchRouting 스트림으로 라우팅 테이블을 수신·캐싱한다.
//   - key로 담당 파티션을 찾아 해당 PS에 요청을 전달한다.
//   - 라우팅 오류 시 테이블 갱신을 기다려 재시도한다.
type Client[Req, Resp any] struct {
    cfg      Config[Req, Resp]
    pmClient *transport.PMClient
    connPool *transport.ConnPool
    routing  atomic.Pointer[domain.RoutingTable]
}

func NewClient[Req, Resp any](cfg Config[Req, Resp]) (*Client[Req, Resp], error)

// Start는 PM WatchRouting 스트림을 시작하고 첫 라우팅 테이블을 수신할 때까지 대기한다.
// 이후 ctx 취소 시까지 백그라운드에서 라우팅 업데이트를 수신한다.
// 종료 시 모든 PS 연결을 닫는다.
func (c *Client[Req, Resp]) Start(ctx context.Context) error

// Send는 key를 담당하는 Actor에 req를 전달하고 Resp를 반환한다.
// 라우팅 오류(ErrPartitionMoved, ErrPartitionNotOwned, ErrPartitionBusy) 시
// 최대 MaxRetries까지 재시도한다.
func (c *Client[Req, Resp]) Send(ctx context.Context, key string, req Req) (Resp, error)
```

### 기동 순서 (Start)

```
Start(ctx):
  1. ch = pmClient.WatchRouting(ctx, clientID)
  2. 첫 라우팅 테이블 수신 대기
       select {
       case rt := <-ch: routing.Store(rt)
       case <-ctx.Done(): return ctx.Err()
       }
  3. go c.consumeRouting(ctx, ch)  // 이후 업데이트를 백그라운드에서 처리
  4. <-ctx.Done()
  5. connPool.Close()
  6. return nil
```

> 2번에서 첫 라우팅 테이블을 받기 전까지 Start가 블로킹된다. Start가 반환되면 Send를 즉시 호출해도 라우팅 테이블이 항상 준비된 상태임을 보장한다. PM이 연결 즉시 현재 테이블을 push하므로(transport.md 참조) 대기 시간은 RTT 수준이다.

### consumeRouting (내부)

```go
func (c *Client[Req, Resp]) consumeRouting(ctx context.Context, ch <-chan *domain.RoutingTable) {
    for rt := range ch {
        c.routing.Store(rt) // atomic swap. Send 측 잠금 불필요.
    }
}
```

### Send 처리 흐름

```
Send(ctx, key, req):
  for attempt := 0; attempt <= MaxRetries; attempt++:
    1. rt = c.routing.Load()
    2. entry, ok = rt.Lookup(key)
       └ ok == false → ErrPartitionNotOwned 즉시 반환
                        (키를 커버하는 파티션 없음 = 클러스터 설계 오류)
    3. conn = connPool.Get(entry.Node.Address)
    4. psClient = transport.NewPSClient(conn, codec)
    5. resp, err = psClient.Send(ctx, entry.Partition.ID, req)
    6. err == nil → return resp, nil
    7. switch err:
       ┌ ErrNotFound      → 즉시 반환 (재시도 불가. Actor가 키 없다고 응답.)
       ├ ErrTimeout       → 즉시 반환 (ctx 재사용 불가)
       ├ ErrActorPanicked → 즉시 반환 (Actor 내부 오류)
       ├ ErrPartitionMoved,
       │ ErrPartitionNotOwned → sleep(RetryInterval) 후 재시도
       │                         (라우팅 갱신 대기. routing.Store가 백그라운드에서 갱신 중.)
       └ ErrPartitionBusy → sleep(RetryInterval) 후 재시도
                             (split/migrate 완료 대기)
  MaxRetries 초과 → 마지막 err 반환
```

> **라우팅 갱신 대기 전략**: `ErrPartitionMoved` / `ErrPartitionNotOwned` 수신 시 PM이 이미 새 라우팅 테이블을 push 중이다. `consumeRouting`이 백그라운드에서 `routing`을 갱신하므로, 재시도 전 짧은 sleep 후 `routing.Load()`를 다시 호출하면 갱신된 테이블을 얻는다. 복잡한 동기화 없이 단순하고 실용적인 방식이다.

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| 라우팅 테이블 보관 방식 | `atomic.Pointer[domain.RoutingTable]` | 매 Send마다 호출되는 핫패스에서 잠금 없이 읽기 가능 |
| Start 블로킹 | 첫 라우팅 수신까지 대기 | Send 호출 시 라우팅이 반드시 준비되어 있음을 보장. nil 체크 불필요. |
| 재시도 대기 방식 | sleep(RetryInterval) | consumeRouting이 백그라운드에서 routing을 갱신. 복잡한 버전 동기화 불필요. |
| PSClient 재사용 | ConnPool에서 conn 재사용, PSClient는 Send마다 생성 | PSClient는 conn 래퍼로 생성 비용 무시 가능. ConnPool이 실제 TCP 커넥션을 캐싱. |
| 재시도 불가 에러 | ErrNotFound, ErrTimeout, ErrActorPanicked | 재시도해도 동일 결과가 예상되거나 (ErrNotFound, Panicked), ctx가 이미 만료 (ErrTimeout). |
| SDK는 etcd 불사용 | PM gRPC WatchRouting으로만 라우팅 수신 | SDK 사용자가 etcd 설정 불필요. etcd 커넥션 수 제한 (cluster.md 참조). |
| 제네릭 타입 파라미터 | `Client[Req, Resp any]` | 요청/응답 타입을 컴파일 타임에 강제. PS의 Actor[Req, Resp]와 대칭. |

---

## 알려진 한계

- **MaxRetries 초과 시 에러**: 재시도 횟수를 다 쓰면 마지막 에러를 반환한다. 클러스터가 장시간 불안정한 경우 호출자가 자체 retry 로직을 추가해야 한다.
- **RetryInterval 고정값**: 지수 백오프 없이 고정 interval로 재시도한다. 클러스터 불안정 시 retry storm이 발생할 수 있다. 구현 단계에서 지수 백오프 도입을 검토한다.
- **라우팅 갱신 타이밍**: `ErrPartitionMoved` 후 sleep하는 동안 PM이 아직 새 라우팅을 push하지 않았으면 재시도도 같은 오류가 반환된다. MaxRetries 내에 수렴하지 않으면 호출자에게 에러가 반환된다.
- **PM 연결 단절 시 Send 동작**: PM과의 WatchRouting 스트림이 끊기면 `PMClient` 내부에서 자동 재연결한다 (transport.md 참조). 재연결 중에도 기존 캐시된 라우팅으로 Send는 계속 동작하나, 라우팅 변경은 반영되지 않는다.
- **단일 PM 주소**: 현재 설계에서 SDK는 단일 PM 주소만 받는다. PM HA 구현 시 복수 주소나 DNS 기반 failover가 필요하다.
