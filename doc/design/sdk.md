# sdk 패키지 설계

Go SDK. 사용자 애플리케이션이 import하여 actorbase 클러스터에 요청을 전송한다.
PS·PM과 달리 gRPC 서버를 갖지 않는 순수 클라이언트 라이브러리다.

의존성: `internal/transport`, `internal/domain`, `provider`

파일 목록:
- `config.go` — Config: SDK 설정 및 의존성 주입 구조체
- `client.go` — Client: 라우팅 조회 및 요청 전송

---

## config.go

| 필드 | 설명 |
|---|---|
| PMAddr | PM gRPC 주소 (필수) |
| TypeID | 이 Client가 대상으로 하는 actor type 식별자. 예: "kv", "bucket" (필수) |
| Codec | PS와 동일한 구현체를 주입해야 한다 (필수) |
| ClientID | 디버깅·로깅용 식별자. 기본값: hostname |
| MaxRetries | 재시도 최대 횟수. 기본값: 3 |
| RetryInterval | 재시도 전 대기 시간. 기본값: 100ms |

---

## client.go

### Start 흐름

```
Start(ctx):
  1. ch = pmClient.WatchRouting(ctx, clientID)
  2. 첫 라우팅 테이블 수신 대기 → routing.Store(rt)
  3. go consumeRouting(ctx, ch)  // 이후 갱신 + ctx 취소 시 connPool.Close
  4. return nil  // 첫 RT 수신 후 즉시 반환. Send 바로 사용 가능.
```

> 이전 설계에서 `Start()`가 `<-ctx.Done()`까지 블로킹했다가 race condition이 발견됐다. goroutine에서 Start를 실행하면 첫 RT 저장 전에 Send가 호출될 수 있었다. 현재 구현에서는 Start 반환 후 Send를 goroutine 없이 바로 호출해도 안전하다.

### Send 흐름

```
Send(ctx, key, req):
  for attempt := 0; attempt <= MaxRetries; attempt++:
    1. rt = routing.Load()
    2. entry, ok = rt.Lookup(TypeID, key)
       └ ok == false → ErrPartitionNotOwned 즉시 반환
    3. conn = connPool.Get(entry.Node.Address)
    4. psClient.Send(ctx, TypeID, entry.Partition.ID, req, &resp)
    5. err == nil → return resp
    6. switch err:
       ┌ ErrNotFound, ErrTimeout, ErrActorPanicked → 즉시 반환
       └ ErrPartitionMoved, ErrPartitionNotOwned,
         ErrPartitionBusy → sleep(RetryInterval) 후 재시도
  MaxRetries 초과 → 마지막 err 반환
```

> **라우팅 갱신 대기**: ErrPartitionMoved 수신 시 PM이 이미 새 라우팅을 push 중이다. `consumeRouting`이 백그라운드에서 `routing`을 갱신하므로, 짧은 sleep 후 재시도하면 갱신된 테이블을 얻는다.

### Scan 흐름

다수의 파티션에 걸친 범위 조회. **누락 없음(no missing keys)** 을 보장한다. 중복은 허용된다.

```
Scan(ctx, startKey, endKey, req):
  currentStart = startKey
  results = []Resp{}

  for currentStart < endKey (또는 endKey == ""):
    1. rt = routing.Load()
    2. partitions = rt.PartitionsInRange(TypeID, currentStart, endKey)
       └ 없음 → break (완료)
    3. 파티션을 KeyRange.Start 기준으로 정렬
    4. for each partition in sorted order (병렬 fan-out):
         psClient.Scan(ctx, TypeID, partitionID, req, &resp,
                       partition.KeyRange.Start, partition.KeyRange.End)

    5. 연속 성공(currentStart부터) 결과 누적:
         for each result (KeyRange.Start 순서):
           if result.err == nil && result.KeyRange.Start == currentStart:
             results = append(results, result.resp)
             currentStart = result.KeyRange.End  // 다음 파티션으로 전진
           else:
             break  // 첫 번째 실패 또는 gap → 여기서부터 재시도

    6. currentStart가 endKey 이상이면 완료

  return results, nil
```

**누락 없음 보장 원리**: stale routing으로 P1=[a,m)을 예상했는데 실제 P1=[a,g)로 split된 경우, PS는 `ErrPartitionMoved`를 반환한다. SDK는 라우팅을 갱신하고 `currentStart=a`부터 재시도하여 P1a=[a,g)와 P1b=[g,m)을 모두 커버한다. 이미 성공한 `[endKey' , currentStart)` 범위는 재시도하지 않는다.

**`[]Resp` 반환**: 파티션별 응답을 하나씩 담은 슬라이스. 호출자가 `Resp` 안의 `Items []T`를 순서대로 이어 붙이면 전체 결과 목록이 된다. SDK는 데이터 내용을 모르므로 중복 제거나 정렬은 호출자가 담당한다.

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| TypeID 필드 | Config.TypeID string | 하나의 Client 인스턴스는 단일 actor type만 대상. rt.Lookup(TypeID, key) 호출 시 사용. |
| 라우팅 테이블 보관 | atomic.Pointer[domain.RoutingTable] | 핫패스에서 잠금 없이 읽기 가능 |
| Start 블로킹 | 첫 라우팅 수신까지 대기 | Send 호출 시 라우팅이 반드시 준비됨을 보장. nil 체크 불필요. |
| 재시도 대기 방식 | sleep(RetryInterval) | consumeRouting이 백그라운드에서 갱신. 복잡한 버전 동기화 불필요. |
| PSClient 재사용 | ConnPool에서 conn 재사용 | ConnPool이 TCP 커넥션 캐싱. PSClient는 conn 래퍼로 생성 비용 무시 가능. |
| SDK는 etcd 불사용 | PM gRPC WatchRouting으로만 라우팅 수신 | SDK 사용자가 etcd 설정 불필요. |

---

## 알려진 한계

- **MaxRetries 초과 시 에러**: 클러스터가 장시간 불안정한 경우 호출자가 자체 retry 로직을 추가해야 한다.
- **RetryInterval 고정값**: 지수 백오프 없이 고정 interval로 재시도. retry storm 발생 가능.
- **PM 연결 단절 시**: 재연결 중에도 기존 캐시된 라우팅으로 Send는 계속 동작하나, 라우팅 변경은 반영되지 않는다.
- **Scan 중복 허용**: 파티션 split 타이밍에 따라 동일 key가 두 번 포함될 수 있다. 중복 제거는 호출자가 처리해야 한다.
- **Scan 부분 실패**: 특정 파티션의 MaxRetries 초과 시 해당 파티션부터 이후 범위가 누락된 채 에러를 반환한다.
