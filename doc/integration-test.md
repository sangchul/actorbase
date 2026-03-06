# actorbase 통합 시나리오 테스트

actorbase의 핵심 동작을 실제 프로세스를 기동하여 검증하는 통합 테스트 절차다.
각 시나리오는 독립적으로 실행할 수 있으며, 이전 시나리오의 상태를 이어받는 경우 명시한다.

---

## 전체 구성도

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              클라이언트                                        │
│                                                                             │
│   ┌─────────────────────────┐          ┌──────────────────────────┐         │
│   │   SDK Client            │          │   abctl                  │         │
│   │  (kv_basic / kv_stress) │          │  routing / split /       │         │
│   └────────────┬────────────┘          │  migrate                 │         │
│                │                       └───────────┬──────────────┘         │
└────────────────┼───────────────────────────────────┼───────────────────────┘
                 │                                   │
         WatchRouting                        gRPC (control)
         (gRPC stream)                               │
                 │                                   │
┌────────────────┼───────────────────────────────────┼───────────────────────┐
│                │       Control Plane               │                       │
│                │                                   │                       │
│                │       ┌───────────────────────────▼──────────────────┐    │
│                └──────▶│   PM  (Partition Manager)   :8000            │    │
│                        │                                              │    │
│                        │  bootstrap / routing push / split / migrate  │    │
│                        └──────────┬──────────────┬────────────────────┘    │
│                                   │              │                         │
│                            라우팅 테이블       NodeJoined /                   │
│                            저장 / 조회         NodeLeft 이벤트                 │
│                                   │              │                         │
│                        ┌──────────▼──────────────▼────────────────────┐    │
│                        │   etcd   :2379                               │    │
│                        │  /actorbase/routing   /actorbase/nodes/      │    │
│                        └──────────┬──────────────┬────────────────────┘    │
│                                   │              │                         │
│                         노드 등록 (lease)      노드 등록 (lease)               │
└───────────────────────────────────┼──────────────┼───────────────────────-─┘
                                    │              │
┌───────────────────────────────────┼──────────────┼────────────────────────┐
│                                   │  Data Plane  │                        │
│         ┌─────────────────────────▼──┐       ┌───▼───────────────────────┐│
│         │  kv_server (PS-1)  :8001   │       │  kv_server (PS-2)  :8002  ││
│         │                            │       │                           ││
│         │  ┌──────────────────────┐  │  ◀──▶ │  ┌──────────────────────┐ ││
│         │  │  ActorHost           │  │migrate│  │  ActorHost           │ ││
│         │  │  ┌────────────────┐  │  │(ctrl) │  │  ┌────────────────┐  │ ││
│         │  │  │ kvActor [0, m) │  │  │       │  │  │ kvActor [m, ∞) │  │ ││
│         │  │  └────────────────┘  │  │       │  │  └────────────────┘  │ ││
│         │  │  WALFlusher          │  │       │  │  WALFlusher          │ ││
│         │  └──────────┬───────────┘  │       │  └──────────┬───────────┘ ││
│         └─────────────┼──────────────┘       └─────────────┼─────────────┘│
│                       │                                    │              │
│            ┌──────────▼──────────┐             ┌───────────▼─────────┐    │
│            │  WAL Store          │             │  WAL Store          │    │
│            │  (node-local)       │             │  (node-local)       │    │
│            │  /tmp/actorbase-ps1 │             │  /tmp/actorbase-ps2 │    │
│            │  /wal               │             │  /wal               │    │
│            └─────────────────────┘             └─────────────────────┘    │
│                                                                           │
│            ┌──────────────────────────────────────────────────────────┐   │
│            │  Checkpoint Store  (shared)                              │   │
│            │  /tmp/actorbase-checkpoint                               │   │
│            │                                                          │   │
│            │  PS-1 ──write──▶  {partition-id}.snapshot  ◀──read── PS-2│   │
│            └──────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────────┘
```

### 통신 흐름 요약

| 화살표 | 프로토콜 | 설명 |
|---|---|---|
| SDK → PM | gRPC stream | `WatchRouting`: 라우팅 테이블 변경을 실시간으로 수신 |
| SDK → PS | gRPC | `Send`: key로 담당 파티션 Actor에 요청 전달 |
| abctl → PM | gRPC | `routing` / `split` / `migrate` 명령 |
| PM ↔ etcd | etcd gRPC | 라우팅 테이블 저장·조회, 노드 등록 이벤트 감시 |
| PS ↔ etcd | etcd gRPC | 노드 등록 (lease 기반 TTL), 라우팅 테이블 구독 |
| PM → PS | gRPC | `ExecuteSplit` / `ExecuteMigrateOut` / `PreparePartition` |
| PS → WAL Store | 파일 I/O | WAL entry 추가·재생 (노드별 로컬 디렉토리) |
| PS ↔ Checkpoint Store | 파일 I/O | Snapshot 저장·복원 (**공유 디렉토리**, migrate 시 핵심) |

---

## 사전 준비

### 환경 요구사항

| 도구 | 버전 | 설치 방법 |
|---|---|---|
| Go | 1.25+ | https://go.dev/dl |
| etcd | v3.6+ | `brew install etcd` / https://etcd.io |

### 바이너리 빌드

```bash
cd actorbase
go build -o bin/kv_server ./examples/kv_server
go build -o bin/pm        ./cmd/pm
go build -o bin/abctl     ./cmd/abctl
```

### 환경 변수 (공통)

```bash
export ETCD_ADDR="localhost:2379"
export PM_ADDR="localhost:8000"
export PS1_ADDR="localhost:8001"
export PS2_ADDR="localhost:8002"
export DATA_DIR_1="/tmp/actorbase-ps1"
export DATA_DIR_2="/tmp/actorbase-ps2"
export CHECKPOINT_DIR="/tmp/actorbase-checkpoint"  # 두 PS가 공유하는 checkpoint 디렉토리
```

### 클린 상태 초기화

각 시나리오 시작 전 실행한다.

```bash
# etcd 데이터 초기화
etcdctl del --prefix /actorbase

# PS 데이터 디렉토리 초기화
rm -rf $DATA_DIR_1 $DATA_DIR_2 $CHECKPOINT_DIR
```

---

## 시나리오 1: 클러스터 부트스트랩

### 목표

PM → PS 순서로 기동했을 때, PM이 첫 번째 PS 등록을 감지하고 전체 키 범위를 커버하는
초기 라우팅 테이블을 자동으로 생성하는지 확인한다.

### 절차

**터미널 1: etcd 기동**

```bash
etcd
```

**터미널 2: PM 기동**

```bash
./bin/pm -addr :8000 -etcd $ETCD_ADDR
```

예상 로그:

```
starting PM addr=:8000
```

**터미널 3: PS-1 기동**

```bash
./bin/kv_server \
  -node-id ps-1 \
  -addr $PS1_ADDR \
  -etcd $ETCD_ADDR \
  -data-dir $DATA_DIR_1 \
  -checkpoint-dir $CHECKPOINT_DIR
```

예상 로그:

```
starting PS node-id=ps-1 addr=localhost:8001
```

PM 로그에서 bootstrap 완료 메시지 확인:

```
pm bootstrap: initial routing table created partitionID=<uuid> node=ps-1
```

**터미널 4: 라우팅 테이블 확인**

```bash
./bin/abctl -pm $PM_ADDR routing
```

예상 출력:

```
Version: 1

PARTITION-ID                          KEY-START         KEY-END           NODE-ID                               NODE-ADDR
------------------------------------  ----------------  ----------------  ------------------------------------  -----------
<uuid>                                (start)           (end)             ps-1                                  localhost:8001
```

### 검증 포인트

- [ ] PM 기동 시 라우팅 테이블 없음 (`currentRT == nil`) 상태에서 bootstrap 고루틴이 시작된다.
- [ ] PS-1 등록 후 PM이 `NodeJoined` 이벤트를 수신하고 초기 테이블을 생성한다.
- [ ] `abctl routing`으로 Version 1, 전체 범위 `["", "")` 파티션을 확인한다.

---

## 시나리오 2: 기본 KV 동작

### 목표

SDK를 통해 Actor에 `set` / `get` / `del` 요청을 전송하고 정상 응답을 확인한다.

**시나리오 1 상태를 이어받는다.**

### 테스트 프로그램

`examples/kv_basic/main.go`로 저장 후 실행한다.

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    adapterjson "github.com/oomymy/actorbase/adapter/json"
    "github.com/oomymy/actorbase/sdk"
)

// cmd/ps의 KV Actor와 동일한 요청/응답 구조
type KVRequest struct {
    Op    string `json:"op"`
    Key   string `json:"key"`
    Value []byte `json:"value"`
}

type KVResponse struct {
    Value []byte `json:"value"`
    Found bool   `json:"found"`
}

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    client, err := sdk.NewClient(sdk.Config[KVRequest, KVResponse]{
        PMAddr: "localhost:8000",
        Codec:  adapterjson.New(),
    })
    if err != nil {
        log.Fatal(err)
    }

    // Start를 별도 고루틴에서 실행 (ctx 취소 시 종료)
    errCh := make(chan error, 1)
    go func() { errCh <- client.Start(ctx) }()

    // set
    _, err = client.Send(ctx, "user:1001", KVRequest{Op: "set", Key: "user:1001", Value: []byte(`{"name":"alice"}`)})
    if err != nil {
        log.Fatalf("set failed: %v", err)
    }
    fmt.Println("set user:1001 OK")

    // get
    resp, err := client.Send(ctx, "user:1001", KVRequest{Op: "get", Key: "user:1001"})
    if err != nil {
        log.Fatalf("get failed: %v", err)
    }
    fmt.Printf("get user:1001 → found=%v value=%s\n", resp.Found, resp.Value)

    // del
    _, err = client.Send(ctx, "user:1001", KVRequest{Op: "del", Key: "user:1001"})
    if err != nil {
        log.Fatalf("del failed: %v", err)
    }
    fmt.Println("del user:1001 OK")

    // get after del
    resp, err = client.Send(ctx, "user:1001", KVRequest{Op: "get", Key: "user:1001"})
    if err != nil {
        log.Fatalf("get after del failed: %v", err)
    }
    fmt.Printf("get user:1001 after del → found=%v\n", resp.Found)

    cancel()
    <-errCh
}
```

### 실행 및 예상 출력

```
set user:1001 OK
get user:1001 → found=true value={"name":"alice"}
del user:1001 OK
get user:1001 after del → found=false
```

### 검증 포인트

- [ ] SDK가 PM에 연결 후 첫 라우팅 테이블을 수신하고 `Start`가 unblock된다.
- [ ] `user:1001` 키가 단일 파티션(전체 범위)으로 라우팅된다.
- [ ] `set` → `get` 순서로 데이터가 일관성 있게 조회된다.
- [ ] `del` 후 `get`에서 `found=false`를 반환한다.

---

## 시나리오 3: 파티션 Split

### 목표

단일 파티션을 `m` 기준으로 분할하고, 분할 후 두 파티션에 걸친 키가 각자 올바른
파티션으로 라우팅되는지 확인한다.

**시나리오 1·2 상태를 이어받는다.**

### 절차

**사전 데이터 삽입**

```bash
# 테스트 데이터: 알파벳 앞쪽(a~l)과 뒤쪽(m~z) 모두 삽입
# (예시이므로 examples/kv_basic을 수정하거나 abctl 대신 직접 SDK를 사용)
```

**Split 실행**

```bash
# 현재 라우팅 테이블에서 파티션 ID 확인
./bin/abctl -pm $PM_ADDR routing

PARTITION_ID="<위에서 확인한 파티션 ID>"

# "m" 기준으로 split: [""..m) 과 [m.."") 두 파티션 생성
./bin/abctl -pm $PM_ADDR split $PARTITION_ID m
```

예상 출력:

```
split successful
new partition ID: <new-uuid>
```

**Split 후 라우팅 테이블 확인**

```bash
./bin/abctl -pm $PM_ADDR routing
```

예상 출력:

```
Version: 2

PARTITION-ID                          KEY-START         KEY-END           NODE-ID     NODE-ADDR
------------------------------------  ----------------  ----------------  ----------  ----------------
<original-uuid>                       (start)           m                 ps-1        localhost:8001
<new-uuid>                            m                 (end)             ps-1        localhost:8001
```

### 검증 포인트

- [ ] `abctl split` 후 라우팅 테이블 버전이 2로 증가한다.
- [ ] 하위 파티션: `[start, m)`, 상위 파티션: `[m, end)`.
- [ ] `user:1001` (< "m") → 하위 파티션, `object:zebra` (>= "m") → 상위 파티션으로 라우팅.
- [ ] SDK가 새 라우팅 테이블을 자동으로 수신하고 이후 요청을 올바른 파티션으로 보낸다.

---

## 시나리오 4: Scale-out (PS 추가 + Migrate)

### 목표

두 번째 PS를 추가하고 `abctl migrate`로 상위 파티션을 PS-2로 이동한 뒤,
이동된 파티션에 대한 데이터 접근이 PS-2에서 정상 동작하는지 확인한다.

**시나리오 3 상태를 이어받는다.**

### 절차

**터미널 5: PS-2 기동**

```bash
./bin/kv_server \
  -node-id ps-2 \
  -addr $PS2_ADDR \
  -etcd $ETCD_ADDR \
  -data-dir $DATA_DIR_2 \
  -checkpoint-dir $CHECKPOINT_DIR
```

**PS-2 등록 확인**

```bash
# etcd에서 노드 목록 조회
etcdctl get --prefix /actorbase/nodes/
```

예상: `ps-1`, `ps-2` 두 노드가 등록되어 있어야 한다.

**상위 파티션 Migrate**

```bash
./bin/abctl -pm $PM_ADDR routing
NEW_PARTITION_ID="<split에서 생성된 상위 파티션 ID>"

./bin/abctl -pm $PM_ADDR migrate $NEW_PARTITION_ID ps-2
```

예상 출력:

```
migrate successful
```

**Migrate 후 라우팅 테이블 확인**

```bash
./bin/abctl -pm $PM_ADDR routing
```

예상 출력:

```
Version: 3

PARTITION-ID                          KEY-START         KEY-END           NODE-ID     NODE-ADDR
------------------------------------  ----------------  ----------------  ----------  ----------------
<original-uuid>                       (start)           m                 ps-1        localhost:8001
<new-uuid>                            m                 (end)             ps-2        localhost:8002
```

### 검증 포인트

- [ ] Migrate 중 라우팅 테이블에 `PartitionStatusDraining`이 잠시 표시된다.
- [ ] Migrate 완료 후 상위 파티션의 NODE-ADDR가 `localhost:8002`로 변경된다.
- [ ] SDK가 새 라우팅 테이블을 수신하고, 이후 `m` 이상 키는 PS-2로 전송된다.
- [ ] Migrate 전 PS-1에 저장된 상위 파티션 데이터가 PS-2에서 정상 조회된다.
  (checkpoint를 통해 PS-2가 상태를 복원)

---

## 시나리오 5: 장애 복구 (ManualPolicy)

### 목표

PS-1을 강제 종료한 뒤, ManualPolicy 환경에서 자동 복구가 없음을 확인하고
`abctl migrate`로 수동으로 파티션을 PS-2로 이동하여 서비스를 복구한다.

**시나리오 4 상태를 이어받는다. (ps-1: 하위 파티션, ps-2: 상위 파티션)**

### 절차

**PS-1 강제 종료**

```bash
# PS-1 프로세스를 SIGKILL로 종료 (graceful shutdown 없이)
kill -9 $(pgrep -f "ps.*node-id ps-1")
```

**etcd Lease 만료 대기**

PS의 기본 EtcdLeaseTTL은 10초다. 10~15초 대기한다.

```bash
sleep 15
```

**노드 상태 확인**

```bash
etcdctl get --prefix /actorbase/nodes/
```

예상: `ps-1` 키가 사라지고 `ps-2`만 남아있어야 한다.

**라우팅 테이블 확인**

```bash
./bin/abctl -pm $PM_ADDR routing
```

> **주의**: ManualPolicy에서 PM은 NodeLeft 이벤트를 무시한다. 라우팅 테이블은
> 여전히 ps-1을 가리키며, SDK가 ps-1으로 요청을 보내다 실패한다.

**하위 파티션 수동 Migrate**

```bash
LOWER_PARTITION_ID="<하위 파티션 ID>"
./bin/abctl -pm $PM_ADDR migrate $LOWER_PARTITION_ID ps-2
```

> ps-1이 다운되어 있으므로 `ExecuteMigrateOut` RPC가 실패한다. Migrator는
> 라우팅 테이블을 Active로 복구한다. 이 경우 checkpoint에서 직접 ps-2에
> PreparePartition을 호출하는 별도의 복구 절차가 필요하다.
>
> **현재 구현의 한계**: ps-1이 완전히 다운된 상태에서는 `migrate`가 실패한다.
> 이는 알려진 한계로, 향후 "강제 migrate" 기능 도입이 필요하다.

**현재 가능한 우회 방법**

```bash
# ps-1을 재기동하여 graceful shutdown 유도
./bin/kv_server -node-id ps-1 -addr $PS1_ADDR -etcd $ETCD_ADDR -data-dir $DATA_DIR_1
# 기동 직후 SIGTERM 전송 → graceful shutdown 후 migrate
```

### 검증 포인트

- [ ] PS-1 강제 종료 후 etcd에서 ps-1 노드 키가 TTL 만료로 삭제된다.
- [ ] ManualPolicy에서 PM이 NodeLeft 이벤트를 처리하지 않고 라우팅 테이블이 유지된다.
- [ ] ps-1이 살아있는 상태에서 `abctl migrate`가 성공한다.
- [ ] Migrate 후 하위 파티션도 ps-2에서 정상 응답한다.

---

## 시나리오 6: SDK 라우팅 자동 갱신

### 목표

Split 또는 Migrate 진행 중에도 SDK 클라이언트가 재시도 로직을 통해 요청을
정상 처리하는지 확인한다.

**시나리오 2 상태를 이어받는다. (단일 파티션)**

### 절차

**SDK 부하 생성기 실행**

아래 프로그램을 `examples/kv_stress/main.go`로 저장하고 실행한다.

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    adapterjson "github.com/oomymy/actorbase/adapter/json"
    "github.com/oomymy/actorbase/sdk"
)

type KVRequest struct {
    Op    string `json:"op"`
    Key   string `json:"key"`
    Value []byte `json:"value"`
}

type KVResponse struct {
    Value []byte `json:"value"`
    Found bool   `json:"found"`
}

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    client, err := sdk.NewClient(sdk.Config[KVRequest, KVResponse]{
        PMAddr:        "localhost:8000",
        Codec:         adapterjson.New(),
        MaxRetries:    5,
        RetryInterval: 200 * time.Millisecond,
    })
    if err != nil {
        log.Fatal(err)
    }

    errCh := make(chan error, 1)
    go func() { errCh <- client.Start(ctx) }()

    // 지속적으로 set/get 반복 (Split/Migrate 중에도 계속 실행)
    success, fail := 0, 0
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()

    timer := time.NewTimer(60 * time.Second)
    defer timer.Stop()

    i := 0
    for {
        select {
        case <-timer.C:
            fmt.Printf("완료: success=%d fail=%d\n", success, fail)
            cancel()
            return
        case <-ticker.C:
            key := fmt.Sprintf("key:%06d", i%1000)
            _, err := client.Send(ctx, key, KVRequest{Op: "set", Key: key, Value: []byte("v")})
            if err != nil {
                fmt.Printf("fail [%s]: %v\n", key, err)
                fail++
            } else {
                success++
            }
            i++
        }
    }
}
```

**Split 실행 (부하 생성기 실행 중에)**

```bash
PARTITION_ID="<현재 파티션 ID>"
./bin/abctl -pm $PM_ADDR split $PARTITION_ID k
```

### 예상 동작

| 단계 | SDK 동작 |
|---|---|
| Split 명령 전달 | 정상 응답 (success 계속 증가) |
| PS가 Split 실행 중 | `ErrPartitionBusy` 수신 → RetryInterval 대기 후 재시도 |
| PM이 새 라우팅 전파 | SDK가 `WatchRouting` 채널을 통해 라우팅 자동 갱신 |
| Split 완료 후 | 각 키가 올바른 파티션으로 라우팅, 정상 응답 재개 |

### 검증 포인트

- [ ] 부하 생성기 실행 중 Split을 실행해도 fail 수가 MaxRetries(5) 이하로 유지된다.
- [ ] Split 완료 후 success가 계속 증가하며 fail이 0으로 수렴한다.
- [ ] SDK 로그에 `ErrPartitionBusy` / `ErrPartitionMoved`로 인한 재시도가 기록된다.

---

## 공통 트러블슈팅

### etcd 연결 실패

```
failed to create etcd client: dial tcp localhost:2379: connection refused
```

→ `etcd` 프로세스가 실행 중인지 확인: `pgrep etcd`

### PM이 라우팅 테이블을 생성하지 않음

→ PM 기동 시점에 PS가 이미 등록되어 있으면 bootstrap은 실행되지 않는다.
  etcd에서 `/actorbase/routing` 키를 확인한다.

```bash
etcdctl get /actorbase/routing
```

→ 값이 있으면 이미 라우팅 테이블이 존재한다. 없으면 PM 로그에서 에러를 확인한다.

### abctl routing에서 응답이 없음

→ PM의 WatchRouting 스트림 연결 실패. PM 프로세스가 실행 중인지 확인한다.

```bash
pgrep -a pm
```

### Migrate 후 데이터가 조회되지 않음

→ `PreparePartition` 단계에서 PS-2가 checkpoint를 로드한다. checkpoint 파일이
  공유 스토리지(NFS, S3 등)에 있어야 두 PS가 모두 접근 가능하다.
  반드시 `-checkpoint-dir`을 동일한 경로로 지정하여 두 PS가 같은 checkpoint 디렉토리를 공유해야 한다.

```bash
# 올바른 예: WAL은 각자, checkpoint는 공유
./bin/kv_server -node-id ps-1 -addr $PS1_ADDR -etcd $ETCD_ADDR \
  -data-dir $DATA_DIR_1 -checkpoint-dir $CHECKPOINT_DIR
./bin/kv_server -node-id ps-2 -addr $PS2_ADDR -etcd $ETCD_ADDR \
  -data-dir $DATA_DIR_2 -checkpoint-dir $CHECKPOINT_DIR
```

---

## 시나리오 실행 순서 요약

```
시나리오 1 → 시나리오 2 → 시나리오 3 → 시나리오 4 → 시나리오 5
                                    ↘
                                     시나리오 6 (시나리오 2에서 분기)
```

| 시나리오 | 핵심 검증 항목 | 예상 소요 시간 |
|---|---|---|
| 1. 부트스트랩 | PM bootstrap, 초기 라우팅 테이블 생성 | 2분 |
| 2. 기본 KV 동작 | SDK set/get/del, Actor Receive | 2분 |
| 3. Split | 파티션 분할, 라우팅 갱신 | 3분 |
| 4. Scale-out + Migrate | PS 추가, 파티션 이동, 데이터 복원 | 5분 |
| 5. 장애 복구 | ManualPolicy, TTL 만료, 수동 migrate | 5분 |
| 6. 라우팅 자동 갱신 | SDK 재시도, WatchRouting push | 5분 |
