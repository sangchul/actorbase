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
│   │  (kv_client / kv_stress)│          │  members / routing /     │         │
│   └────────────┬────────────┘          │  split / migrate         │         │
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
│            ┌──────────────────────────────────────────────────────────┐   │
│            │  WAL Store  (shared)                                     │   │
│            │  /tmp/actorbase/wal                                      │   │
│            │                                                          │   │
│            │  {partition-A}/{lsn}  ← PS-1이 쓰고 failover 시 PS-2가 읽음│  │
│            │  {partition-B}/{lsn}  ← PS-2가 담당                     │   │
│            └──────────────────────────────────────────────────────────┘   │
│                                                                           │
│            ┌──────────────────────────────────────────────────────────┐   │
│            │  Checkpoint Store  (shared)                              │   │
│            │  /tmp/actorbase/checkpoint                               │   │
│            │                                                          │   │
│            │  PS-1 ──write──▶  {partition-id}.snapshot  ◀──read── PS-2│  │
│            └──────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────────┘
```

### 통신 흐름 요약

| 화살표 | 프로토콜 | 설명 |
|---|---|---|
| SDK → PM | gRPC stream | `WatchRouting`: 라우팅 테이블 변경을 실시간으로 수신 |
| SDK → PS | gRPC | `Send`: key로 담당 파티션 Actor에 요청 전달 |
| abctl → PM | gRPC | `members` / `routing` / `split` / `migrate` 명령 |
| PM ↔ etcd | etcd gRPC | 라우팅 테이블 저장·조회, 노드 등록 이벤트 감시 |
| PS ↔ etcd | etcd gRPC | 노드 등록 (lease 기반 TTL), 라우팅 테이블 구독 |
| PM → PS | gRPC | `ExecuteSplit` / `ExecuteMigrateOut` / `PreparePartition` |
| PS → WAL Store | 파일 I/O | WAL entry 추가·재생 (**공유 디렉토리**, 파티션별 서브디렉토리로 격리) |
| PS ↔ Checkpoint Store | 파일 I/O | Snapshot 저장·복원 (**공유 디렉토리**, migrate/failover 시 핵심) |

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
go build -o bin/kv_client ./examples/kv_client
go build -o bin/pm        ./cmd/pm
go build -o bin/abctl     ./cmd/abctl
go build -o bin/kv_stress ./examples/kv_stress
```

### 환경 변수 (공통)

```bash
export ETCD_ADDR="localhost:2379"
export PM_ADDR="localhost:8000"
export PS1_ADDR="localhost:8001"
export PS2_ADDR="localhost:8002"
export WAL_DIR="/tmp/actorbase/wal"              # 모든 PS가 공유하는 WAL 디렉토리 (파티션별 서브디렉토리로 격리)
export CHECKPOINT_DIR="/tmp/actorbase/checkpoint" # 모든 PS가 공유하는 checkpoint 디렉토리
```

### 클린 상태 초기화

각 시나리오 시작 전 실행한다.

```bash
# etcd 데이터 초기화
etcdctl del --prefix /actorbase

# 공유 WAL 및 checkpoint 디렉토리 초기화
rm -rf $WAL_DIR $CHECKPOINT_DIR
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
./bin/pm -addr :8000 -etcd $ETCD_ADDR -actor-types kv
```

`-actor-types`는 bootstrap 시 생성할 actor type 목록이다 (쉼표 구분).
복수 타입 예시: `-actor-types bucket,object`

예상 로그:

```
starting PM addr=:8000 actor_types=kv
```

**터미널 3: PS-1 기동**

```bash
./bin/kv_server \
  -node-id ps-1 \
  -addr $PS1_ADDR \
  -etcd $ETCD_ADDR \
  -wal-dir $WAL_DIR \
  -checkpoint-dir $CHECKPOINT_DIR
```

예상 로그:

```
starting PS node-id=ps-1 addr=localhost:8001
```

PM 로그에서 bootstrap 완료 메시지 확인:

```
pm bootstrap: initial routing table created actor_types=[kv] node=ps-1
```

**터미널 4: 멤버 확인**

```bash
./bin/abctl -pm $PM_ADDR members
```

예상 출력:

```
NODE-ID                               ADDRESS              STATUS
------------------------------------  --------------------  ------
ps-1                                  localhost:8001        active
```

**라우팅 테이블 확인**

```bash
./bin/abctl -pm $PM_ADDR routing
```

예상 출력:

```
Version: 1

PARTITION-ID                          ACTOR-TYPE    KEY-START         KEY-END           NODE-ID                               NODE-ADDR
------------------------------------  ------------  ----------------  ----------------  ------------------------------------  -----------
<uuid>                                kv            (start)           (end)             ps-1                                  localhost:8001
```

### 검증 포인트

- [ ] PM 기동 시 라우팅 테이블 없음 (`currentRT == nil`) 상태에서 bootstrap 고루틴이 시작된다.
- [ ] PS-1 등록 후 PM이 `NodeJoined` 이벤트를 수신하고 `kv` actor type의 초기 파티션을 생성한다.
- [ ] `abctl members`로 ps-1이 active 상태로 등록된 것을 확인한다.
- [ ] `abctl routing`으로 Version 1, actor_type=kv, 전체 범위 `["", "")` 파티션을 확인한다.

---

## 시나리오 2: 기본 KV 동작

### 목표

SDK를 통해 Actor에 `set` / `get` / `del` 요청을 전송하고 정상 응답을 확인한다.

**시나리오 1 상태를 이어받는다.**

### 절차

`bin/kv_client` CLI를 사용한다.

```bash
# set
./bin/kv_client -pm $PM_ADDR set user:1001 '{"name":"alice"}'

# get
./bin/kv_client -pm $PM_ADDR get user:1001

# del
./bin/kv_client -pm $PM_ADDR del user:1001

# get after del (not found → exit code 1)
./bin/kv_client -pm $PM_ADDR get user:1001 || echo "not found (expected)"
```

### 예상 출력

```
ok
{"name":"alice"}
ok
key "user:1001" not found
not found (expected)
```

### 검증 포인트

- [ ] `kv_client set`이 `ok`를 출력한다.
- [ ] `kv_client get`이 설정한 값을 출력한다.
- [ ] `kv_client del`이 `ok`를 출력한다.
- [ ] `del` 후 `get`에서 `key not found`와 exit code 1을 반환한다.

---

## 시나리오 3: 파티션 Split

### 목표

단일 파티션을 `m` 기준으로 분할하고, 분할 후 두 파티션에 걸친 키가 각자 올바른
파티션으로 라우팅되는지 확인한다.

**시나리오 1·2 상태를 이어받는다.**

### 절차

**사전 데이터 삽입**

```bash
# 알파벳 앞쪽 (split 기준 "m" 미만)
./bin/kv_client -pm $PM_ADDR set apple red
./bin/kv_client -pm $PM_ADDR set banana yellow

# 알파벳 뒤쪽 (split 기준 "m" 이상)
./bin/kv_client -pm $PM_ADDR set mango orange
./bin/kv_client -pm $PM_ADDR set zebra black
```

**Split 실행**

```bash
# 현재 라우팅 테이블에서 파티션 ID 확인
./bin/abctl -pm $PM_ADDR routing

PARTITION_ID="<위에서 확인한 파티션 ID>"

# "m" 기준으로 split: [""..m) 과 [m.."") 두 파티션 생성
# actor-type을 명시해야 한다 (안전 검증용 — PM이 라우팅 테이블의 실제 값과 비교)
./bin/abctl -pm $PM_ADDR split kv $PARTITION_ID m
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

PARTITION-ID                          ACTOR-TYPE    KEY-START         KEY-END           NODE-ID     NODE-ADDR
------------------------------------  ------------  ----------------  ----------------  ----------  ----------------
<original-uuid>                       kv            (start)           m                 ps-1        localhost:8001
<new-uuid>                            kv            m                 (end)             ps-1        localhost:8001
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
  -wal-dir $WAL_DIR \
  -checkpoint-dir $CHECKPOINT_DIR
```

**PS-2 등록 확인**

```bash
./bin/abctl -pm $PM_ADDR members
```

예상 출력:

```
NODE-ID                               ADDRESS              STATUS
------------------------------------  --------------------  ------
ps-1                                  localhost:8001        active
ps-2                                  localhost:8002        active
```

**상위 파티션 Migrate**

```bash
./bin/abctl -pm $PM_ADDR routing
NEW_PARTITION_ID="<split에서 생성된 상위 파티션 ID>"

# actor-type을 명시해야 한다 (안전 검증용)
./bin/abctl -pm $PM_ADDR migrate kv $NEW_PARTITION_ID ps-2
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

PARTITION-ID                          ACTOR-TYPE    KEY-START         KEY-END           NODE-ID     NODE-ADDR
------------------------------------  ------------  ----------------  ----------------  ----------  ----------------
<original-uuid>                       kv            (start)           m                 ps-1        localhost:8001
<new-uuid>                            kv            m                 (end)             ps-2        localhost:8002
```

### 검증 포인트

- [ ] Migrate 중 라우팅 테이블에 `PartitionStatusDraining`이 잠시 표시된다.
- [ ] Migrate 완료 후 상위 파티션의 NODE-ADDR가 `localhost:8002`로 변경된다.
- [ ] SDK가 새 라우팅 테이블을 수신하고, 이후 `m` 이상 키는 PS-2로 전송된다.
- [ ] Migrate 전 PS-1에 저장된 상위 파티션 데이터가 PS-2에서 정상 조회된다.
  (checkpoint를 통해 PS-2가 상태를 복원)

---

## 시나리오 5: 예기치 않은 장애 복구 (자동 Failover)

### 목표

PS-1을 강제 종료(SIGKILL)했을 때 PM이 etcd lease 만료를 감지하고
`failoverNode`를 통해 자동으로 파티션을 PS-2로 재배치하는지 확인한다.
Policy와 무관하게 항상 동작한다.

### WAL replay 검증의 의미

단순히 "데이터가 있다"를 확인하는 것만으로는 부족하다.
Split/Migrate 직후에 SIGKILL하면 checkpoint가 최신 상태이므로 WAL replay 없이도 복원된다.
**진짜 검증은 "checkpoint 이후 WAL에만 존재하는 데이터"가 failover 후 복원되는지다.**

이 시나리오는 SIGKILL 직전에 단순 set 연산을 추가하여 checkpoint 없이 WAL에만 존재하는
데이터를 의도적으로 만든다. checkpoint가 트리거되는 조건은 두 가지다.

| 트리거 | 기본값 | 조건 |
|---|---|---|
| WAL threshold 자동 checkpoint | 100건 | WAL entry가 100건 쌓이면 자동 실행 |
| periodic checkpoint | 1분 | CheckpointScheduler가 1분마다 실행 |

set 3건은 threshold 100에 훨씬 못 미치고, PS가 기동된 지 1분이 지나지 않았으므로
periodic checkpoint도 찍히지 않는다. 따라서 set 3건 후 SIGKILL하면 정확히
"WAL에만 있는 데이터" 상태가 만들어진다.

```
Split → checkpoint (명시 호출)
Migrate → checkpoint (명시 호출)
set apple=red2  → WAL에만 기록 (threshold 100, periodic 1분 → 둘 다 미도달)
set avocado=green → WAL에만 기록
set cherry=red3 → WAL에만 기록
SIGKILL
Failover → checkpoint 로드 + WAL replay → 3건 모두 복원되어야 PASS
```

**시나리오 4 상태를 이어받는다. (ps-1: 하위 파티션, ps-2: 상위 파티션)**

### 절차

**SIGKILL 직전 WAL 전용 데이터 삽입**

```bash
# checkpoint를 유발하지 않는 단순 set 연산만 추가
# → WAL에는 기록되고(응답 수신 = WAL flush 완료) checkpoint에는 없는 상태
./bin/kv_client -pm $PM_ADDR set apple red2
./bin/kv_client -pm $PM_ADDR set avocado green
./bin/kv_client -pm $PM_ADDR set cherry red3
```

**PS-1 강제 종료**

```bash
# SIGKILL: graceful shutdown 없이 즉시 프로세스 종료
kill -9 $(pgrep -f "kv_server.*ps-1")
```

**etcd Lease 만료 대기**

PS의 기본 EtcdLeaseTTL은 10초다. 만료까지 10~15초 대기한다.
이 시간 동안 하위 파티션(`[start, m)`)으로 향하는 요청은 실패한다.

```bash
sleep 15
```

**노드 상태 확인**

```bash
./bin/abctl -pm $PM_ADDR members
```

예상 출력:

```
NODE-ID                               ADDRESS              STATUS
------------------------------------  --------------------  ------
ps-2                                  localhost:8002        active
```

**PM 자동 Failover 확인**

PM 로그에서 failover 완료 메시지를 확인한다.

```
pm: failover complete partition=<uuid> from=ps-1 to=ps-2
```

**라우팅 테이블 확인**

```bash
./bin/abctl -pm $PM_ADDR routing
```

예상 출력 (하위 파티션이 ps-2로 이동):

```
Version: 4

PARTITION-ID                          ACTOR-TYPE    KEY-START         KEY-END           NODE-ID     NODE-ADDR
------------------------------------  ------------  ----------------  ----------------  ----------  ----------------
<original-uuid>                       kv            (start)           m                 ps-2        localhost:8002
<new-uuid>                            kv            m                 (end)             ps-2        localhost:8002
```

**Failover 후 WAL replay 검증**

```bash
# checkpoint 이전 데이터 (checkpoint에서 복원)
./bin/kv_client -pm $PM_ADDR get banana    # yellow
./bin/kv_client -pm $PM_ADDR get mango     # orange

# SIGKILL 직전 WAL에만 있던 데이터 (WAL replay로 복원)
./bin/kv_client -pm $PM_ADDR get apple     # red2 (checkpoint 이전 값 red가 아니어야 함)
./bin/kv_client -pm $PM_ADDR get avocado   # green
./bin/kv_client -pm $PM_ADDR get cherry    # red3
```

### 검증 포인트

- [ ] PS-1 강제 종료 후 etcd에서 ps-1 노드 키가 TTL 만료로 삭제된다.
- [ ] PM이 `NodeLeft` 이벤트를 감지하고 `failoverNode`를 실행한다.
- [ ] 라우팅 테이블의 하위 파티션이 ps-2로 이동된다 (버전 증가).
- [ ] checkpoint 이전 데이터(`banana`, `mango`)가 정상 조회된다.
- [ ] **WAL에만 있던 데이터(`apple=red2`, `avocado`, `cherry`)가 WAL replay로 완전 복원된다.**
- [ ] `apple`이 이전 값 `red`가 아닌 최신 값 `red2`여야 한다. (WAL replay가 실제로 적용됐음을 증명)
- [ ] TTL 만료(~10초) 이후 하위 파티션 요청이 재개된다.

---

## 시나리오 7: Graceful Shutdown — 파티션 선이전

### 목표

PS-1을 정상 종료(SIGTERM)했을 때 `drainPartitions`가 실행되어
자신의 파티션을 PM에 위임하고, 이전이 완료된 뒤 프로세스가 종료되는지 확인한다.
부하 생성기를 동시에 실행하여 연산 중단이 최소화되는지 검증한다.

**시나리오 4 상태를 이어받는다. (ps-1: 하위 파티션, ps-2: 상위 파티션)**

### 절차

**부하 생성기 실행 (별도 터미널)**

```bash
./bin/kv_stress -pm $PM_ADDR
```

kv_stress는 100ms 간격으로 set 요청을 반복하며 success/fail을 집계한다.

**PS-1 Graceful Shutdown**

```bash
# SIGTERM: shutdown() → drainPartitions → GracefulStop → EvictAll → Deregister
kill $(pgrep -f "kv_server.*ps-1")
```

PS-1 로그에서 drain 과정을 확인한다:

```
ps: drain: partition migrated partition=<uuid> target=ps-2
```

**Shutdown 완료 확인**

```bash
./bin/abctl -pm $PM_ADDR members
```

예상 출력:

```
NODE-ID                               ADDRESS              STATUS
------------------------------------  --------------------  ------
ps-2                                  localhost:8002        active
```

**라우팅 테이블 확인**

```bash
./bin/abctl -pm $PM_ADDR routing
```

예상: 모든 파티션이 ps-2로 이동, etcd NodeLeft 이벤트로 failoverNode 실행 시 no-op

```
Version: 4

PARTITION-ID                          ACTOR-TYPE    KEY-START         KEY-END           NODE-ID     NODE-ADDR
------------------------------------  ------------  ----------------  ----------------  ----------  ----------------
<original-uuid>                       kv            (start)           m                 ps-2        localhost:8002
<new-uuid>                            kv            m                 (end)             ps-2        localhost:8002
```

**데이터 정합성 확인**

```bash
./bin/kv_client -pm $PM_ADDR get apple
./bin/kv_client -pm $PM_ADDR get mango
```

### Graceful vs Unexpected death 비교

| 항목 | Graceful (SIGTERM) | Unexpected (SIGKILL) |
|---|---|---|
| 파티션 이전 방식 | PS가 PM에 `RequestMigrate` 위임 | PM이 `failoverNode`로 자동 복구 |
| 연산 중단 시간 | 수 초 (drain 중 ErrPartitionBusy) | etcd TTL 만료까지 (~10초) |
| Data loss 가능성 | 없음 (checkpoint 후 이전) | WALStore 구현에 따라 다름 |
| NodeLeft 후 failoverNode | 파티션 없으므로 no-op | 파티션 있으므로 failover 실행 |

### 검증 포인트

- [ ] SIGTERM 후 PS-1 로그에 `ps: drain: partition migrated` 메시지가 출력된다.
- [ ] `drainPartitions` 완료 후 `GracefulStop` → `Deregister` 순서로 진행된다.
- [ ] NodeLeft 이벤트 발생 시 라우팅 테이블에 ps-1 파티션이 없으므로 failoverNode가 no-op으로 종료된다.
- [ ] kv_stress의 fail 수가 drain 기간 동안만 증가하고 이후 0으로 수렴한다.
- [ ] Shutdown 후 모든 파티션 데이터가 ps-2에서 정상 조회된다.

---

## 시나리오 6: SDK 라우팅 자동 갱신

### 목표

Split 또는 Migrate 진행 중에도 SDK 클라이언트가 재시도 로직을 통해 요청을
정상 처리하는지 확인한다.

**시나리오 2 상태를 이어받는다. (단일 파티션)**

### 절차

**SDK 부하 생성기 실행**

`examples/kv_stress/`를 빌드하여 실행한다.

```bash
go build -o bin/kv_stress ./examples/kv_stress
./bin/kv_stress
```

kv_stress는 60초 동안 100ms 간격으로 set 요청을 반복하며 success/fail을 집계한다.
(`sdk.Client.Start()`는 첫 라우팅 테이블 수신 후 즉시 반환하므로 goroutine 불필요)

**Split 실행 (부하 생성기 실행 중에)**

```bash
PARTITION_ID="<현재 파티션 ID>"
./bin/abctl -pm $PM_ADDR split kv $PARTITION_ID k
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
# 올바른 예: WAL과 checkpoint 모두 공유 디렉토리 사용
./bin/kv_server -node-id ps-1 -addr $PS1_ADDR -etcd $ETCD_ADDR \
  -wal-dir $WAL_DIR -checkpoint-dir $CHECKPOINT_DIR
./bin/kv_server -node-id ps-2 -addr $PS2_ADDR -etcd $ETCD_ADDR \
  -wal-dir $WAL_DIR -checkpoint-dir $CHECKPOINT_DIR
```

---

---

## 시나리오 9: PM HA Failover

### 목표

두 번째 PM(PM-2)을 standby로 기동하고, 현재 leader(PM-1)를 SIGKILL했을 때
PM-2가 자동으로 새 leader로 선출되어 클러스터가 계속 서비스하는지 확인한다.

**시나리오 7·8 이후 상태를 이어받아도 되고, 클러스터가 정상 동작 중이면 어느 시점에서도 실행 가능하다.**

### 전제

```bash
export PM2_ADDR="localhost:8003"
```

### 절차

**PM-2 standby로 기동**

PM-2는 `CampaignLeader`에서 블로킹 중이며, gRPC 포트를 열지 않는다.

```bash
./bin/pm -addr :8003 -etcd $ETCD_ADDR -actor-types kv &
PM2_PID=$!
sleep 2
```

**PM-2가 아직 gRPC 서버를 열지 않았는지 확인 (standby 상태 검증)**

```bash
# PM-2로 abctl 명령 시도 → connection refused 또는 타임아웃 예상
./bin/abctl -pm $PM2_ADDR routing && echo "UNEXPECTED: should fail" || echo "OK: standby confirmed"
```

**PM-1 강제 종료**

```bash
kill -9 $PM_PID
```

**etcd lease 만료 + PM-2 선출 대기 (~15초)**

```bash
sleep 18
```

**PM-2가 leader로 선출되어 gRPC 서버를 열었는지 확인**

```bash
./bin/abctl -pm $PM2_ADDR routing
```

예상 출력: 기존 라우팅 테이블이 etcd에서 복원됨

```
Version: <N>

PARTITION-ID   ACTOR-TYPE   KEY-START   KEY-END   NODE-ID   NODE-ADDR
...
```

PM-2 프로세스 로그 확인:

```
pm: elected as leader addr=:8003
```

**PM-2를 통해 데이터 접근**

```bash
# 기존 데이터 조회
./bin/kv_client -pm $PM2_ADDR get apple

# 신규 데이터 저장/조회
./bin/kv_client -pm $PM2_ADDR set ha_test "works"
./bin/kv_client -pm $PM2_ADDR get ha_test
```

### 검증 포인트

- [ ] PM-2 기동 직후 gRPC 포트가 열리지 않는다 (standby 상태 확인).
- [ ] PM-1 SIGKILL 후 etcd TTL 만료 (~10초) 뒤 PM-2가 leader로 선출된다.
- [ ] PM-2 로그에 `"pm: elected as leader"` 메시지가 출력된다.
- [ ] PM-2에서 `abctl routing`으로 기존 라우팅 테이블이 복원된 것을 확인한다.
- [ ] PM-2를 통해 기존 데이터 조회 및 신규 데이터 저장이 정상 동작한다.

### HA 모드 SDK 자동 재연결 검증 (선택)

SDK `EtcdEndpoints`를 사용하는 경우, PM 장애 시 SDK가 자동으로 새 PM에 재연결한다.
아래는 programmatic 검증 예시 (`examples/kv_stress` 등에서 `EtcdEndpoints` 사용 시):

```go
client, _ := sdk.NewClient(sdk.Config[KVReq, KVResp]{
    EtcdEndpoints: []string{"localhost:2379"},  // PMAddr 대신 etcd로 리더 자동 발견
    TypeID: "kv",
    Codec:  codec,
})
```

PM-1 SIGKILL → PM-2 선출 후 SDK는 `consumeRouting`에서 채널 닫힘을 감지하고
etcd에서 PM-2 주소를 재발견하여 자동 재연결한다. 이 기간 동안 Send는 캐시된 라우팅으로 계속 동작한다.

---

## 시나리오 실행 순서 요약

```
시나리오 1 → 시나리오 2 → 시나리오 3 → 시나리오 4 → 시나리오 5 (SIGKILL → PS 자동 failover)
                                    │                └→ 시나리오 7 (SIGTERM → graceful drain)
                                    ↘                └→ 시나리오 8 (Range Scan)
                                     시나리오 6 (시나리오 2에서 분기, SDK 재시도)

시나리오 9 (PM HA Failover) — 클러스터가 정상 동작 중 어느 시점에서도 실행 가능
```

| 시나리오 | 핵심 검증 항목 | 예상 소요 시간 |
|---|---|---|
| 1. 부트스트랩 | PM bootstrap, 초기 라우팅 테이블 생성, abctl members | 2분 |
| 2. 기본 KV 동작 | SDK set/get/del, Actor Receive | 2분 |
| 3. Split | 파티션 분할, 라우팅 갱신 | 3분 |
| 4. Scale-out + Migrate | PS 추가, abctl members (2노드), 파티션 이동, 데이터 복원 | 5분 |
| 5. 예기치 않은 장애 복구 | SIGKILL, TTL 만료, PM 자동 failover, 데이터 복원 | 5분 |
| 6. SDK 라우팅 자동 갱신 | SDK 재시도, WatchRouting push | 5분 |
| 7. Graceful Shutdown | SIGTERM, drainPartitions, 연산 중단 최소화 | 5분 |
| 8. Range Scan | 다중 파티션 fan-out, 부분 범위 조회 | 3분 |
| 9. PM HA Failover | PM-2 standby, PM-1 SIGKILL, PM-2 리더 승계, 데이터 정상 접근 | 5분 |
