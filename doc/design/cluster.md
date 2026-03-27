# internal/cluster 패키지 설계

etcd 기반 클러스터 멤버십 및 라우팅 테이블 관리.
PS·PM이 사용한다. SDK는 etcd에 직접 접근하지 않는다.

의존성: `internal/domain`, `go.etcd.io/etcd/client/v3`

파일 목록:
- `registry.go`   — NodeRegistry (heartbeat), NodeCatalog (영속 상태): etcd 기반 노드 관리
- `membership.go` — MembershipWatcher: 노드 join/leave 이벤트 감지
- `routing.go`    — RoutingTableStore: 라우팅 테이블 저장/조회/watch
- `pm.go`         — PM 리더 선출 (CampaignLeader, WaitForLeader, GetLeaderAddr)
- `policy.go`     — AutoBalancer 정책 저장/로드 (SavePolicy, LoadPolicy, ClearPolicy)

---

## etcd 키 스키마

```
/actorbase/node_catalog/{nodeID}  → NodeInfo JSON  (no TTL, PM 관리, 4가지 상태)
/actorbase/nodes/{nodeID}         → NodeInfo JSON  (TTL lease, PS heartbeat, 생존 신호)
/actorbase/routing                → RoutingTable JSON (no TTL)
/actorbase/pm/election/{leaseID}  → PM 리더 주소 (TTL lease, etcd election)
/actorbase/policy                 → AutoBalancer 정책 YAML (no TTL)
```

| 키 | 쓰는 주체 | 읽는 주체 | TTL |
|---|---|---|---|
| `/actorbase/node_catalog/{nodeID}` | PM (NodeCatalog) | PM (내부) | 없음 |
| `/actorbase/nodes/{nodeID}` | PS (NodeRegistry) | PM (MembershipWatcher, reconcile) | ~10초 |
| `/actorbase/routing` | PM | PS (Watch) | 없음 |
| `/actorbase/pm/election/{leaseID}` | PM (CampaignLeader) | PS (WaitForLeader), SDK | ~10초 |
| `/actorbase/policy` | PM (SavePolicy/ClearPolicy) | PM (LoadPolicy) | 없음 |

---

## etcd 확장성 고려

### SDK가 etcd를 직접 watch하지 않는 이유

SDK 인스턴스가 수백 개라면 etcd watch 커넥션도 수백 개가 된다. etcd는 대규모 클라이언트 fanout에 적합하지 않으며, SDK 사용자가 etcd 접속 설정을 직접 관리해야 하는 부담도 생긴다.

대신 PM이 etcd를 단일 watch하고 SDK에 gRPC로 fanout한다:

```
etcd ←── watch(1개) ── PM ── gRPC push ──→ SDK 인스턴스 (수백 개)
```

### PS etcd watch의 확장성 한계 (미해결)

현재 PS는 라우팅 테이블 변경을 etcd를 직접 watch한다. 설계 목표 규모(수백~1,000 노드)에서 etcd v3는 수천 개의 watch 커넥션을 지원하므로 우선 이 방식을 유지한다. 실제 운영에서 부하가 문제가 되면 PM이 PS에도 gRPC push하는 방식으로 전환을 검토한다.

---

## NodeRegistry

PS의 etcd heartbeat 등록/해제를 담당한다. lease + keepalive 방식으로 노드 장애 시 TTL 만료로 자동 제거된다. **liveness 신호 전용**으로, 노드 상태 관리는 NodeCatalog가 담당한다.

| 메서드 | 설명 |
|---|---|
| `Register(ctx, node)` | TTL lease로 heartbeat 키 등록 후 ctx 취소까지 keepalive 유지 |
| `Deregister(ctx, nodeID)` | lease를 즉시 revoke (heartbeat 키 즉시 제거) |
| `ListLiveNodeIDs(ctx)` | 현재 heartbeat 키가 살아있는 nodeID 목록 반환. PM startup reconcile에서 사용. |

**PS 생명주기:**
- 시작: RequestJoin 승인 후 `go registry.Register(ctx, myNode)` (ctx = PS 전체 생명주기)
- 종료: `registry.Deregister(ctx, myNode.ID)` 후 ctx cancel

---

## NodeCatalog

PM이 관리하는 영속 노드 레지스트리. TTL 없이 etcd에 4가지 상태(Waiting/Active/Draining/Failed)를 저장한다. `abctl node add`로 사전 등록한 노드만 클러스터에 합류할 수 있다.

| 메서드 | 설명 |
|---|---|
| `AddNode(ctx, node)` | Waiting 상태로 노드 신규 등록. 이미 존재하면 에러. |
| `UpdateStatus(ctx, nodeID, status)` | 노드 상태 변경. PM 내부 전환에만 사용. |
| `RemoveNode(ctx, nodeID)` | 노드 완전 삭제. |
| `GetNode(ctx, nodeID)` | 단일 노드 조회. 없으면 found=false. |
| `ListNodes(ctx)` | 4가지 상태 포함 전체 노드 목록 반환. |

**PM 사용처:**
- `RequestJoin` RPC: Waiting 상태 검증 후 Active 전이
- `SetNodeDraining` RPC: Active → Draining 전이
- `handleNodeLeft`: Draining → Waiting, 또는 Active/Draining → Failed 전이
- `abctl node add/remove/reset`: AddNode, RemoveNode, Failed→Waiting 전이
- `reconcileCatalog` (PM startup): heartbeat 없는 Active/Draining → Waiting 복원

---

## MembershipWatcher

노드 join/leave 이벤트를 감지한다. PM이 사용한다.

`Watch(ctx)` 채널로 `NodeJoined` / `NodeLeft` 이벤트를 수신한다.

**etcd watch 재연결 보정**: 네트워크 단절 후 재연결 시 `/actorbase/nodes/` 전체를 스냅샷으로 읽어 이전 상태와 diff하여 놓친 이벤트를 보정한다.

---

## RoutingTableStore

라우팅 테이블 저장/조회/watch를 담당한다.

| 메서드 | 설명 |
|---|---|
| `Save(ctx, rt)` | 라우팅 테이블을 etcd에 저장 |
| `Load(ctx)` | 현재 라우팅 테이블 조회. 없으면 nil 반환. |
| `Watch(ctx)` | 변경 시마다 새 테이블을 전달하는 채널. 시작 시 현재 테이블을 즉시 전달. |

`domain.RoutingTable`은 unexported 필드를 가지므로 내부 DTO로 변환하여 JSON 직렬화한다. JSON을 사용하는 이유는 운영 가시성(etcdctl로 직접 확인 가능)이다. 핫패스가 아니므로 성능 영향은 없다.

---

## pm.go (PM 리더 선출 / 발견)

etcd election 기반 PM HA를 지원하는 유틸리티. `go.etcd.io/etcd/client/v3/concurrency` 패키지를 사용한다.

| 함수 | 설명 |
|---|---|
| `CampaignLeader(ctx, etcdCli, pmAddr)` | etcd election 참여. 리더가 될 때까지 블로킹. 반환된 `*Session`을 Close하면 resign. |
| `GetLeaderAddr(ctx, etcdCli)` | 현재 PM 리더 주소를 조회. 리더 없으면 에러. PS의 drainPartitions 및 SDK HA 모드에서 사용. |
| `WaitForLeader(ctx, etcdCli)` | PM 리더가 선출될 때까지 최대 30초 watch 대기. 리더 주소를 반환. PS Start() 초반에 호출. |

**election 동작 원리:**
- PM 여러 개가 동시에 `CampaignLeader`를 호출하면 etcd election이 하나를 leader로 선출한다.
- Leader가 된 PM만 이후 로직(gRPC 서버 오픈 등)을 진행한다. 나머지는 standby 상태로 대기.
- Leader PM 종료(ctx 취소 → `sess.Close()`) 시 etcd lease가 revoke되고, standby PM 중 하나가 자동으로 새 leader로 선출된다.
- etcd election 키: `/actorbase/pm/election`, lease TTL: 10초.

PS 기동 시 `WaitForLeader` 실패 → PS 기동 중단.

---

## policy.go (AutoBalancer 정책 영속화)

PM의 AutoBalancer 정책을 etcd에 저장·복원하는 유틸리티.

| 함수 | 설명 |
|---|---|
| `SavePolicy(ctx, etcdCli, yamlStr)` | etcd `/actorbase/policy`에 YAML 저장 (TTL 없음) |
| `LoadPolicy(ctx, etcdCli)` | 저장된 YAML 반환. 없으면 빈 문자열. |
| `ClearPolicy(ctx, etcdCli)` | 키 삭제 (ManualPolicy로 복귀) |

PM 재시작 시 `Start()`에서 `LoadPolicy`를 호출하여 AutoPolicy를 자동 복원한다.

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| SDK 라우팅 수신 | PM gRPC push | etcd watch 커넥션 수 제한. SDK 사용자가 etcd 설정 불필요. |
| PS 라우팅 수신 | etcd watch (현재) | 목표 규모에서 허용 가능. 부하 문제 시 PM gRPC push로 전환 검토. |
| 노드 생존 신호 | etcd lease + keepalive (NodeRegistry) | 노드 장애 시 TTL 만료로 자동 제거. gossip 불필요. |
| 노드 상태 관리 | etcd no-TTL (NodeCatalog) | Waiting/Failed 상태는 heartbeat와 별개로 영속. PM 재시작에도 유지. |
| 클러스터 합류 제어 | RequestJoin RPC (PM 승인) | PM이 허락하지 않은 PS는 기동 불가. Waiting 상태 사전 등록 필수. |
| Failed 복구 | `abctl node reset` 수동 | 장애 노드의 자동 재합류 방지. 운영자가 장애 확인 후 명시적 허가. |
| PM 재시작 reconcile | startup 시 catalog vs heartbeat diff | PM 크래시 중 PS가 종료되면 catalog가 Active로 남는 케이스 복원. |
| PM HA | etcd election (concurrency.Campaign) | 여러 PM이 경쟁, 리더만 gRPC 서버 오픈. 리더 장애 시 standby가 자동 승계. |
| RoutingTable 직렬화 | JSON (내부 DTO) | 운영 가시성. 핫패스 아님. |
| Watch 재연결 보정 | 스냅샷 diff | etcd watch reconnect 시 이벤트 유실 방지. |
| Watch 초기 동기화 | Watch 시작 시 현재 테이블 즉시 전달 | PS 시작 시 Load + Watch 이중 호출 불필요. |

---

## 알려진 한계

- **PS etcd watch 확장성**: 노드 1,000대 = etcd watch 1,000개. 규모가 커지면 PM gRPC push 방식 전환 필요.
- **RoutingTable 크기**: 파티션 수가 매우 많아지면 etcd 단일 키의 JSON이 커진다.
- **PM HA split/migrate 동시성**: etcd election으로 active PM은 하나뿐이므로 `opMu`로의 직렬화는 여전히 안전하다. 단, leader 전환 직후 standby가 이전 leader의 진행 중인 operation 상태를 알 수 없다.
- **NodeLeft 이벤트 지연**: lease TTL 동안 장애 노드가 살아있는 것으로 간주된다.
- **reconcileCatalog 경쟁 조건**: PM 재시작과 PS 재기동이 동시에 발생하면 reconcile이 Active 노드를 Waiting으로 되돌린 직후 PS가 RequestJoin을 재시도해야 한다. TTL 기반 heartbeat가 아직 살아있다면 reconcile이 해당 노드를 건너뛰므로 문제 없다.
