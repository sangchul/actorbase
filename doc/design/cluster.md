# internal/cluster 패키지 설계

etcd 기반 클러스터 멤버십 및 라우팅 테이블 관리.
PS·PM이 사용한다. SDK는 etcd에 직접 접근하지 않는다.

의존성: `internal/domain`, `go.etcd.io/etcd/client/v3`

파일 목록:
- `registry.go`   — NodeRegistry: etcd lease 기반 노드 등록/조회
- `membership.go` — MembershipWatcher: 노드 join/leave 이벤트 감지
- `routing.go`    — RoutingTableStore: 라우팅 테이블 저장/조회/watch
- `pm.go`         — PM presence 등록/감지 (RegisterPM, WaitForPM, GetPMAddr)
- `policy.go`     — AutoBalancer 정책 저장/로드 (SavePolicy, LoadPolicy, ClearPolicy)

---

## etcd 키 스키마

```
/actorbase/nodes/{nodeID}   → NodeInfo JSON  (TTL lease)
/actorbase/routing          → RoutingTable JSON (no TTL)
/actorbase/pm/{addr}        → PM 주소 (TTL lease, PM presence 표시용)
/actorbase/policy           → AutoBalancer 정책 YAML (no TTL)
```

| 키 | 쓰는 주체 | 읽는 주체 |
|---|---|---|
| `/actorbase/nodes/{nodeID}` | PS | PM (ListNodes, MembershipWatcher) |
| `/actorbase/routing` | PM | PS (Watch) |
| `/actorbase/pm/{addr}` | PM (RegisterPM) | PS (WaitForPM, drainPartitions) |
| `/actorbase/policy` | PM (SavePolicy/ClearPolicy) | PM (LoadPolicy, Start 시 복원) |

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

PS의 etcd 등록/해제/조회를 담당한다. lease + keepalive 방식으로 노드 장애 시 TTL 만료로 자동 제거된다.

| 메서드 | 설명 |
|---|---|
| `Register(ctx, node)` | etcd에 등록 후 ctx 취소까지 keepalive 유지 |
| `Deregister(ctx, nodeID)` | lease를 즉시 revoke |
| `ListNodes(ctx)` | 현재 등록된 모든 노드 반환 |

**PS 생명주기:**
- 시작: `go registry.Register(ctx, myNode)` (ctx = PS 전체 생명주기)
- 종료: `registry.Deregister(ctx, myNode.ID)` 후 ctx cancel

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

## pm.go (PM presence 관리)

PS가 PM 없이 기동되는 것을 방지하기 위한 유틸리티.

| 함수 | 설명 |
|---|---|
| `RegisterPM(ctx, etcdCli, pmAddr)` | PM 기동 시 etcd에 lease로 등록. keepalive는 내부 goroutine에서 처리. |
| `GetPMAddr(ctx, etcdCli)` | etcd에서 PM 주소 조회. PS의 drainPartitions에서 사용. |
| `WaitForPM(ctx, etcdCli)` | PM이 등록될 때까지 최대 10초 대기. 없으면 에러 반환. |

PS 기동 시 WaitForPM 실패 → PS 기동 중단.

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
| 노드 등록 방식 | etcd lease + keepalive | 노드 장애 시 TTL 만료로 자동 제거. 별도 gossip 라이브러리 불필요. |
| RoutingTable 직렬화 | JSON (내부 DTO) | 운영 가시성. 핫패스 아님. |
| Watch 재연결 보정 | 스냅샷 diff | etcd watch reconnect 시 이벤트 유실 방지. |
| Watch 초기 동기화 | Watch 시작 시 현재 테이블 즉시 전달 | PS 시작 시 Load + Watch 이중 호출 불필요. |

---

## 알려진 한계

- **PS etcd watch 확장성**: 노드 1,000대 = etcd watch 1,000개. 규모가 커지면 PM gRPC push 방식 전환 필요.
- **RoutingTable 크기**: 파티션 수가 매우 많아지면 etcd 단일 키의 JSON이 커진다.
- **PM HA 미지원**: RoutingTableStore.Save에 동시성 제어가 없으므로 PM 단일 인스턴스일 때만 안전하다.
- **NodeLeft 이벤트 지연**: lease TTL 동안 장애 노드가 살아있는 것으로 간주된다.
