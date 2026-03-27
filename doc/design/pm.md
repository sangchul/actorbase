# pm 패키지 설계

Partition Manager(PM) 조립 패키지. 클러스터 멤버십 감시, 라우팅 테이블 관리, SDK 라우팅 push, split/migrate 조율을 담당한다.

의존성: `internal/cluster`, `internal/rebalance`, `internal/transport`, `internal/domain`, `provider`, `policy`

파일 목록:
- `config.go`              — Config: PM 설정 및 의존성 주입 구조체
- `server.go`              — Server: 컴포넌트 조립 및 생명주기 관리
- `manager_handler.go`     — PartitionManagerService gRPC 핸들러 (SDK/abctl → PM)
- `balancer.go`            — balancerRunner: 주기적 stats 수집 및 split/migrate 자동 실행
- `client.go`              — Client: PM 관리 플레인 공개 클라이언트 (abctl 등이 사용)

> BalancePolicy 구현체(`ThresholdPolicy`, `NoopBalancePolicy`)는 최상위 `policy/` 패키지에 위치한다.

---

## config.go

| 필드 | 설명 |
|---|---|
| ListenAddr | gRPC 수신 주소 (필수) |
| EtcdEndpoints | etcd 엔드포인트 목록 (필수) |
| ActorTypes | bootstrap 시 생성할 actor type 목록 (필수, 최소 1개) |
| HTTPAddr | 웹 콘솔 HTTP 서버 주소 (예: `:8080`). 비어 있으면 웹 콘솔을 시작하지 않는다. |
| Metrics | nil이면 no-op 구현체 사용 |
| BalancePolicy | nil이면 NoopBalancePolicy 사용. YAML policy 적용 중에는 YAML policy가 우선. |

> PM은 NodeRegistry에 등록하지 않는다. etcd election(`/actorbase/pm/election`)을 통해 리더를 선출하며, 리더만 gRPC 서버를 기동한다. PS는 `WaitForLeader`로 리더 선출을 확인한 후 기동된다.

---

## server.go

### 컴포넌트 조립 (NewServer)

```
NewServer:
  1. cfg 검증 (필수 필드 누락 확인)
  2. cfg.BalancePolicy == nil이면 NoopBalancePolicy로 대체
  3. etcd 클라이언트 생성
  4. NodeRegistry, NodeCatalog, MembershipWatcher, RoutingTableStore 생성
  5. ConnPool, Splitter, Migrator, Merger 생성
  6. gRPC 서버 생성 + PartitionManagerService 핸들러 등록
```

### 기동 순서 (Start)

```
Start:
  1. serverCtx = ctx                                                   // balancer goroutine lifetime 참조용
  2. sess = cluster.CampaignLeader(ctx, etcdCli, cfg.ListenAddr)      // etcd election — 리더 될 때까지 블로킹
                                                                       // standby PM은 여기서 대기
     defer sess.Close()                                                // 종료 시 resign → standby가 새 리더 선출
  3. cluster.LoadPolicy(ctx, etcdCli)                                  // 저장된 AutoPolicy 복원 (있으면 balancer 시작)
  4. reconcileCatalog(ctx)                                             // catalog Active/Draining vs live heartbeat 대조
                                                                       // heartbeat 없는 노드 → Waiting 복원
  5. currentRT = routingStore.Load()
  6. routing.Store(currentRT)
  7. go watchRouting(ctx)        // etcd watch → 구독자 broadcast
  8. go watchMembership(ctx)     // 노드 join/leave → Policy 호출
  9. if currentRT == nil:
       go bootstrap(ctx)         // 첫 PS 등록 대기 후 초기 테이블 생성
  10. if cfg.HTTPAddr != "":
        go consoleSrv.Start(ctx) // 웹 콘솔 HTTP 서버 (go:embed 정적 파일 + REST API)
  11. grpcSrv.Serve(listener)    // 리더만 gRPC 포트 오픈
  12. <-ctx.Done()
  13. grpcSrv.GracefulStop() + connPool.Close()
      // defer sess.Close() 실행 → etcd lease revoke → standby 자동 선출
```

> **HA 동작**: leader가 종료되면 `sess.Close()`가 etcd lease를 revoke한다. standby PM들은 `Campaign()`에서 대기 중이다가 하나가 새 leader로 선출된다. 새 leader는 etcd에서 routing table, policy를 복원하고 gRPC 서버를 오픈한다.

> PM은 자신이 보유한 Actor나 상태가 없으므로 leader 전환이 단순하다. 모든 상태는 etcd에 있다.

### reconcileCatalog (내부)

PM 재시작 시 catalog 상태를 live heartbeat와 동기화한다.

```
reconcileCatalog(ctx):
  1. nodeRegistry.ListLiveNodeIDs()  // 현재 heartbeat 키 목록
  2. nodeCatalog.ListNodes()         // catalog 전체 노드 목록
  3. for each catalog node where status == Active or Draining:
       if node.ID not in liveSet:
         nodeCatalog.UpdateStatus(node.ID, Waiting)
         // PM 크래시 중 PS가 종료되면 catalog가 Active로 남는 케이스 복원
```

> heartbeat가 살아있는 Active 노드는 건드리지 않는다. Failed 노드도 건드리지 않는다 (이미 운영자 확인 필요 상태).

### watchRouting (내부)

etcd 라우팅 테이블 변경을 감지하여 로컬 캐시(`routing atomic.Pointer`)를 갱신하고 모든 WatchRouting 구독자에게 broadcast한다.

broadcast 전략: 각 구독자는 최신값(`atomic.Pointer`)과 신호 채널(버퍼 1)을 갖는다. 빠른 연속 변경 시 중간 값은 건너뛰고 최신 값만 전달된다. SDK 입장에서는 최신 라우팅 테이블만 있으면 충분하다.

### watchMembership (내부)

노드 이벤트를 `handleNodeJoined` / `handleNodeLeft`로 위임한다. 각각 goroutine으로 실행.

### handleNodeJoined (내부)

```
handleNodeJoined(node):
  1. nodeCatalog.GetNode(node.ID) → catalog에서 상태 확인
     Active가 아니면 경고 로그 (정상 flow에서는 RequestJoin 후 heartbeat가 생기므로 이미 Active)
  2. nodeCatalog.ListNodes(Active 필터) + routingStore.Load()
  3. activeBalancePolicy().OnNodeJoined(ctx, node, quickClusterStats) → []BalanceAction
  4. executeBalanceActions(actions)
```

### handleNodeLeft (내부)

```
handleNodeLeft(node):
  1. nodeCatalog.GetNode(node.ID) → wasDraining = (status == Draining)
  2. if wasDraining:
       failoverDeadNode(ctx, node.ID)        // 잔여 파티션 있으면 failover (drain 완료 시 no-op)
       nodeCatalog.UpdateStatus(node.ID, Waiting)  // 정상 종료 → Waiting 복귀
     else:
       nodeCatalog.UpdateStatus(node.ID, Failed)   // 예기치 않은 장애 → Failed
       failoverDeadNode(ctx, node.ID)        // 파티션 failover
       // Failed 상태 유지. abctl node reset으로만 Waiting 복귀.
  3. activeBalancePolicy().OnNodeLeft(ctx, node, stats) → []BalanceAction
  4. executeBalanceActions(actions)
```

> Draining 후 NodeLeft: drainPartitions가 파티션을 이미 이전했으므로 failoverDeadNode는 no-op.
> 예기치 않은 장애 NodeLeft: Failed 상태가 유지되어 운영자가 `abctl node reset`으로 명시적 허가 전까지 재합류 불가.

### failoverDeadNode (내부)

Policy와 **무관하게** dead node에 잔여 파티션이 남아 있으면 active 노드로 자동 failover한다. `handleNodeLeft`에서 호출된다.

```
failoverDeadNode(ctx, deadNodeID):
  1. routingStore.Load()로 현재 라우팅 테이블 조회
  2. dead node에 남은 파티션 목록 추출
  3. nodeCatalog.ListNodes(Active 필터) → active 노드 목록 (dead node 제외)
  4. 파티션을 round-robin으로 active 노드에 배분
  5. 각 파티션에 대해 migrator.Failover(ctx, partitionID, targetNodeID) 호출
```

> Policy의 `OnNodeLeft`가 ActionFailover를 반환하지 않는 경우(예: `NoopBalancePolicy`)에도
> failoverDeadNode가 잔여 파티션을 반드시 복구한다. 이는 **Policy 구현과 무관한 안전망**이다.

### activeBalancePolicy (내부)

```
activeBalancePolicy():
  YAML policy 적용 중 (activeRunnerCfg != nil) → YAML policy 반환
  아니면 → cfg.BalancePolicy 반환
```

### quickClusterStats (내부)

이벤트 핸들러용 경량 ClusterStats. GetStats RPC 없이 라우팅 테이블에서만 구성.
live 노드: Reachable=true, 파티션 목록 (RPS 없음).
dead 노드: Reachable=false, 파티션 목록 (라우팅 테이블 기반).

### executeBalanceActions (내부)

```
for action in actions:
  Split:    opMu.Lock → splitter.Split
  Migrate:  opMu.Lock → migrator.Migrate
  Failover: opMu.Lock → migrator.Failover
  Merge:    opMu.Lock → merger.Merge (upper가 다른 노드이면 먼저 migrator.Migrate)
```

### bootstrap (내부)

빈 클러스터(라우팅 테이블 없음) 시 첫 PS 등록을 기다려 초기 라우팅 테이블을 생성한다.

`cfg.ActorTypes` 각 타입마다 전체 키 범위 `["", "")` → 첫 번째 PS 로 초기 파티션을 생성한다. etcd CAS(Compare-And-Swap)로 중복 생성을 방어한다.

---

## manager_handler.go

| RPC | 처리 |
|---|---|
| WatchRouting | 연결 즉시 현재 테이블 전송 후 변경 시마다 스트리밍 push |
| RequestJoin | catalog에서 nodeID 조회 → Waiting 상태 검증 → Active 전이. 미등록/비-Waiting이면 PERMISSION_DENIED. |
| SetNodeDraining | catalog에서 nodeID 조회 → Active 상태 검증 → Draining 전이. |
| AddNode | nodeCatalog.AddNode() — Waiting 상태로 신규 등록. 이미 존재하면 AlreadyExists. |
| RemoveNode | 상태 검증 (Waiting/Failed만 허용) 후 nodeCatalog.RemoveNode(). Active/Draining이면 FailedPrecondition. |
| ResetNode | Failed 상태 검증 후 nodeCatalog.UpdateStatus(Waiting). Failed가 아니면 FailedPrecondition. |
| RequestSplit | AutoPolicy 활성 시 `PERMISSION_DENIED` 거부. 아니면 opMu 잠금 후 splitter.Split |
| RequestMigrate | AutoPolicy 활성 시 `PERMISSION_DENIED` 거부. 아니면 opMu 잠금 후 migrator.Migrate |
| RequestMerge | AutoPolicy 활성 시 `PERMISSION_DENIED` 거부. 아니면 opMu 잠금 후 merger.Merge |
| ListMembers | nodeCatalog.ListNodes() 조회 후 4가지 상태 포함 반환 |
| GetClusterStats | Active 노드에만 GetStats RPC를 병렬 호출 (5초 timeout) 후 집계 |
| ApplyPolicy | YAML 파싱 → cluster.SavePolicy → autoBalancer 재시작 |
| GetPolicy | 현재 activePolicyYAML 및 active 여부 반환 |
| ClearPolicy | cluster.ClearPolicy → autoBalancer 중단 → NoopBalancePolicy(또는 cfg.BalancePolicy)로 복귀 |

> `opMu`로 split/migrate를 직렬화한다. autoBalancer도 동일한 `opMu`를 사용한다.

---

## policy

**NoopBalancePolicy** (최상위 `policy/noop.go`): 모든 메서드가 nil 반환. `pm.Config.BalancePolicy`의 기본값.

**ThresholdPolicy** (최상위 `policy/threshold.go`): YAML `abctl policy apply`로 활성화되는 임계값 기반 `provider.BalancePolicy` 구현체.
- `Evaluate`: RPS/key count 임계값 초과 시 split, 인접 파티션 쌍의 합산 RPS/key count가 임계값 이하이면 merge (stable_rounds 연속 충족 필요), 노드 간 파티션 수/RPS 불균형 시 migrate 반환.
- `OnNodeJoined`: 부하가 가장 많은 노드에서 파티션 하나를 새 노드로 migrate 반환.
- `OnNodeLeft`: Reachable=false인 노드의 파티션에 ActionFailover 반환.

사용자는 `provider.BalancePolicy` 인터페이스를 직접 구현하여 `pm.Config{BalancePolicy: myPolicy}`로 주입할 수 있다. 자세한 내용은 `doc/design/auto-balancer.md` 참조.

**autoBalancer 차단 없음**: AutoPolicy(YAML) 활성 중에도 `abctl split`, `abctl migrate` 수동 명령은 `PERMISSION_DENIED`로 거부된다.

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| 라우팅 broadcast 방식 | atomic.Pointer + 버퍼 1 채널 | 느린 구독자는 중간 값을 skip. SDK는 최신 라우팅만 필요. |
| split/migrate 직렬화 | opMu sync.Mutex | 단순하고 안전. PM 단일 인스턴스 환경에서 충분. |
| BalancePolicy 인터페이스 | provider.BalancePolicy (OnNodeJoined / OnNodeLeft / Evaluate) | Policy 교체 가능. 기본값은 NoopBalancePolicy. |
| bootstrap 초기 파티션 | ActorTypes별로 전체 키 범위 파티션 생성 | 다중 actor type 클러스터 지원. |
| bootstrap CAS | etcd Compare-And-Swap | PM HA 환경에서 중복 생성 방어. |
| 클러스터 합류 제어 | RequestJoin RPC + NodeCatalog Waiting 검증 | PM이 허락하지 않은 PS 차단. 사전 등록 필수. |
| Failed 상태 유지 | 자동 복구 없음, abctl node reset 필수 | 장애 노드의 자동 재합류 방지. 운영자가 장애 확인 후 명시적 허가. |
| NodeLeft 장애 복구 위치 | Policy + failoverDeadNode 이중 보장 | Policy가 ActionFailover를 반환할 수 있고, 그 후 failoverDeadNode가 잔여 파티션을 추가 복구. graceful이면 파티션 없어 자연히 no-op. |
| Failover vs Migrate | Migrator.Failover: ExecuteMigrateOut 건너뜀 | source PS 죽었으면 gRPC 호출 불가. checkpoint에서 직접 복원. |
| PM 리더 선출 | etcd election (`concurrency.Campaign`) | 여러 PM 중 하나만 active. 리더 장애 시 standby가 자동 승계. PS가 PM 없이 기동되는 것도 방지. |
| PM startup reconcile | leader 선출 직후 catalog vs heartbeat diff | PM 크래시 중 PS 종료 시 catalog Active 잔존 케이스 자동 복원. |

---

## 알려진 한계

- **PM leader 전환 중 가용성 간격**: leader 종료 후 etcd TTL 만료(~10초)까지 새 leader가 gRPC 서버를 열지 않는다. 이 기간 동안 SDK는 캐시된 라우팅으로 PS에 직접 요청할 수 있지만 split/migrate/WatchRouting은 불가하다.
- **단일 PS 클러스터 장애**: 가용 target 노드가 없으면 failoverNode가 파티션을 재배치하지 못한다.
- **autoBalancer split key 품질**: keyRangeMidpoint는 바이트 레벨 산술 평균으로 계산하므로, 키 분포가 균등하지 않은 경우 편향된 split이 발생할 수 있다.
