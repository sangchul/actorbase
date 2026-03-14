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
| Metrics | nil이면 no-op 구현체 사용 |
| BalancePolicy | nil이면 NoopBalancePolicy 사용. YAML policy 적용 중에는 YAML policy가 우선. |

> PM은 NodeRegistry에 등록하지 않는다. PS가 PM 없이 기동되는 것을 방지하기 위해 별도 presence 키(`/actorbase/pm/{addr}`)를 lease로 등록한다.

---

## server.go

### 컴포넌트 조립 (NewServer)

```
NewServer:
  1. cfg 검증 (필수 필드 누락 확인)
  2. cfg.Policy == nil이면 ManualPolicy로 대체
  3. etcd 클라이언트 생성
  4. NodeRegistry, MembershipWatcher, RoutingTableStore 생성
  5. ConnPool, Splitter, Migrator 생성
  6. gRPC 서버 생성 + PartitionManagerService 핸들러 등록
```

### 기동 순서 (Start)

```
Start:
  1. serverCtx = ctx                                    // balancer goroutine lifetime 참조용
  2. cluster.RegisterPM(ctx, etcdCli, cfg.ListenAddr)  // etcd에 PM presence 등록
  3. cluster.LoadPolicy(ctx, etcdCli)                   // 저장된 AutoPolicy 복원 (있으면 balancer 시작)
  4. currentRT = routingStore.Load()
  5. routing.Store(currentRT)
  6. go watchRouting(ctx)        // etcd watch → 구독자 broadcast
  7. go watchMembership(ctx)     // 노드 join/leave → Policy 호출
  8. if currentRT == nil:
       go bootstrap(ctx)         // 첫 PS 등록 대기 후 초기 테이블 생성
  9. grpcSrv.Serve(listener)
  10. <-ctx.Done()
  11. grpcSrv.GracefulStop() + connPool.Close()
```

> PM은 자신이 보유한 Actor나 상태가 없으므로 종료가 단순하다. 모든 상태는 etcd에 있다.

### watchRouting (내부)

etcd 라우팅 테이블 변경을 감지하여 로컬 캐시(`routing atomic.Pointer`)를 갱신하고 모든 WatchRouting 구독자에게 broadcast한다.

broadcast 전략: 각 구독자는 최신값(`atomic.Pointer`)과 신호 채널(버퍼 1)을 갖는다. 빠른 연속 변경 시 중간 값은 건너뛰고 최신 값만 전달된다. SDK 입장에서는 최신 라우팅 테이블만 있으면 충분하다.

### watchMembership (내부)

노드 이벤트를 `handleNodeJoined` / `handleNodeLeft`로 위임한다. 각각 goroutine으로 실행.

### handleNodeJoined (내부)

```
handleNodeJoined(node):
  1. nodeRegistry.ListNodes() + routingStore.Load()
  2. activeBalancePolicy().OnNodeJoined(ctx, node, quickClusterStats) → []BalanceAction
  3. executeBalanceActions(actions)
```

### handleNodeLeft (내부)

```
handleNodeLeft(node, reason):
  1. nodeRegistry.ListNodes() + routingStore.Load()
  2. quickClusterStats에 dead node를 Reachable=false로 포함 (라우팅 테이블 기반 파티션 목록)
  3. activeBalancePolicy().OnNodeLeft(ctx, node, reason, stats) → []BalanceAction
  4. executeBalanceActions(actions)  // ActionFailover가 포함될 수 있음
```

> graceful drain 완료 후 NodeLeft가 발생하면 라우팅 테이블에 해당 노드 파티션이 없으므로
> OnNodeLeft가 ActionFailover를 반환하지 않아 no-op가 된다.

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
```

### bootstrap (내부)

빈 클러스터(라우팅 테이블 없음) 시 첫 PS 등록을 기다려 초기 라우팅 테이블을 생성한다.

`cfg.ActorTypes` 각 타입마다 전체 키 범위 `["", "")` → 첫 번째 PS 로 초기 파티션을 생성한다. etcd CAS(Compare-And-Swap)로 중복 생성을 방어한다.

---

## manager_handler.go

| RPC | 처리 |
|---|---|
| WatchRouting | 연결 즉시 현재 테이블 전송 후 변경 시마다 스트리밍 push |
| RequestSplit | AutoPolicy 활성 시 `PERMISSION_DENIED` 거부. 아니면 opMu 잠금 후 splitter.Split |
| RequestMigrate | AutoPolicy 활성 시 `PERMISSION_DENIED` 거부. 아니면 opMu 잠금 후 migrator.Migrate |
| ListMembers | nodeRegistry.ListNodes() 조회 후 반환 |
| GetClusterStats | 모든 PS에 GetStats RPC를 병렬 호출 (5초 timeout) 후 집계 |
| ApplyPolicy | YAML 파싱 → cluster.SavePolicy → autoBalancer 재시작 |
| GetPolicy | 현재 activePolicyYAML 및 active 여부 반환 |
| ClearPolicy | cluster.ClearPolicy → autoBalancer 중단 → ManualPolicy |

> `opMu`로 split/migrate를 직렬화한다. autoBalancer도 동일한 `opMu`를 사용한다.

---

## policy

**NoopBalancePolicy** (최상위 `policy/noop.go`): 모든 메서드가 nil 반환. `pm.Config.BalancePolicy`의 기본값.

**ThresholdPolicy** (최상위 `policy/threshold.go`): YAML `abctl policy apply`로 활성화되는 임계값 기반 `provider.BalancePolicy` 구현체.
- `Evaluate`: RPS/key count 임계값 초과 시 split, 노드 간 파티션 수/RPS 불균형 시 migrate 반환.
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
| NodeLeft 장애 복구 위치 | BalancePolicy.OnNodeLeft 반환 ActionFailover | 정책 구현체가 failover 여부를 결정. graceful이면 파티션 없어 자연히 no-op. |
| Failover vs Migrate | Migrator.Failover: ExecuteMigrateOut 건너뜀 | source PS 죽었으면 gRPC 호출 불가. checkpoint에서 직접 복원. |
| PM presence 등록 | etcd `/actorbase/pm/{addr}`에 lease로 등록 | PS가 PM 없이 기동되는 것을 방지. |

---

## 알려진 한계

- **PM 단일 인스턴스**: PM이 다운되면 split/migrate/WatchRouting 불가. PS는 계속 동작하고 SDK는 캐시된 라우팅으로 요청을 처리할 수 있으나, 라우팅 변경은 반영되지 않는다. PM HA는 향후 과제.
- **단일 PS 클러스터 장애**: 가용 target 노드가 없으면 failoverNode가 파티션을 재배치하지 못한다.
- **autoBalancer split key 품질**: keyRangeMidpoint는 바이트 레벨 산술 평균으로 계산하므로, 키 분포가 균등하지 않은 경우 편향된 split이 발생할 수 있다.
