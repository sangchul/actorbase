# ps 패키지 설계

Partition Server(PS) 조립 패키지. 내부 컴포넌트(`engine`, `cluster`, `transport`)를 조합하여 PS를 구성하고 기동·종료를 관리한다.

의존성: `internal/engine`, `internal/cluster`, `internal/transport`, `provider`

파일 목록:
- `config.go`            — BaseConfig + TypeConfig[Req,Resp]: PS 설정 구조체
- `server.go`            — ServerBuilder, Register[Req,Resp](), Server: 조립 및 생명주기 관리
- `partition_handler.go` — PartitionService gRPC 핸들러 (SDK → PS, data plane)
- `control_handler.go`   — PartitionControlService gRPC 핸들러 (PM → PS, control plane)

---

## config.go

multi-actor-type 지원을 위해 설정이 두 레벨로 분리된다.

**BaseConfig**: PS 인스턴스 전체의 공통 설정. 모든 actor type이 공유.

| 필드 | 설명 |
|---|---|
| NodeID, Addr | 클러스터 내 유일한 PS 식별자, gRPC 수신 주소 |
| EtcdEndpoints | etcd 엔드포인트 목록 |
| MailboxSize, FlushSize, FlushInterval | ActorHost / WAL 배치 설정 |
| IdleTimeout, EvictInterval | EvictionScheduler 설정 |
| CheckpointInterval, CheckpointWALThreshold | CheckpointScheduler 설정 |
| EtcdLeaseTTL | 노드 lease TTL. 기본값: 10초 |
| DrainTimeout | 파티션 선이전 최대 대기 시간. 기본값: 60초 |
| ShutdownTimeout | EvictAll 최대 대기 시간. 기본값: 30초 |

**TypeConfig[Req, Resp]**: 특정 actor type에 대한 설정.

| 필드 | 설명 |
|---|---|
| TypeID | actor type 식별자 ("kv", "bucket" 등) |
| Factory | 파티션별 Actor 생성 팩토리 |
| Codec | SDK와 동일한 구현체를 주입해야 한다 |
| WALStore, CheckpointStore | 스토리지 구현체 |

---

## server.go

### multi-actor-type 타입 소거 구조

Go 제네릭은 non-generic 타입에 generic 메서드를 허용하지 않는다. 이를 우회하기 위해:
- `actorDispatcher` 인터페이스로 `engine.ActorHost[Req,Resp]`를 타입 소거
- `typedDispatcher[Req,Resp]`가 Codec을 이용해 `[]byte ↔ Req/Resp` 변환을 처리
- `Server`는 non-generic. `dispatchers map[string]actorDispatcher`로 타입별 호스트를 관리

`actorDispatcher` 인터페이스:

```go
type actorDispatcher interface {
    Send(ctx, partitionID string, payload []byte) ([]byte, error)
    Activate(ctx context.Context, partitionID string) error
    Evict(ctx context.Context, partitionID string) error
    EvictAll(ctx context.Context) error
    Split(ctx context.Context, partitionID, splitKey, keyRangeStart, keyRangeEnd, newPartitionID string) (string, error)
    StartSchedulers(ctx context.Context, ...)
    GetStats() []engine.PartitionStats  // Auto Balancer용
    TypeID() string
}
```

### 사용 예시

```go
builder := ps.NewServerBuilder(ps.BaseConfig{NodeID: "ps-1", Addr: ":8001", ...})

ps.Register(builder, ps.TypeConfig[KVRequest, KVResponse]{
    TypeID: "kv", Factory: ..., Codec: ..., WALStore: ..., CheckpointStore: ...,
})

srv, err := builder.Build()
srv.Start(ctx)
```

### 컴포넌트 조립 (Build)

```
Register[Req, Resp](builder, typeCfg):
  1. typeID 중복 검사
  2. engine.NewActorHost[Req, Resp] 생성
  3. typedDispatcher{host, codec} 생성 → dispatchers[typeID]에 저장

Build():
  1. etcd 클라이언트, NodeRegistry, RoutingTableStore 생성
  2. gRPC 서버 생성 (transport.NewGRPCServer)
  3. partitionHandler{dispatchers} 등록 (data plane)
  4. controlHandler{dispatchers} 등록 (control plane)
  5. Server 반환
```

### 기동 순서 (Start)

```
Start:
  1. cluster.WaitForPM(ctx, etcdCli)      // PM이 etcd에 등록될 때까지 대기 (최대 10초)
  2. go registry.Register(ctx, myNode)    // etcd 등록 + keepalive 시작
  3. rtCh = rtStore.Watch(ctx)            // RoutingTable watch 시작
  4. <초기 RoutingTable 수신 대기>         // 준비 전 요청 수락 방지
  5. go watchRouting(ctx, rtCh)           // 이후 갱신 백그라운드 처리
  6. grpcSrv.Serve(listener)              // gRPC 수신 시작 (비동기)
  7. for each dispatcher:
       go dispatcher.StartSchedulers(ctx, ...)
  8. <-ctx.Done()
  9. graceful shutdown
```

### 종료 순서 (graceful shutdown)

```
ctx 취소 시:
  1. drainPartitions(drainCtx)          // 파티션 선이전: PM에 RequestMigrate
                                        // gRPC 서버가 살아있는 동안 실행해야 함
  2. grpcSrv.GracefulStop()             // 진행 중인 RPC 완료 대기
  3. for each dispatcher:
       dispatcher.EvictAll(shutdownCtx) // drain 실패 파티션 safety checkpoint
  4. registry.Deregister(ctx, nodeID)  // etcd lease 즉시 revoke
```

### drainPartitions (내부)

PS 종료 전 자신의 파티션을 PM에 위임하는 절차.

```
drainPartitions(ctx):
  1. cluster.GetPMAddr(ctx, etcdCli)  // etcd에서 PM 주소 조회
  2. PMClient.ListMembers()           // 가용 노드 목록 조회
  3. routing.Load()에서 내 파티션 추출 (entry.Node.ID == myNodeID)
  4. for each 파티션:
       target = targets[round-robin] (나 제외 active 노드)
       PMClient.RequestMigrate(entry.Partition.ActorType, partitionID, target)
  5. 실패 파티션: 로그 후 계속. NodeLeft 후 failoverNode가 처리.
```

> `drainPartitions`에는 DrainTimeout(기본 60초)을, `EvictAll`에는 별도 ShutdownTimeout(기본 30초)을 사용한다.

---

## partition_handler.go (data plane)

`req.ActorType`으로 dispatcher를 선택하여 Actor에 요청을 전달한다.

```
Send(req):
  1. routing.LookupByPartition(req.PartitionID) → entry
     └ 없음 → ErrPartitionNotOwned
  2. entry.Node.ID != myNodeID → ErrPartitionNotOwned
  3. dispatchers[req.ActorType] → dispatcher
     └ 없음 → 알 수 없는 actor type 에러
  4. dispatcher.Send(ctx, req.PartitionID, req.Payload)

Scan(req):
  1. routing.LookupByPartition(req.PartitionID) → entry
     └ 없음 → ErrPartitionNotOwned
  2. entry.Node.ID != myNodeID → ErrPartitionNotOwned
  3. entry.Partition.KeyRange.Start != req.ExpectedKeyRangeStart
     || entry.Partition.KeyRange.End != req.ExpectedKeyRangeEnd
     → ErrPartitionMoved  // stale routing 감지: SDK가 라우팅 갱신 후 재시도
  4. dispatchers[req.ActorType] → dispatcher
  5. dispatcher.Send(ctx, req.PartitionID, req.Payload)  // 동일 경로 재사용
```

> Scan은 Send와 동일한 Actor.Receive() 경로를 통과한다. `"scan"`/`"list"` op를 인식한 Actor가 범위 내 항목을 `Resp` 안에 담아 반환한다. SDK가 파티션별 결과를 수집하여 `[]Resp`로 합쳐 호출자에게 돌려준다.

---

## control_handler.go (control plane)

PM의 split/migrate/stats 명령을 처리한다. 각 핸들러는 `req.ActorType`으로 dispatcher를 조회 후 호출한다.

| RPC | 처리 흐름 |
|---|---|
| ExecuteSplit | 1) splitKey가 없고 KeyRangeStart/End도 없으면 routing table에서 partition key range 조회<br>2) dispatcher.Split(ctx, partitionID, splitKey, keyRangeStart, keyRangeEnd, newPartitionID) → usedKey<br>3) ExecuteSplitResponse{SplitKey: usedKey} 반환 |
| ExecuteMigrateOut | dispatcher.Evict(ctx, partitionID) |
| PreparePartition | dispatcher.Activate(ctx, partitionID) |
| GetStats | 모든 dispatcher.GetStats() 결과를 집계하여 반환 |

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| multi-actor-type 지원 | actorDispatcher 인터페이스 + typedDispatcher | Go 제네릭은 generic method를 허용하지 않아 타입 소거 필요 |
| Register 위치 | package-level generic 함수 | generic method 제약 우회. `Register[Req,Resp](b, cfg)` 형태 |
| 설정 분리 | BaseConfig + TypeConfig[Req,Resp] | 공통 설정과 타입별 설정의 명확한 분리 |
| dispatcher 라우팅 | req.ActorType → dispatchers[actorType] | proto에 actor_type 필드로 handlers가 non-generic 가능 |
| 라우팅 테이블 보관 | atomic.Pointer[domain.RoutingTable] | 핫패스에서 잠금 없이 읽기 가능 |
| graceful shutdown 순서 | drainPartitions → GracefulStop → EvictAll → Deregister | drain 선행으로 정상 파티션 이전 |

---

## 알려진 한계

- **Split 중 mailbox 중단**: split 파티션의 mailbox가 Actor.Split + checkpoint 저장 시간만큼 중단된다.
- **Deregister 실패 시 lease 잔존**: etcd lease가 TTL까지 남아 PM 장애 감지에 지연이 생긴다.
- **PartitionService에 인증 없음**: 클러스터 내부망 환경 가정. mTLS 등 인증은 향후 과제.
