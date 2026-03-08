# actorbase — 분산 저장소 비교

## actorbase의 핵심 포지션

actorbase는 범용 분산 KV 저장소가 아니다.
**Actor = Partition** 모델에 기반한 **도메인 특화 상태 관리 플랫폼**이다.

핵심 전제:
- 각 파티션은 단일 스레드 Actor로 관리된다 (lock-free sequential processing)
- 사용자가 Actor 인터페이스를 구현해 비즈니스 로직을 플랫폼에 주입한다
- 복제 대신 distributed CheckpointStore(S3/NFS)로 장애를 대비한다
- PM이 라우팅 테이블을 관리하고 SDK에 push한다 (SDK→PS direct routing)

아키텍처 참조: **HBase**. HMaster/RegionServer/ZooKeeper/HDFS 구조가 PM/PS/etcd/distributed-fs와 거의 1:1 대응된다.
"RegionServer가 복제 없이 단독으로 Region을 담당하되, HDFS가 내구성을 보장한다"는 HBase의 철학을 actorbase가 그대로 계승한다.

---

## 비교표

| 항목 | actorbase | HBase | Redis Cluster | Cassandra | etcd | TiKV | DynamoDB |
|---|---|---|---|---|---|---|---|
| **데이터 모델** | 사용자 정의 Actor | Column Family (schema) | Key-Value | Key-Value (CQL) | Key-Value | Key-Value (MVCC) | Key-Value (Document) |
| **일관성** | Strong (단일 Actor 직렬화) | Strong (단일 Region → 단일 RS) | Eventual (replica lag) | Tunable (QUORUM~ANY) | Strong (Raft) | Strong (Raft) | Eventual / Strong (옵션) |
| **파티셔닝** | Range-based, 수동 split | Range-based (Row key), 자동 split | Hash slot (16384) | Consistent hashing | 단일 노드 수준 (소규모) | Range-based (Region) | Hash 기반 자동 |
| **복제** | **없음** (distributed fs 의존) | **없음** (HDFS 블록 복제 의존) | 1 primary + N replica | N replica (기본 3) | Raft 다수결 | Raft 다수결 | 3 AZ 자동 |
| **자동 rebalancing** | **미구현** (수동 Split/Migrate) | 자동 (크기 임계값 + balancer) | 자동 resharding | 자동 vnodes | 해당 없음 | 자동 (Region split) | 자동 |
| **Master/Coordinator HA** | **단일 PM (SPOF)** | Active/Standby (ZooKeeper election) | 각 shard 자체 election | 없음 (peer-to-peer) | Raft 리더 선출 | PD leader election | 완전 관리형 |
| **장애 복구** | Failover (checkpoint + WAL replay) | HDFS에서 Region 재open + WAL replay | replica 자동 승격 | 자동 (hint handoff) | Raft 자동 | Raft 자동 | 자동 |
| **커스텀 비즈니스 로직** | **Actor 인터페이스로 주입** | Coprocessor (Observer/Endpoint, 제한적) | Lua 스크립트 제한적 | UDF 제한적 | 없음 | 없음 | Lambda trigger |
| **인메모리 버퍼** | **없음** (on-demand activation) | MemStore(쓰기) + BlockCache(읽기) | 기본 (인메모리 DB) | 없음 | 없음 | 없음 | 없음 |
| **WAL 방식** | 사용자 정의 WALStore | HLog (HDFS에 기록) | AOF / RDB | Commit log | WAL (bbolt) | RocksDB WAL | 내부 |
| **Checkpoint/Flush** | Full snapshot (incremental 미지원) | HFile flush + compaction (LSM-tree) | RDB snapshot | Memtable flush (SST) | snapshot (bbolt) | SST compaction | 내부 |
| **쿼리** | Key lookup only | Row key range scan, column filter | Key lookup, scan | CQL (range, filter) | Key/prefix lookup | Key/range lookup | 제한적 (GSI) |
| **트랜잭션** | Actor 내부만 (파티션 간 없음) | Row 단위 ACID, 다중 row 없음 | MULTI/EXEC (단일 노드) | LWT (경량) | STM (단일 클러스터) | MVCC 분산 트랜잭션 | 조건부 쓰기 |
| **클러스터 조율** | etcd + PM | ZooKeeper + HMaster | Cluster Bus (gossip) | Gossip (ring) | 자체 (Raft) | etcd (PD) | 완전 관리형 |
| **클라이언트 라우팅** | PM→SDK gRPC push, direct | META 테이블 lookup → direct | MOVED redirect | 드라이버 토큰 | 드라이버 | 드라이버 | HTTP endpoint |
| **운영 복잡도** | 중간 (etcd + PM + PS) | 높음 (HDFS + ZooKeeper + HBase) | 중간 | 높음 | 낮음 | 높음 | 낮음 (관리형) |
| **목표 규모** | 수백~1,000 노드 | 수백~수천 노드 | 수백 노드 | 수백~수천 노드 | 수십 노드 | 수천 노드 | 무제한 (관리형) |

---

## HBase와의 심층 비교

HBase는 actorbase가 아키텍처 참조로 삼은 시스템이다.
구성 요소가 거의 1:1로 대응되지만, 핵심 철학이 갈리는 지점이 있다.

### 구성 요소 대응

```
HBase                       actorbase
────────────────────────────────────────────────────
HMaster                  ←→  PM (Partition Manager)
RegionServer             ←→  PS (Partition Server)
Region                   ←→  Partition (Actor)
ZooKeeper                ←→  etcd
HDFS                     ←→  distributed CheckpointStore (S3/NFS)
HLog (WAL)               ←→  WALStore
MemStore flush → HFile   ←→  Actor Snapshot → CheckpointStore
META 테이블 lookup        ←→  PM→SDK RoutingTable push
```

### 동일한 핵심 철학: "복제 없이 shared storage로 내구성 확보"

HBase의 가장 독창적인 결정은 RegionServer에 복제를 두지 않은 것이다.

```
HBase: RegionServer가 쓰는 HFile은 HDFS에 저장 → HDFS가 블록을 3중 복제
       RegionServer 장애 시 → 다른 RegionServer가 HDFS에서 HFile + HLog를 읽어 Region을 열면 됨

actorbase: PS가 쓰는 Checkpoint는 distributed CheckpointStore(S3/NFS)에 저장
           PS 장애 시 → 다른 PS가 CheckpointStore에서 Checkpoint를 읽어 파티션을 복원하면 됨
```

같은 아이디어다. 저장 계층의 내구성(HDFS/S3)에 위임하고, 컴퓨팅 계층(RS/PS)은 Stateless에 가깝게 유지한다.

### 갈리는 지점

#### 1. 데이터 모델: Column Family vs 사용자 정의 Actor

HBase는 Row key + Column Family + Qualifier로 정형화된 스키마를 제공한다.
actorbase는 Actor 인터페이스를 구현하면 어떤 도메인 로직이든 올릴 수 있다.

HBase Coprocessor가 이 간격을 일부 줄여주지만 제약이 많다:
- Observer Coprocessor: put/get/scan 전후 훅. JVM 위에서 동작, 배포 부담.
- Endpoint Coprocessor: 서버 측 aggregation. RPC로 호출하나 범용 연산은 표현 어려움.

actorbase Actor는 임의의 Go 코드를 파티션 상태 머신으로 실행한다. 로직 표현 자유도가 훨씬 높다.

#### 2. 쓰기 경로: LSM-tree vs WAL + full snapshot

```
HBase 쓰기 경로:
  write → HLog(WAL) → MemStore(메모리 버퍼) → flush → HFile(SST)
  compaction: minor(SST merge) / major(전체 merge, GC 포함)

actorbase 쓰기 경로:
  Receive() → WAL append → Actor 상태 변경 (in-memory)
  주기적: Actor.Snapshot() → CheckpointStore.Save (full snapshot)
  WAL trim: checkpoint LSN 이전 WAL 삭제
```

HBase의 compaction은 실질적으로 incremental checkpoint에 가깝다. SST를 단계적으로 merge하므로
전체 상태를 한 번에 직렬화할 필요가 없다. actorbase는 이 부분이 full snapshot으로 단순화되어 있어
상태가 커지면 checkpoint 비용이 높아진다는 차이가 있다.

#### 3. 인메모리 버퍼: MemStore vs 없음

HBase는 MemStore가 쓰기를 버퍼링한다. flush 전까지 읽기는 MemStore + HFile을 합쳐서 반환한다.
actorbase는 Actor 자체가 상태를 메모리에 보유하지만 명시적 버퍼 계층이 없다.
on-demand activation으로 자주 쓰이는 Actor만 메모리에 존재한다.

#### 4. Master HA: Active/Standby vs SPOF

HBase HMaster는 ZooKeeper leader election으로 Active/Standby HA를 지원한다.
Standby Master는 HMaster 장애 시 수 초 내 승격된다.

actorbase PM은 현재 단일 인스턴스다. PM 장애 시 라우팅 갱신과 failover가 불가능해진다.
etcd leader lock을 활용한 Active/Standby 방식이 향후 과제다.

#### 5. 자동 rebalancing: 내장 vs 미구현

HBase는 Region 크기가 임계값(기본 10GB)을 초과하면 자동 split하고,
Load Balancer가 RegionServer 간 Region 수를 균등하게 유지한다.

actorbase는 Split/Migrate RPC는 구현됐으나 트리거 로직이 없다. 운영자가 수동 호출한다.

### 요약: actorbase가 HBase에서 빌린 것과 단순화한 것

| 항목 | HBase | actorbase |
|---|---|---|
| 복제 없이 shared storage 의존 | HDFS | S3 / NFS |
| WAL + 상태 분리 | HLog + HFile | WALStore + CheckpointStore |
| Range 파티셔닝 + split | Region split | Partition split |
| Master-Worker 구조 | HMaster + RegionServer | PM + PS |
| 클러스터 조율 | ZooKeeper | etcd |
| Checkpoint 방식 | LSM compaction (점진적) | full snapshot (단순) |
| Master HA | Active/Standby | 미구현 (SPOF) |
| 자동 rebalancing | 내장 | 미구현 |
| 서버 측 커스텀 로직 | Coprocessor (제한적) | Actor 인터페이스 (자유도 높음) |

---

## actorbase 장점

### 1. Actor = Partition: 로직과 상태의 공존

```
일반 KV 저장소:  클라이언트 → KV store (get) → 클라이언트에서 연산 → KV store (put)
actorbase:       클라이언트 → Actor.Receive() (Actor가 직접 연산 + 상태 변경)
```

복잡한 도메인 연산(조건부 업데이트, 파생 상태 계산 등)을 네트워크 라운드트립 없이 처리한다.
Actor가 자신의 상태에 대한 유일한 소유자이므로 경쟁 조건이 구조적으로 없다.

### 2. 단일 스레드 Actor — lock-free sequential processing

각 파티션이 독립적인 goroutine mailbox로 동작한다. 동일 파티션 내 요청은 자연히 직렬화되며,
mutex나 MVCC 없이 강한 일관성이 보장된다. 파티션 간 병렬도는 파티션 수에 비례한다.

### 3. 유연한 저장소 전략

WALStore와 CheckpointStore를 인터페이스로 추상화했다.

- WALStore: 로컬 파일 / Redis Streams / Kafka 등 선택 가능
- CheckpointStore: S3 / NFS / GCS 등 distributed fs 선택 가능

사용자가 durability 요구사항에 맞게 저장소를 선택한다.

### 4. 도메인 특화 플랫폼

Actor 인터페이스를 구현하면 임의의 도메인 로직을 분산 상태 머신으로 구동할 수 있다.
범용 KV 위에 도메인 로직을 올리는 방식 대비 레이어가 줄어들고 일관성 보장이 명확하다.

### 5. SDK → PS 다이렉트 라우팅

PM이 라우팅 테이블을 SDK에 push하므로, SDK는 파티션 소유 PS에 직접 요청을 보낸다.
proxy 노드가 없어 홉이 줄어든다.

---

## actorbase 한계 (현재 구현 기준)

### A. 복제 없음 — distributed fs 의존

**현황**: 복제 대신 shared CheckpointStore(S3/NFS)로 장애를 대비한다.

```
Redis Cluster: primary 장애 시 replica가 수 초 내 자동 승격
actorbase:     PS 장애 시 PM이 failover 실행 → CheckpointStore에서 마지막 checkpoint + WAL replay
```

**한계**:
- CheckpointStore(S3/NFS) 자체가 장애나면 복원 불가
- fs WAL(로컬 디스크) 사용 시 실제 장비 장애(하드웨어 고장, OS 크래시)에 의한 데이터 유실 가능. 단, WAL 디렉토리를 공유 스토리지(NFS 등)에 두면 프로세스 종료 수준의 장애는 완전 복원 가능.
- failover 시간이 etcd TTL + PreparePartition 시간에 비례 (수 초~수십 초)

**완화**: networked WALStore(Redis Streams) 사용 시 WAL 유실 없음. CheckpointStore는 S3처럼 고가용성 분산 fs 사용 권장.

---

### B. PM 단일 장애점 (SPOF)

**현황**: PM이 단일 인스턴스. PM 다운 시 라우팅 테이블 갱신 불가, rebalance 불가.

```
TiKV: PD leader election으로 HA
etcd: Raft로 HA
actorbase PM: 단일 인스턴스, HA 미구현
```

**영향**: PM이 다운되면 기존 SDK 인스턴스는 캐시된 라우팅 테이블로 요청을 계속 처리하나,
새 SDK 인스턴스 시작이나 라우팅 변경(failover 포함)이 불가능해진다.

**향후 과제**: Raft 기반 PM leader election 또는 etcd leader lock으로 HA 구현.

---

### C. 자동 rebalancing 미구현

**현황**: Split과 Migrate RPC는 구현됐으나, 부하 기반 자동 rebalancing이 없다.
`AutoRebalancePolicy`의 `OnNodeJoined`, `checkAndSplit`이 stub 상태.

```
TiKV:  Region이 크기 임계값 초과 시 자동 split + 자동 이동
actorbase: 운영자가 abctl로 수동 split / migrate 호출
```

**향후 과제**:
- PS가 파티션 크기/요청량 메트릭을 PM에 보고
- PM AutoRebalancePolicy가 임계값 초과 파티션 자동 split
- 노드 추가 시 파티션 분산 자동 migrate

---

### D. Incremental Snapshot 미지원

**현황**: checkpoint는 항상 full state 직렬화. 상태가 크면 checkpoint 비용이 높아 드물게 찍게 되고 WAL replay 시간이 길어진다.

**완화**: split threshold로 파티션 크기를 제한하면 full snapshot 비용도 제한된다. 현재 설계 목표 규모에서는 허용 가능한 것으로 판단.

**향후 과제**: `Actor.SnapshotDelta(sinceVersion)` + `Actor.ApplyDelta(delta)` 인터페이스 추가.

---

### E. 파티션 간 트랜잭션 없음

파티션 내부(단일 Actor)는 직렬 처리로 일관성이 보장되나, 여러 파티션에 걸친 원자적 연산은 없다.

**적합한 사용 사례**: 파티션 경계 내에서 완결되는 연산 (e.g., 단일 object의 metadata 읽기/쓰기).
**부적합한 사용 사례**: 여러 키에 걸친 ACID 트랜잭션이 필요한 경우.

---

### F. 인메모리 캐싱 없음

Actor는 필요 시(on-demand) checkpoint에서 활성화된다. 자주 접근되는 Actor가 evict되면 재활성화 비용이 발생한다.

**현황**: `engine.Config.MaxActors` 초과 시 LRU evict. evict된 Actor의 다음 요청은 checkpoint + WAL replay 비용 발생.

---

## 용도 적합성 요약

| 요구사항 | 적합도 |
|---|---|
| 도메인 특화 상태 머신 (커스텀 Actor 로직) | **최적** |
| 파티션 내 강한 일관성 | **최적** |
| 단일 파티션 내 복잡 연산 (lock-free) | **최적** |
| S3/NFS 기반 shared CheckpointStore 활용 | **적합** |
| 범용 KV CRUD | 가능하나 과잉 설계 |
| 고가용성 (PM SPOF 허용 불가) | **현재 미지원** |
| 자동 rebalancing 필요 | **현재 미지원** |
| 파티션 간 ACID 트랜잭션 | **미지원 (설계 외)** |
| 고빈도 쓰기 + 메모리 캐싱 | Redis Cluster 권장 |
| 대규모 범용 KV (수천 노드) | Cassandra / TiKV 권장 |

---

## 알려진 한계 요약 (로드맵)

| 한계 | 심각도 | 향후 과제 |
|---|---|---|
| PM SPOF | 높음 | Raft 기반 PM HA |
| 자동 rebalancing 없음 | 중간 | AutoRebalancePolicy 구현, 메트릭 수집 |
| 복제 없음 (distributed fs 의존) | 중간 | networked WAL 권장, CheckpointStore HA 운영 |
| Incremental snapshot 없음 | 낮음 | Actor 인터페이스 확장 (향후 검토) |
| PS etcd watch 확장성 (1,000+ 노드) | 낮음 | PM이 PS에도 gRPC push 방식으로 전환 |
| PM RoutingTableStore 동시성 없음 | 낮음 | PM HA 구현 시 etcd CAS transaction 추가 |
