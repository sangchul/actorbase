# PS HA (High Availability)

현재 actorbase의 PS는 단일 노드에 파티션을 보유한다. PS가 죽으면 PM이 감지한 뒤
다른 PS로 파티션을 failover하지만, 이 과정에서 **10~30초의 다운타임**이 발생한다.
이 문서는 다운타임을 줄이기 위한 두 가지 개선 방향을 기술한다.

---

## 1. etcd Lease TTL 단축

### 현재 상황

PS는 etcd에 heartbeat lease를 유지한다. PM은 lease가 만료되는 것을 감지해
노드 장애를 인식한다. 현재 TTL은 **10~15초**이며, 이것이 failover 전체 시간의
대부분을 차지한다.

```
PS 죽음 → lease 만료 대기 (10~15초) → PM 감지 → failover 시작
```

### 개선 방향

TTL을 줄이면 감지 시간이 단축된다.

| TTL | 감지 시간 | 트레이드오프 |
|---|---|---|
| 10~15초 (현재) | 10~15초 | 네트워크 순단에 안전 |
| 3~5초 | 3~5초 | 짧은 네트워크 순단 시 오탐 가능 |
| 1~2초 | 1~2초 | etcd에 heartbeat 부하 증가 |

### 보완책: PS 간 직접 헬스체크

etcd lease 만료를 기다리지 않고, PM이 PS에 직접 ping을 보내 장애를 조기 감지하는
방식을 병행할 수 있다. lease TTL은 안전망으로 유지하고, 직접 헬스체크로 감지 시간을
단축한다.

```
PM → PS gRPC ping (1초 주기)
  응답 없음 3회 연속 → 장애로 판정 → failover 시작
  (lease 만료 전에 선제 대응)
```

---

## 2. Primary-Replica WAL Replication

### 현재 failover 흐름 (lease 만료 이후)

```
PM: 장애 감지
  → target PS 선택
  → target PS: checkpoint store에서 전체 상태 로드   ← 데이터 크기에 비례
  → target PS: checkpoint 이후 WAL 전체 replay       ← 미처리 엔트리 수에 비례
  → routing table 업데이트 → SDK 전파
  → 서비스 재개

소요 시간: 200ms ~ 수 초 (예측 불가)
```

파티션 수가 많을수록 복구 시간이 곱해진다.

### 핵심 아이디어

현재 WAL은 Redis Streams에 저장된다. Replica PS가 **같은 Redis Streams를
consumer group으로 구독**하면, primary의 변경 사항을 실시간으로 따라잡을 수 있다.
Primary가 replica를 직접 관리할 필요 없이, Redis가 복제 버퍼 역할을 한다.

```
[Primary PS]        [Redis Streams]       [Replica PS]
  │                       │                    │
  ├─ Actor.Apply()        │                    │
  ├─ WAL.AppendBatch() ──▶│                    │
  │                       │◀─ consumer group ──┤
  │                       │   알아서 읽어감        ├─ Actor replay
  │                       │                    │  (메모리 상태 유지)
```

### Replica 방식의 failover 흐름

```
PM: 장애 감지
  → routing table에서 replica PS 선택
  → replica PS: 메모리에 Actor 상태 이미 존재     ← 0 비용
  → replica PS: Redis에서 미처리 WAL만 consume   ← lag만큼만
  → routing table 업데이트 → SDK 전파
  → 서비스 재개

소요 시간: 10ms ~ 100ms (lag에 의존, 예측 가능)
```

### 두 방식 비교 (lease 만료 이후 기준)

| factor | 현재 방식 | Replica 방식 |
|---|---|---|
| Actor 상태 구축 | checkpoint 전체 로드 + map 구축 | 이미 메모리에 있음 (0) |
| 복구 데이터 소스 | checkpoint store (디스크/S3) | Redis Streams (메모리) |
| replay 대상 | checkpoint 이후 전체 WAL | replica lag만 (수십~수백 entries) |
| 소요 시간 범위 | 200ms ~ 수 초 | 10ms ~ 100ms |
| 예측 가능성 | 낮음 (데이터 크기 의존) | 높음 (lag은 항상 작게 유지) |
| 파티션 수 영향 | 파티션 수만큼 곱해짐 | 거의 없음 |

### WAL 삭제 조건 변경

현재는 checkpoint 완료 후 WAL을 삭제한다. Replica 방식에서는 replica가 따라잡은
것을 확인한 뒤 삭제해야 한다.

```
현재: checkpoint 완료 → WAL 삭제
변경: checkpoint 완료 + replica consumer offset 확인 → WAL 삭제
```

Redis Streams의 consumer group offset으로 replica가 어디까지 읽었는지
PM이 확인할 수 있다.

### 구현에 필요한 변경

| 항목 | 변경 내용 |
|---|---|
| PM routing table | 파티션별 `primary` / `replica` PS 목록 추가 |
| Replica PS | 외부 요청 처리 안 함, WAL consume + Actor 상태 유지만 |
| WAL 삭제 조건 | replica ACK 확인 후 삭제 |
| Failover | PM이 replica를 primary로 승격 + routing table 업데이트 |
| Split-brain 방지 | PM이 etcd epoch 번호로 primary 권한 중재 |

### 복제 단위와 Actor의 관계

복제의 단위는 **PS 노드**이며, Actor는 복제를 인식하지 않는다.
Actor는 PS 안의 처리 단위일 뿐이고, PS가 보유한 모든 파티션의 WAL이
replica PS로 복제된다. Actor 간에 별도의 replication 관계는 없다.

```
[PS-1 (primary)]          [PS-2 (replica)]
  partition A  ──WAL──▶   partition A (copy)
  partition B  ──WAL──▶   partition B (copy)
  Actor-1, Actor-2         Actor-1, Actor-2 (same state)
```

### 권장 배치: 3대 구성

장비 3대에 PS를 하나씩 두고, PM이 파티션별로 primary/replica를 분산 배정한다.
**replica는 항상 primary와 다른 장비에** 배치한다는 규칙만 지키면 된다.

```
Machine-1: PS-1
Machine-2: PS-2
Machine-3: PS-3

partition A -> primary: PS-1, replica: PS-2
partition B -> primary: PS-2, replica: PS-3
partition C -> primary: PS-3, replica: PS-1
```

장비 1대가 죽으면 그 장비의 primary 파티션들은 replica가 있는 다른 장비에서
즉시 승격된다. PM이 배치 규칙을 자동으로 적용하므로 운영자가 직접 설정할
필요가 없다.

### 장애 복구 및 신규 장비 투입 절차

**장애 발생 시 (자동)**

```
1. PM detects PS-1 failure
2. PM promotes replica PS for each partition owned by PS-1
   (already in memory → immediate)
3. PM updates routing table → SDK picks up new routes
4. Service resumes
```

**새 장비 투입 (수동)**

```
5. abctl node remove ps-1        # remove Failed node from catalog
6. abctl node add ps-4 <addr>    # register new node as Waiting
7. start kv_server on new machine → RequestJoin → Active
8. PM assigns ps-4 as replica for partitions that lost their replica
   → ps-4 consumes WAL from Redis Streams to rebuild state
   → once caught up, replica is ready
```

| 단계 | 현재 방식 | Replica 방식 |
|---|---|---|
| 장애 → 서비스 재개 | 수동 failover + WAL replay | 자동 승격, 즉시 |
| 새 장비 투입 | 파티션 migrate 필요 | replica 배정 후 WAL 따라잡기 |
| 새 장비 준비 시간 | migrate 완료까지 | WAL 전체 consume까지 (데이터 양에 비례) |

**신규 replica 합류 최적화**

새 PS가 WAL을 처음부터 전부 replay하면 시간이 오래 걸린다.
primary의 최신 checkpoint를 복사해서 초기 상태를 구축한 뒤,
checkpoint 이후 WAL만 consume하면 시간을 크게 단축할 수 있다.
MySQL의 `mysqldump` + binlog 방식과 동일한 패턴이다.

```
New replica joins:
  1. Copy latest checkpoint from primary -> restore initial state
  2. Consume only WAL entries after checkpoint seq
  3. Replica ready
```

replica가 구축 완료되기 전까지 해당 파티션은 replica가 1개 미만인 상태로
운영된다. 이 기간 중 추가 장애 시 데이터 손실 위험이 있으므로, PM은
"replica 구축 중" 상태를 routing table에 표시하고 완료 전까지 해당 파티션의
split/migrate를 막아야 한다.

### 현재 구조와의 적합성

actorbase는 이미 Redis Streams WAL, PM routing table, etcd 기반 PM HA를 갖추고 있다.
구조적으로 replica 방식을 수용할 준비가 되어 있으며, 별도의 복제 프로토콜 없이
기존 WAL을 복제 로그로 재활용할 수 있다는 것이 핵심이다.
