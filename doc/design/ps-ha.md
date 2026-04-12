# PS HA (High Availability)

## 현재 구현 (PM-direct Heartbeat 기반)

PS는 PM에 직접 Heartbeat RPC를 1초마다 전송한다. etcd Lease TTL 방식은 제거됐다.

```
정상 운영:
  PS → PM Heartbeat RPC (1초마다)
  PM: HeartbeatTimeout(5s) 초과 → handleNodeLeft() 즉시 호출

SIGKILL RTO: HeartbeatTimeout(5s) + walFlushMargin(3s) + WAL replay 시간 ≈ 8s+
SIGTERM RTO: PS가 EvictionComplete RPC 전송 → PM이 즉시 handleNodeLeft() → ~수백ms
```

walFlushMargin은 dead PS가 WAL flush를 완료했는지 알 수 없어 안전을 위해 기다리는 시간이다.
WAL flush 완료 신호(EvictionComplete)를 수신하면 이 대기를 건너뛴다.

---

## Primary-Replica WAL Replication

### 핵심 아이디어

WAL stream 키가 `{wal:{partitionID}}:stream`으로 **partitionID 기반**이다.
Primary PS가 바뀌어도 replica는 동일한 Redis Stream을 계속 읽으면 된다.
별도 복제 프로토콜 없이 Redis가 복제 버퍼 역할을 한다.

```
[Primary PS]        [Redis Streams]       [Replica PS]
  │                       │                    │
  ├─ Actor.Apply()        │                    │
  ├─ WAL.AppendBatch() ──▶│                    │
  │                       │◀─── XRANGE ────────┤
  │                       │     (100ms 폴링)    ├─ actor.Replay()
  │                       │                    │  (메모리 상태 유지)
```

### 3가지 역할 전환 오퍼레이션

```
PrepareReplica (FollowWAL)  cold path  새 PS를 replica로 처음 배정
                            checkpoint load + WAL replay → followLoop 시작

PreparePartition            warm path  replica PS를 primary로 승격
                            follower.actor 재사용 → mailbox 시작 (WAL replay 없음)

DemoteToReplica             warm path  기존 primary를 replica로 전환
                            mailbox 정지 → confirmedLSN부터 followLoop 시작
                            (checkpoint load 없음 — actor가 메모리에 이미 있음)
```

### Role Change (역할 교환)

Primary(PS_A) ↔ Replica(PS_B) 역할을 교환하는 복합 오퍼레이션.
Migration과 Drain의 핵심 메커니즘.

```
doRoleChange(partitionID, fromPrimary=PS_A, toReplica=PS_B):

  1. PreparePartition(PS_B)
       PS_B: followLoop 정지 → lag catch-up → startMailbox(warm actor)
       소요: ~100ms (replica lag만큼)

  2. DemoteToReplica(PS_A)
       PS_A: mailbox 드레인 → WAL flush → confirmedLSN부터 followLoop 시작
       소요: ~수십ms (mailbox drain)

  3. RT 업데이트: NodeID=PS_B, ReplicaNodeID=PS_A
```

---

## Failover RTO 비교

| 경로 | 소요 시간 | 비고 |
|------|----------|------|
| replica 없음 (cold) | 5s + 3s + WAL replay | 기존 방식 |
| replica 있음 (warm) | 5s + ~100ms | walFlushMargin 스킵 |
| SIGTERM (graceful) | ~수백ms | EvictionComplete + walFlushMargin 스킵 |

감지 시간(HeartbeatTimeout 5s)은 두 경로 공통이다.
replica가 있으면 warm promotion으로 walFlushMargin(3s) + WAL replay를 제거한다.

---

## 기존 운영 기능과의 통합

| 기능 | 기존 방식 | Replica 도입 후 |
|------|----------|----------------|
| **Migration** | ExecuteMigrateOut → cold PreparePartition (수백ms~수초) | target이 replica이면 doRoleChange (~100ms) |
| **Node Drain** | 파티션마다 cold migration 반복 | 파티션마다 doRoleChange (warm) |
| **Failover (dead node)** | round-robin + cold PreparePartition | replica 있으면 warm promotion |
| **Failover (replica 없음)** | round-robin + cold PreparePartition | 기존 그대로 |

---

## 생명주기 시나리오

### 1. Replica 신규 배정

```
트리거: handleNodeJoined / handleNodeLeft 완료 후 / split 완료 후

PM: assignReplicas()
  → ReplicaNodeID 없는 파티션에 다른 Active PS 선정
  → PrepareReplica(replicaPS, partitionID, actorType, ...)

replica PS: FollowWAL(ctx, partitionID)
  1. restoreFromCheckpoint()   → 공유 CheckpointStore에서 최신 checkpoint 로드
  2. replayWAL()               → checkpoint LSN+1부터 현재까지 WAL replay
  3. followLoop goroutine 시작 → 100ms 폴링, 새 항목 apply

PM: RouteEntry.ReplicaNodeID = replicaPS.ID 저장
```

catch-up 완료 전까지 해당 파티션은 replica 미비 상태. v1: 로그만 기록.

---

### 2. Failover (Primary 장애 → Replica 승격)

```
PM: HeartbeatTimeout(5s) → handleNodeLeft(PS_A)

waitForEviction:
  모든 dead 파티션에 ReplicaNodeID 있음 → walFlushMargin 스킵

failoverDeadNode(PS_A):
  entry.ReplicaNodeID="PS_B" → fast path:
    Migrator.Failover(P, PS_B):
      PreparePartition(PS_B)
        PS_B.doActivate(): followers["P"] 존재
          doPromote():
            follower.cancel() + <-done (followLoop 종료)
            ReadFrom(lastLSN+1)  → lag catch-up
            startMailbox(follower.actor)  ← warm actor, ~100ms
  RT: NodeID=PS_B, ReplicaNodeID=""
  go assignReplicas()  → PS_C를 새 replica로 배정
```

---

### 3. Role Change (Migration / Drain)

```
PM: doRoleChange(P, fromPrimary=PS_A, toReplica=PS_B)

Step 1: PreparePartition(PS_B)  → warm promotion (위 Failover 경로와 동일)

Step 2: DemoteToReplica(PS_A, P)
  PS_A.DemoteToReplica(ctx, P):
    actorEntry = actors["P"]
    mailbox.stop()           → 드레인 + WAL flush
    confirmedLSN = mailbox.confirmedLSN.Load()
    f = replicaFollower{actor: actorEntry.actor, lastLSN: confirmedLSN}
    actors → followers 이동
    go followLoop(f, P)      ← warm, checkpoint load 없음

Step 3: RT: NodeID=PS_B, ReplicaNodeID=PS_A
```

**Migration**: target이 replica인 경우 doRoleChange 사용. 아닌 경우 기존 cold path 유지.
**Drain**: 각 primary 파티션마다 doRoleChange 후 RemoveReplica로 정리.

---

### 4. Split 시 replica 동작

```
Before: P [a,z) → primary: PS_A, replica: PS_B

ExecuteSplit(PS_A, P, "m", P_new):
  P_lower [a,m): partitionID=P, WAL stream 동일 → PS_B follower 자동 유지
  P_upper [m,z): partitionID=P_new, WAL stream 신규 → replica 없음

buildSplitTable:
  P_lower.ReplicaNodeID = PS_B  (유지)
  P_upper.ReplicaNodeID = ""    (신규 배정 필요)

split 완료 후 assignReplicas() → P_upper에 replica 배정
```

P_lower는 WAL stream key가 동일하므로 PS_B follower가 자동으로 P_lower replica가 된다.

---

### 5. Merge 시 replica 동작

```
Before:
  P_lower → primary: PS_A, replica: PS_B
  P_upper → primary: PS_A, replica: PS_B

Merge:
  1. RemoveReplica(PS_B, P_upper) → PS_B의 P_upper follower 정리
  2. ExecuteMerge(PS_A, P_lower, P_upper)
     → P_lower actor: Import(P_upper state)
     → P_upper WAL: TrimBefore(max) (전체 삭제)
  3. RT: P_upper 항목 제거, P_lower 유지 (ReplicaNodeID=PS_B)

PS_B의 P_lower follower: merge 결과도 WAL에 기록 → 자동 반영
```

---

### 6. Replica 장애 → 재구축

```
PS_B 장애 → PM: HeartbeatTimeout → handleNodeLeft(PS_B)
  failoverDeadNode(PS_B)  → PS_B primary 파티션 처리 (없으면 no-op)
  go assignReplicas():
    P: ReplicaNodeID="PS_B", isActiveNode("PS_B")=false
    → ReplicaNodeID="" 초기화
    → pick PS_C (Active, 다른 노드)
    → PrepareReplica(PS_C, P, ...)  → cold FollowWAL
    → RouteEntry.ReplicaNodeID = "PS_C"
```

`handleNodeLeft`에서 항상 `assignReplicas()`를 호출하므로
"primary 파티션 없이 replica만 가진 노드"의 장애도 처리된다.

---

## WAL Trimming과 Replica

현재: checkpoint 완료 → TrimBefore(checkpointLSN) 즉시 호출.

Replica가 TrimBefore 이전 WAL을 아직 읽지 못한 경우 gap이 발생할 수 있다.
gap 감지 시 replica는 checkpoint를 재로드하여 self-healing한다:

```
followLoop:
  ReadFrom(lastLSN+1)
    entries[0].LSN > lastLSN+1 → gap 감지
    → reloadFollower(): checkpoint 재로드 + WAL replay
    → 정상 following 재개

gap reload 3회 초과 시 follower 중단.
PM이 heartbeat lag로 감지 후 assignReplicas()로 재배정.
```

Primary의 TrimBefore 타이밍은 변경하지 않는다 (v1).
checkpoint 주기를 짧게 설정하면 gap 재로드 비용이 작아진다.

---

## 권장 배치: 3대 구성

```
Machine-1: PS-1
Machine-2: PS-2
Machine-3: PS-3

partition A → primary: PS-1, replica: PS-2
partition B → primary: PS-2, replica: PS-3
partition C → primary: PS-3, replica: PS-1
```

PM이 `assignReplicas()`에서 primary와 다른 노드를 자동 선택하므로
운영자가 직접 배치를 설정할 필요가 없다.

장비 1대 장애 시:
- 해당 장비의 primary 파티션: replica가 있는 다른 장비에서 warm promotion
- 해당 장비의 replica 파티션: 남은 2대 중 1대에 cold re-assignment

---

## 구현에 필요한 변경

| 항목 | 변경 내용 |
|------|-----------|
| `RouteEntry` | `ReplicaNodeID string` 추가 |
| `actorbase.proto` | `PrepareReplica`, `RemoveReplica`, `DemoteToReplica` RPC 추가 |
| `HeartbeatRequest` | `replica_positions map<partitionID, lastLSN>` 추가 (모니터링용) |
| `ActorHost` | `replicaFollower` 구조체, `FollowWAL`, `DemoteToReplica`, `StopFollowWAL`, `doPromote` |
| PS control handler | PrepareReplica, RemoveReplica, DemoteToReplica 핸들러 |
| PM server | `assignReplicas()`, `doRoleChange()`, failover fast path, walFlushMargin skip |
| migrator | `buildMigratedTable(clearReplicaNodeID bool)` |
| splitter | buildSplitTable — P_upper.ReplicaNodeID="" |
| merger | Merge 전 RemoveReplica(P_upper) 호출 |
