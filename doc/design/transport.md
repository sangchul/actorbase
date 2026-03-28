# internal/transport 패키지 설계

gRPC 기반 노드 간 통신. proto 정의, 공통 서버/클라이언트 팩토리를 제공한다.
PS·PM·SDK 각각의 비즈니스 로직 핸들러는 `ps/`, `pm/`, `sdk/` 패키지에 구현한다.

의존성: `internal/domain`, `provider`, `google.golang.org/grpc`

파일 목록:
- `proto/actorbase.proto` — 모든 gRPC 서비스·메시지 정의
- `proto/` (generated)    — protoc 생성 코드
- `server.go`             — 공통 gRPC 서버 팩토리
- `client.go`             — 타입별 gRPC 클라이언트 + 커넥션 풀

---

## Codec (provider/codec.go)

Actor의 Req/Resp 타입을 bytes로 직렬화/역직렬화한다. SDK와 PS 양쪽에 동일한 구현체를 주입해야 한다. `adapter/json`에 기본 JSONCodec 구현을 제공한다.

---

## gRPC 서비스 구조

통신 경로에 따라 세 개의 서비스로 구분한다.

```
┌─────────┐  PartitionService      ┌─────────┐
│   SDK   │ ──── Send ──────────▶  │   PS    │
└─────────┘                        └─────────┘
     │                                  ▲
     │  PartitionManagerService    PartitionControlService
     ▼                                  │
┌─────────┐ ─── ExecuteSplit ──────────▶│
│   PM    │ ─── ExecuteMigrateOut ─────▶│
│         │ ─── ExecuteMerge ──────────▶│
└─────────┘
     ▲
     │  PartitionManagerService
┌─────────┐
│  abctl  │ ─── RequestSplit / RequestMigrate / AddNode / RemoveNode / ResetNode
└─────────┘
     ▲
     │  PartitionManagerService (RequestJoin / SetNodeDraining)
┌─────────┐
│   PS    │
└─────────┘
```

| 서비스 | 노출 주체 | 호출 주체 | 역할 |
|---|---|---|---|
| `PartitionService` | PS | SDK | Actor에 요청 전달 (data plane) |
| `PartitionManagerService` | PM | SDK, abctl, PS | 라우팅 push, 멤버십 관리, split/migrate 요청 (management plane) |
| `PartitionControlService` | PS | PM | split/migrate 실행 명령 (control plane) |

---

## proto/actorbase.proto

```protobuf
syntax = "proto3";
package actorbase.v1;
option go_package = "github.com/sangchul/actorbase/internal/transport/proto;actorbasepb";

// ─── Data Plane: SDK → PS ────────────────────────────────
service PartitionService {
  rpc Send(SendRequest)   returns (SendResponse);
  rpc Scan(ScanRequest)   returns (ScanResponse);
}
message SendRequest {
  string partition_id = 1;
  bytes  payload      = 2; // Codec.Marshal(req)
  string actor_type   = 3;
}
message SendResponse {
  bytes payload = 1;
}
message ScanRequest {
  string partition_id              = 1;
  bytes  payload                   = 2; // Codec.Marshal(req)
  string actor_type                = 3;
  string expected_key_range_start  = 4; // SDK가 알고 있는 파티션 범위 (stale routing 감지용)
  string expected_key_range_end    = 5;
}
message ScanResponse {
  bytes payload = 1;
}

// ─── Management Plane: SDK/abctl/PS → PM ─────────────────
service PartitionManagerService {
  rpc WatchRouting(WatchRoutingRequest)       returns (stream RoutingTableProto);
  // 멤버십 관리 (PS lifecycle)
  rpc RequestJoin(RequestJoinRequest)         returns (RequestJoinResponse);     // PS 시작 시 호출. Waiting → Active.
  rpc SetNodeDraining(SetNodeDrainingRequest) returns (SetNodeDrainingResponse); // PS SIGTERM 시 호출. Active → Draining.
  // 노드 사전 등록 / 운영 명령 (abctl)
  rpc AddNode(AddNodeRequest)                 returns (AddNodeResponse);         // Waiting 상태로 신규 등록
  rpc RemoveNode(RemoveNodeRequest)           returns (RemoveNodeResponse);      // Waiting/Failed 노드 삭제
  rpc ResetNode(ResetNodeRequest)             returns (ResetNodeResponse);       // Failed → Waiting (수동 복구)
  rpc ListMembers(ListMembersRequest)         returns (ListMembersResponse);
  // 파티션 조작 (abctl/PM 내부)
  rpc RequestSplit(SplitRequest)              returns (SplitResponse);
  rpc RequestMigrate(MigrateRequest)          returns (MigrateResponse);
  rpc RequestMerge(MergeRequest)              returns (MergeResponse);
  // 통계 / 정책
  rpc GetClusterStats(GetClusterStatsRequest) returns (GetClusterStatsResponse);
  rpc ApplyPolicy(ApplyPolicyRequest)         returns (ApplyPolicyResponse);
  rpc GetPolicy(GetPolicyRequest)             returns (GetPolicyResponse);
  rpc ClearPolicy(ClearPolicyRequest)         returns (ClearPolicyResponse);
}
message RequestJoinRequest  { string node_id = 1; string address = 2; }
message RequestJoinResponse {}
message SetNodeDrainingRequest  { string node_id = 1; }
message SetNodeDrainingResponse {}
message AddNodeRequest  { string node_id = 1; string address = 2; }
message AddNodeResponse {}
message RemoveNodeRequest  { string node_id = 1; }
message RemoveNodeResponse {}
message ResetNodeRequest  { string node_id = 1; }
message ResetNodeResponse {}
message WatchRoutingRequest { string client_id = 1; }
message SplitRequest   { string partition_id = 1; string split_key = 2; string actor_type = 3; }
message SplitResponse  { string new_partition_id = 1; }
message MigrateRequest { string partition_id = 1; string target_node_id = 2; string actor_type = 3; }
message MigrateResponse {}
message MergeRequest { string actor_type = 1; string lower_partition_id = 2; string upper_partition_id = 3; }
message MergeResponse {}
message ListMembersRequest {}
message MemberInfo     { string node_id = 1; string address = 2; NodeStatus status = 3; }
message ListMembersResponse { repeated MemberInfo members = 1; }
// GetClusterStats: PM이 모든 PS에 GetStats를 병렬 호출하여 집계 후 반환
message GetClusterStatsRequest  { string node_id = 1; } // 빈 문자열이면 전체
message GetClusterStatsResponse { repeated NodeStatsProto nodes = 1; }
message NodeStatsProto {
  string node_id   = 1; string node_addr = 2; double node_rps = 3;
  int32  partition_count = 4; repeated PartitionStatsProto partitions = 5;
}
// ApplyPolicy: YAML raw string을 PM에 전송 → AutoPolicy 활성화
message ApplyPolicyRequest  { string policy_yaml = 1; }
message ApplyPolicyResponse {}
message GetPolicyRequest  {}
message GetPolicyResponse { string policy_yaml = 1; bool active = 2; }
message ClearPolicyRequest  {}
message ClearPolicyResponse {}

// ─── Control Plane: PM → PS ──────────────────────────────
service PartitionControlService {
  rpc ExecuteSplit(ExecuteSplitRequest)           returns (ExecuteSplitResponse);
  rpc ExecuteMigrateOut(ExecuteMigrateOutRequest) returns (ExecuteMigrateOutResponse);
  rpc PreparePartition(PreparePartitionRequest)   returns (PreparePartitionResponse);
  // ExecuteMerge는 PM이 PS에게 두 파티션의 merge를 명령한다.
  rpc ExecuteMerge(ExecuteMergeRequest)           returns (ExecuteMergeResponse);
  rpc GetStats(GetStatsRequest)                   returns (GetStatsResponse);
}
message ExecuteSplitRequest {
  string partition_id     = 1;
  string split_key        = 2; // 비어 있으면 PS가 SplitHinter 또는 midpoint로 결정
  string new_partition_id = 3;
  string actor_type       = 4;
  string key_range_start  = 5; // split_key 미제공 시 PS의 midpoint 계산용
  string key_range_end    = 6; // split_key 미제공 시 PS의 midpoint 계산용
}
message ExecuteSplitResponse {
  string split_key = 1; // PS가 실제 사용한 split key (Splitter가 라우팅 테이블 갱신에 사용)
}
message ExecuteMigrateOutRequest {
  string partition_id   = 1;
  string target_node_id = 2;
  string target_address = 3;
  string actor_type     = 4;
}
message ExecuteMigrateOutResponse {}
message PreparePartitionRequest {
  string partition_id    = 1;
  string key_range_start = 2;
  string key_range_end   = 3;
  string actor_type      = 4;
}
message PreparePartitionResponse {}
message ExecuteMergeRequest {
  string actor_type          = 1;
  string lower_partition_id  = 2;
  string upper_partition_id  = 3;
}
message ExecuteMergeResponse {}
// GetStats: PS 노드 전체 통계 조회 (파티션별 RPS, key count)
message GetStatsRequest {}
message GetStatsResponse {
  repeated PartitionStatsProto partitions    = 1;
  double                       node_rps      = 2;
  int32                        partition_count = 3;
}
message PartitionStatsProto {
  string partition_id = 1; string actor_type = 2;
  int64  key_count    = 3; // -1이면 Countable 미구현
  double rps          = 4; // 최근 60초 슬라이딩 윈도우 평균
}

// ─── Shared ───────────────────────────────────────────────
message RoutingTableProto {
  int64                    version = 1;
  repeated RouteEntryProto entries = 2;
}
message RouteEntryProto {
  string     partition_id    = 1;
  string     key_range_start = 2;
  string     key_range_end   = 3; // "" = 상한 없음
  string     node_id         = 4;
  string     node_address    = 5;
  NodeStatus node_status     = 6;
  string     actor_type      = 7;
}
enum NodeStatus {
  NODE_STATUS_WAITING  = 0;
  NODE_STATUS_ACTIVE   = 1;
  NODE_STATUS_DRAINING = 2;
  NODE_STATUS_FAILED   = 3;
}
```

---

## gRPC 에러 매핑

PS 핸들러에서 `provider` 에러를 gRPC status로 변환하고, SDK 클라이언트에서 역변환한다.

| provider 에러 | gRPC status code | SDK 동작 |
|---|---|---|
| ErrNotFound | NOT_FOUND | 호출자에게 전달 |
| ErrPartitionMoved | FAILED_PRECONDITION | 라우팅 갱신 후 재시도 |
| ErrPartitionNotOwned | UNAVAILABLE | 라우팅 갱신 후 재시도 |
| ErrPartitionBusy | RESOURCE_EXHAUSTED | 잠시 후 재시도 |
| ErrTimeout | DEADLINE_EXCEEDED | 호출자에게 전달 |
| ErrActorPanicked | INTERNAL (특정 메시지) | 호출자에게 전달 |
| 기타 서버 내부 오류 | INTERNAL | `fmt.Errorf("internal server error: %s", msg)` |

> **Scan 전용**: PS의 Scan 핸들러는 `expected_key_range`와 실제 파티션 range가 불일치하면 `ErrPartitionMoved`를 반환한다. SDK는 이를 받아 라우팅을 갱신하고 해당 범위만 재시도한다 (누락 없음 보장).

> `fromGRPCStatus`는 메시지 문자열 비교로 `ErrActorPanicked`와 기타 INTERNAL 오류를 구분한다. 이렇게 하지 않으면 "partition already on node" 같은 실제 서버 오류가 ErrActorPanicked로 오인된다.

---

## server.go / client.go

**NewGRPCServer**: panic recover, 요청 로깅, 메트릭 수집 interceptor가 적용된 `*grpc.Server`를 반환한다.

**ConnPool**: 주소별 gRPC 커넥션을 캐싱한다. SDK가 라우팅 변경 시마다 새 커넥션을 만들지 않도록 재사용한다.

**PSClient** (SDK → PS):
- `Send(ctx, actorType, partitionID, req, respPtr)` — Codec으로 직렬화/역직렬화. gRPC status는 provider error로 변환하여 반환.
- `Scan(ctx, actorType, partitionID, req, respPtr, expectedStart, expectedEnd)` — Send와 동일하나 파티션 key range 검증을 위한 `expected_key_range_start/end`를 추가로 전송.

**PMClient** (SDK/abctl/PS → PM):
- `WatchRouting`, `RequestSplit`, `RequestMigrate`, `RequestMerge`, `ListMembers` — 기존
- `RequestJoin(nodeID, addr)`, `SetNodeDraining(nodeID)` — PS lifecycle (PS → PM)
- `AddNode(nodeID, addr)`, `RemoveNode(nodeID)`, `ResetNode(nodeID)` — 노드 관리 (abctl → PM)
- `ApplyPolicy(yamlStr)`, `GetPolicy() (yamlStr, active, err)`, `ClearPolicy()` — 정책 관리 (abctl → PM)
- `GetClusterStats(nodeID) ([]NodeStats, err)` — 통계 (`nodeID` 빈 문자열이면 전체 노드)

WatchRouting 스트림이 끊기면 내부에서 자동 재연결 후 재구독한다.

**PSController** interface (PM → PS): `ExecuteSplit`, `ExecuteMigrateOut`, `PreparePartition`, `ExecuteMerge`, `GetStats`. PM이 PS에 제어 명령을 보낼 때 사용하는 인터페이스.

**PSClientFactory** interface: `GetClient(addr string) (PSController, error)`. rebalance 패키지가 PS 주소로부터 `PSController`를 얻을 때 사용. `ConnPool`을 직접 의존하지 않아 단위 테스트에서 mock 주입이 가능하다.

**NewConnPoolFactory(pool \*ConnPool) PSClientFactory**: `ConnPool` 기반 `PSClientFactory` 구현체. `pm.NewServer`에서 생성하여 Splitter/Migrator/Merger/balancerRunner에 주입한다.

### gRPC 에러 추가 매핑

| 상황 | gRPC status code | 설명 |
|---|---|---|
| AutoPolicy 활성 중 수동 split/migrate 시도 | PERMISSION_DENIED | `fromGRPCStatus`에서 raw 메시지로 반환 (에러 내용 보존) |

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| Payload 직렬화 | provider.Codec 인터페이스 | JSON/protobuf/커스텀 교체 가능. SDK·PS 양측 동일 구현체 주입. |
| 기본 Codec | adapter/json JSONCodec | 별도 설정 없이 바로 사용 가능. |
| gRPC 서비스 분리 | data / management / control 3개 | 역할·방향이 명확히 다름. |
| 에러 전달 방식 | gRPC status code | proto message에 에러 필드 없이 gRPC 표준 방식 사용. |
| WatchRouting 재연결 | PMClient 내부 자동 재연결 | SDK가 스트림 종료를 직접 처리하지 않아도 됨. |
| 커넥션 풀 | ConnPool (transport 내부) | 라우팅 변경 시마다 새 커넥션 생성 방지. |
| PSClientFactory 인터페이스 | ConnPool 위 추상화 레이어 | Splitter/Migrator/Merger/balancerRunner가 ConnPool에 직접 의존하지 않아 단위 테스트에서 mock 교체 가능. |

---

## 알려진 한계

- **Codec 불일치**: SDK와 PS에 다른 Codec이 주입되면 런타임 역직렬화 에러. 컴파일 타임에 검증 불가.
- **WatchRouting 재연결 gap**: 재연결 중 라우팅 변경이 발생하면 일시적으로 stale 라우팅을 사용할 수 있다. SDK의 재시도 로직이 보완한다.
