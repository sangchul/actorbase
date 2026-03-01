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

## provider/codec.go (신규 추가)

transport 설계에 따라 `provider` 패키지에 `Codec` 인터페이스를 추가한다.

```go
// Codec은 Actor의 Req/Resp 타입을 bytes로 직렬화/역직렬화한다.
// SDK와 PS 양쪽에 동일한 Codec 구현체를 주입해야 한다.
// 기본 구현체로 JSONCodec을 제공한다.
type Codec interface {
    Marshal(v any) ([]byte, error)
    Unmarshal(data []byte, v any) error
}
```

`adapter/json/codec.go`에 기본 구현체를 제공한다.

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
└─────────┘
     ▲
     │  PartitionManagerService
┌─────────┐
│  abctl  │ ─── RequestSplit / RequestMigrate
└─────────┘
```

| 서비스 | 노출 주체 | 호출 주체 | 역할 |
|---|---|---|---|
| `PartitionService` | PS | SDK | Actor에 요청 전달 (data plane) |
| `PartitionManagerService` | PM | SDK, abctl | 라우팅 push, split/migrate 요청 (management plane) |
| `PartitionControlService` | PS | PM | split/migrate 실행 명령 (control plane) |

---

## proto/actorbase.proto

```protobuf
syntax = "proto3";

package actorbase.v1;

option go_package = "actorbase/internal/transport/proto;actorbasepb";

// ─────────────────────────────────────────────────────────
// Data Plane: SDK → PS
// ─────────────────────────────────────────────────────────

service PartitionService {
  // Send는 partitionID의 Actor에게 요청을 전달하고 응답을 반환한다.
  // payload는 Codec으로 직렬화된 사용자 정의 Req 타입이다.
  // 에러는 gRPC status code로 전달한다.
  rpc Send(SendRequest) returns (SendResponse);
}

message SendRequest {
  string partition_id = 1;
  bytes  payload      = 2; // Codec.Marshal(req)
}

message SendResponse {
  bytes payload = 1; // Codec.Marshal(resp)
}

// ─────────────────────────────────────────────────────────
// Management Plane: SDK/abctl → PM
// ─────────────────────────────────────────────────────────

service PartitionManagerService {
  // WatchRouting은 연결 즉시 현재 라우팅 테이블을 전송하고,
  // 이후 변경이 생길 때마다 스트리밍으로 push한다.
  rpc WatchRouting(WatchRoutingRequest) returns (stream RoutingTableProto);

  // RequestSplit은 파티션 split을 PM에 요청한다.
  // abctl 또는 자동 rebalance 정책이 호출한다.
  rpc RequestSplit(SplitRequest) returns (SplitResponse);

  // RequestMigrate는 파티션 migration을 PM에 요청한다.
  rpc RequestMigrate(MigrateRequest) returns (MigrateResponse);
}

message WatchRoutingRequest {
  string client_id = 1; // 디버깅·로깅용 SDK 인스턴스 식별자
}

message SplitRequest {
  string partition_id = 1;
  string split_key    = 2; // 새 파티션의 시작 키
}

message SplitResponse {
  string new_partition_id = 1;
}

message MigrateRequest {
  string partition_id   = 1;
  string target_node_id = 2;
}

message MigrateResponse {}

// ─────────────────────────────────────────────────────────
// Control Plane: PM → PS
// ─────────────────────────────────────────────────────────

service PartitionControlService {
  // ExecuteSplit은 PM이 PS에게 파티션 split을 명령한다.
  rpc ExecuteSplit(ExecuteSplitRequest) returns (ExecuteSplitResponse);

  // ExecuteMigrateOut은 PM이 PS에게 파티션을 대상 노드로 이동시키도록 명령한다.
  rpc ExecuteMigrateOut(ExecuteMigrateOutRequest) returns (ExecuteMigrateOutResponse);
}

message ExecuteSplitRequest {
  string partition_id     = 1;
  string split_key        = 2;
  string new_partition_id = 3; // PM이 미리 할당한 새 파티션 ID
}

message ExecuteSplitResponse {}

message ExecuteMigrateOutRequest {
  string partition_id    = 1;
  string target_node_id  = 2;
  string target_address  = 3; // gRPC 접속 주소. PS가 직접 연결할 수 있도록 전달.
}

message ExecuteMigrateOutResponse {}

// ─────────────────────────────────────────────────────────
// Shared Messages
// ─────────────────────────────────────────────────────────

message RoutingTableProto {
  int64                   version = 1;
  repeated RouteEntryProto entries = 2;
}

message RouteEntryProto {
  string     partition_id    = 1;
  string     key_range_start = 2;
  string     key_range_end   = 3; // "" = 상한 없음
  string     node_id         = 4;
  string     node_address    = 5;
  NodeStatus node_status     = 6;
}

enum NodeStatus {
  NODE_STATUS_ACTIVE   = 0;
  NODE_STATUS_DRAINING = 1;
}
```

---

## gRPC 에러 매핑

`provider` 에러 타입을 gRPC status code로 변환한다.
PS 핸들러에서 `provider` 에러를 gRPC status로 변환하고, SDK 클라이언트에서 역변환한다.

| provider 에러 | gRPC status code | SDK 동작 |
|---|---|---|
| `ErrNotFound` | `NOT_FOUND` | 호출자에게 전달 |
| `ErrPartitionMoved` | `FAILED_PRECONDITION` | PM에서 새 라우팅 조회 후 재시도 |
| `ErrPartitionNotOwned` | `UNAVAILABLE` | 라우팅 갱신 후 재시도 |
| `ErrPartitionBusy` | `RESOURCE_EXHAUSTED` | 잠시 후 재시도 |
| `ErrTimeout` | `DEADLINE_EXCEEDED` | 호출자에게 전달 |
| `ErrActorPanicked` | `INTERNAL` | 호출자에게 전달 |

```go
// 내부 유틸리티 함수 (transport 패키지)
func toGRPCStatus(err error) error  // provider error → gRPC status
func fromGRPCStatus(err error) error // gRPC status → provider error
```

---

## server.go

PS와 PM이 공통으로 사용하는 gRPC 서버 팩토리.
비즈니스 로직 핸들러 구현은 각각 `ps/`, `pm/` 패키지에서 담당한다.

```go
// ServerConfig는 gRPC 서버 공통 설정.
type ServerConfig struct {
    ListenAddr string
    Metrics    provider.Metrics
}

// NewGRPCServer는 공통 interceptor가 적용된 *grpc.Server를 반환한다.
//
// 적용 interceptor (unary + stream):
//   - 패닉 recover: goroutine panic → INTERNAL status 변환
//   - 요청 로깅:   slog로 method, duration, status 기록
//   - 메트릭 수집: 요청 수, 에러 수, latency histogram
func NewGRPCServer(cfg ServerConfig) *grpc.Server
```

### 사용 예시 (ps/server.go)

```go
grpcSrv := transport.NewGRPCServer(transport.ServerConfig{
    ListenAddr: cfg.ListenAddr,
    Metrics:    cfg.Metrics,
})
pb.RegisterPartitionServiceServer(grpcSrv, &partitionHandler{host: actorHost})
pb.RegisterPartitionControlServiceServer(grpcSrv, &controlHandler{host: actorHost})
```

---

## client.go

### ConnPool

SDK가 여러 PS 노드에 연결할 때 커넥션을 재사용한다.

```go
// ConnPool은 주소별 gRPC 커넥션을 캐싱한다.
// SDK가 라우팅 테이블 갱신으로 새 PS 노드에 접속할 때 사용한다.
type ConnPool struct {
    mu    sync.RWMutex
    conns map[string]*grpc.ClientConn // "host:port" → conn
}

func NewConnPool() *ConnPool

// Get은 addr에 대한 커넥션을 반환한다. 없으면 새로 생성한다.
func (p *ConnPool) Get(addr string) (*grpc.ClientConn, error)

// Close는 모든 커넥션을 닫는다.
func (p *ConnPool) Close() error
```

### PSClient (SDK → PS, data plane)

```go
// PSClient는 Partition Server로 Actor 요청을 전송한다.
// SDK가 사용한다.
type PSClient struct {
    conn   *grpc.ClientConn
    client pb.PartitionServiceClient
    codec  provider.Codec
}

func NewPSClient(conn *grpc.ClientConn, codec provider.Codec) *PSClient

// Send는 partitionID의 Actor에게 req를 전달하고 Resp를 반환한다.
// payload 직렬화/역직렬화는 Codec이 담당한다.
// gRPC status error는 provider error로 변환하여 반환한다.
func (c *PSClient) Send(ctx context.Context, partitionID string, req any, respPtr any) error
```

### PMClient (SDK/abctl → PM, management plane)

```go
// PMClient는 Partition Manager와 통신한다.
// SDK: WatchRouting으로 라우팅 테이블 수신.
// abctl: RequestSplit / RequestMigrate 호출.
type PMClient struct {
    conn   *grpc.ClientConn
    client pb.PartitionManagerServiceClient
}

func NewPMClient(conn *grpc.ClientConn) *PMClient

// WatchRouting은 라우팅 테이블 변경 채널을 반환한다.
// 연결 직후 현재 테이블을 즉시 전달한다.
// 스트림이 끊기면 자동으로 재연결 후 재구독한다.
// ctx 취소 시 채널이 닫힌다.
func (c *PMClient) WatchRouting(ctx context.Context, clientID string) <-chan *domain.RoutingTable

func (c *PMClient) RequestSplit(ctx context.Context, partitionID, splitKey string) (newPartitionID string, err error)
func (c *PMClient) RequestMigrate(ctx context.Context, partitionID, targetNodeID string) error
```

### PSControlClient (PM → PS, control plane)

```go
// PSControlClient는 PM이 PS에게 split/migrate를 명령하는 데 사용한다.
type PSControlClient struct {
    conn   *grpc.ClientConn
    client pb.PartitionControlServiceClient
}

func NewPSControlClient(conn *grpc.ClientConn) *PSControlClient

func (c *PSControlClient) ExecuteSplit(ctx context.Context, partitionID, splitKey, newPartitionID string) error
func (c *PSControlClient) ExecuteMigrateOut(ctx context.Context, partitionID, targetNodeID, targetAddr string) error
```

---

## 주요 결정 사항

| 항목 | 결정 | 근거 |
|---|---|---|
| Payload 직렬화 | `provider.Codec` 인터페이스 | JSON/protobuf/커스텀 교체 가능. SDK·PS 양측에 동일 구현체 주입. |
| 기본 Codec | `adapter/json` JSONCodec | 별도 설정 없이 바로 사용 가능. 성능 필요 시 교체. |
| gRPC 서비스 분리 | data / management / control 3개 | 역할·방향이 명확히 다름. 단일 서비스로 합치면 PS·PM 경계가 모호해짐. |
| 에러 전달 방식 | gRPC status code | proto message에 에러 필드 없이 gRPC 표준 방식 사용. |
| WatchRouting 재연결 | PMClient 내부 자동 재연결 | SDK가 스트림 종료를 직접 처리하지 않아도 됨. |
| 핸들러 구현 위치 | `ps/`, `pm/` 패키지 | transport는 proto·공통 설정만 제공. 비즈니스 로직은 각 패키지에서 담당. |
| 커넥션 풀 | `ConnPool` (transport 내부) | SDK가 라우팅 변경 시마다 새 커넥션을 만들지 않도록 재사용. |

---

## 알려진 한계

- **Codec 불일치**: SDK와 PS에 다른 Codec이 주입되면 런타임 역직렬화 에러가 발생한다. 컴파일 타임에 검증할 수 없으므로, 배포 시 동일 Codec 사용을 운영 규약으로 강제해야 한다.
- **WatchRouting 재연결 gap**: 스트림 재연결 중에 라우팅 변경이 발생하면 일시적으로 stale 라우팅을 사용할 수 있다. 재연결 시 PM이 즉시 현재 테이블을 push하므로 gap은 최소화된다. SDK의 `ErrPartitionMoved` 처리(재시도 로직)가 이를 보완한다.
- **단방향 스트리밍 한계**: `WatchRouting`은 서버→클라이언트 단방향 스트리밍이다. 네트워크 단절 시 클라이언트가 감지하는 데 gRPC keepalive 설정에 의존한다. keepalive 파라미터는 구현 단계에서 결정한다.
