# actorbase 요구사항 문서

## 1. 배경 및 동기

### 1.1 현황

S3 호환 분산 저장소의 object metadata를 MongoDB sharded cluster로 운영 중이다.

### 1.2 문제점

**샤딩 유연성 부족**
- MongoDB의 청크 split/migration이 순수하게 데이터 크기 기반으로 동작한다.
- 특정 파티션에 부하가 집중되는 hotspot 상황에 대응하기 어렵다.
- 운영자가 부하 기반으로 파티션을 직접 조정할 수단이 없다.

**동시성 모델의 복잡성**
- 단일 스레드 모델로 작성하면 trivial한 로직이 MongoDB에서는 CAS(Compare-And-Swap)와 트랜잭션으로 복잡해진다.
- 코드 복잡도 증가 및 성능 저하를 유발한다.

**운영 부담**
- MongoDB sharded cluster 운영에 상당한 운영 지식과 툴링이 필요하다.

### 1.3 목표

Go 라이브러리 형태로 제공되는 actor 기반 분산 key-value 저장소를 구현한다. 사용자가 자신의 데이터 구조와 비즈니스 로직을 actor model로 표현하면, actorbase가 이를 분산 환경에서 실행하도록 한다.

---

## 2. 설계 가정

이 절의 가정들은 설계, 구현, 테스트 전반의 기준이 된다. 가정을 벗어나는 환경에서의 동작은 보장하지 않는다.

### 2.1 배포 환경

- **기업 내부 네트워크**에서만 운영된다. 공개 인터넷에 노출되는 환경은 고려하지 않는다.
- 네트워크 경계 보안(방화벽, VPN 등)은 인프라 수준에서 갖춰져 있다고 가정한다.
- 노드 간 통신에 별도의 mTLS나 암호화를 기본 제공하지 않는다. 필요 시 인프라 레벨에서 처리한다.

### 2.2 장애 모델

- **Crash-stop 장애**만 고려한다. 노드는 정상 동작하거나 완전히 멈추는 두 가지 상태만 가진다.
- **비잔틴 장애(Byzantine failure)**는 고려하지 않는다. 악의적이거나 임의의 잘못된 응답을 보내는 노드는 없다고 가정한다.
- 클러스터 내 노드들은 신뢰할 수 있다고 가정한다.

### 2.3 규모

- 노드 수 기준으로 **수백~약 1,000대** 규모를 대상으로 한다.
- 수만 대 이상의 초대형 클러스터는 설계 목표에 포함하지 않는다.
- 이 규모 가정은 etcd lease 기반 멤버십, 단일 PM 인스턴스 등 여러 설계 결정의 근거가 된다.

---

## 3. 시스템 개요

### 3.1 핵심 설계 원칙

- **라이브러리 형태**: 독립 실행 바이너리가 아닌 Go 라이브러리로 배포한다. 사용자는 `import`하여 사용한다.
- **Actor = Partition**: 하나의 Actor가 하나의 파티션(shard)을 담당하며, 특정 key range를 관리한다.
- **단일 스레드 Actor**: Actor 내부는 단일 스레드로 동작하므로 CAS/트랜잭션 없이 일관성을 보장한다.
- **플러그인 방식 Persistent Layer**: 영속성 백엔드를 Go interface로 추상화하여 사용자가 자신의 환경에 맞게 구현한다.
- **기존 라이브러리 활용**: Raft, gRPC, etcd 등 검증된 라이브러리를 기반으로 구현한다.

---

## 4. Actor 모델

### 4.1 Actor 정의

사용자는 아래 Go interface를 구현하여 Actor를 정의한다.

```go
// Framework가 정의하는 interface
type Actor interface {
    Receive(ctx Context, msg Message) error
    Snapshot() ([]byte, error)  // 체크포인트용 직렬화
    Restore([]byte) error        // 복구용 역직렬화
}
```

### 4.2 사용 예시

```go
type BucketActor struct {
    objects map[string]*ObjectMeta
}

func (a *BucketActor) Receive(ctx Context, msg Message) error {
    switch m := msg.(type) {
    case *PutObject:
        a.objects[m.Key] = m.Meta
    case *GetObject:
        ctx.Reply(a.objects[m.Key])
    case *DeleteObject:
        delete(a.objects, m.Key)
    }
    return nil
}

func (a *BucketActor) Snapshot() ([]byte, error) {
    return json.Marshal(a.objects)
}

func (a *BucketActor) Restore(data []byte) error {
    return json.Unmarshal(data, &a.objects)
}
```

### 4.3 Actor 생명주기

- Actor는 활성 상태일 때 메모리에 상주한다.
- 비활성(idle) 상태가 되면 메모리에서 evict된다.
- 해당 key range로 요청이 들어오면 Persistent Layer에서 상태를 로드하여 재활성화된다.
- Actor는 write-back 캐시처럼 동작한다.

### 4.4 Actor 실행 모델

- Actor당 단일 스레드(mailbox 기반) 실행을 보장한다.
- Actor 내부에서 CAS나 트랜잭션 없이 데이터를 직접 수정할 수 있다.
- 하나의 Partition Server가 여러 Actor(파티션)를 호스팅한다.

---

## 5. Persistent Layer

### 5.1 인터페이스 정의

영속성 백엔드는 플러그인 방식의 Go interface로 추상화한다.

```go
type Store interface {
    Save(key string, data []byte) error
    Load(key string) ([]byte, error)
    Delete(key string) error
}
```

### 5.2 사용 예시

```go
system := actorbase.New(actorbase.Config{
    Store: &MyS3Store{bucket: "my-bucket"},
})
```

### 5.3 저장 대상

Persistent Layer에는 다음 두 가지를 저장한다.
- **Checkpoint**: Actor 상태의 스냅샷
- **WAL (Write-Ahead Log)**: Checkpoint 이후의 모든 write 연산 로그

---

## 6. 장애 복구

### 6.1 복구 전략

Checkpoint와 WAL을 함께 사용한다. 둘은 대안이 아닌 상호 보완 관계다.

**복구 흐름:**
1. 노드 장애 감지
2. 재시작 시 Persistent Layer에서 마지막 **Checkpoint** 로드 (복구 기준점 확보)
3. Checkpoint의 LSN(Log Sequence Number) 이후의 **WAL replay**
4. Actor가 장애 직전 상태로 완전히 복구됨

이는 PostgreSQL, RocksDB 등 주요 데이터베이스와 동일한 표준 접근 방식이다.

### 6.2 체크포인트 주기

- 주기적으로 Actor의 in-memory 상태를 Persistent Layer에 저장한다.
- 구체적인 주기 및 설정 방식은 구현 단계에서 결정한다.

---

## 7. 클러스터 관리

### 7.1 클러스터 메타데이터 저장소

**etcd**를 클러스터 메타데이터 저장소로 사용한다.

etcd에 저장되는 정보:
- 노드 등록 정보
- 파티션/샤드 맵
- 리더 정보
- 라우팅 테이블

### 7.2 클러스터 멤버십

별도의 gossip 라이브러리 없이 **etcd lease**를 활용하여 클러스터 멤버십을 관리한다. etcd가 이미 필수 의존성이므로 추가 의존성 없이 구현할 수 있다.

**동작 방식:**
1. 노드 기동 시 etcd에 자신의 정보(주소, 포트 등)를 TTL lease와 함께 등록
2. 노드는 주기적으로 lease를 갱신(heartbeat)
3. 노드 장애 시 heartbeat가 끊기면 TTL 만료 후 lease 자동 삭제
4. PM 및 다른 노드는 etcd watch로 멤버십 변경을 즉시 감지

gossip 방식 대비 **강한 일관성(linearizable)**으로 멤버십 상태를 확인할 수 있으며, 대상 노드 규모(수십~백 노드)에서 충분한 성능을 제공한다.

### 7.3 Partition Manager (PM)

파티션 토폴로지 관리를 담당하는 별도의 daemon 프로세스다.

**아키텍처:**

```
[Admin/Operator]
      |
      v (split/migrate 요청)
[PM - Partition Manager] <--- etcd (메타데이터 저장소)
      |
      v (실행 명령)
[Partition Server] ---> PM (결과 보고)
      |
      v
PM가 라우팅 테이블 갱신 --> 모든 참여자에 전파
      |
      v
[Client Library] (로컬 캐시 갱신)
```

**PM 책임:**
- 운영자 또는 자동화 정책으로부터 파티션 split/migration 요청 수신
- 담당 Partition Server에 실행 명령 전송 및 조율
- Partition Server로부터 결과 수신
- etcd의 라우팅 테이블 갱신
- 모든 참여자(서버 및 클라이언트)에 라우팅 정보 전파

**PM 가용성:**
- 초기에는 단일 인스턴스로 운영한다.
- PM 다중화(HA)는 향후 과제로 미룬다.

---

## 8. 클라이언트 라우팅

### 8.1 라우팅 방식

- Client Library는 etcd에 직접 접근하지 않는다.
- **Client Library가 PM에 접속하여 라우팅 테이블을 가져온다.**
- 라우팅 테이블은 Client Library 로컬에 캐시한다.
- 파티션 토폴로지가 변경되면 PM이 모든 등록된 클라이언트에 라우팅 정보를 push한다.
- 캐시 미스 또는 stale 데이터 감지 시 PM에서 재조회한다.

이 방식은 Kafka 클라이언트의 브로커 메타데이터 조회, Vitess의 vtgate 라우팅과 유사하다.

### 8.2 Client API

- **Go SDK만 제공**한다.
- 초기 범위에서는 외부 클라이언트를 위한 gRPC나 HTTP API를 제공하지 않는다.
- SDK는 애플리케이션과 같은 Go 프로세스에 라이브러리로 임베드된다.

---

## 9. 파티션 Split / Migration

### 9.1 지원 모드

세 가지 모드를 모두 지원한다.

| 모드 | 설명 |
|---|---|
| **자동 (metric 기반)** | PM이 부하/QPS metric을 모니터링하고 임계치 초과 시 자동으로 split/migration 트리거 |
| **정책 기반** | 운영자가 규칙/정책 설정, PM이 조건 충족 시 실행 |
| **수동 (ops 툴링)** | 운영자가 admin 툴을 통해 명시적으로 split/move 명령 발행, PM에 전달 |

MongoDB의 핵심 문제였던 "크기 기반 자동 split만 가능"을 해결하기 위해 세 가지 모드를 모두 지원한다.

---

## 10. 전체 아키텍처

```
┌─────────────────────────────────────────┐
│           사용자 애플리케이션            │
│  (actorbase 라이브러리 import,           │
│   Go interface로 커스텀 Actor 정의)      │
└───────────────┬─────────────────────────┘
                │ Go SDK
                ▼
┌─────────────────────────────────────────┐
│         Client Library                  │
│  - 캐시된 파티션 맵으로 요청 라우팅     │
│  - 라우팅 갱신 시 PM에 문의             │
└───────────────┬─────────────────────────┘
                │ gRPC
                ▼
┌─────────────────────────────────────────┐
│       Partition Server(s)               │
│  - 여러 Actor(파티션) 호스팅            │
│  - 각 Actor: 단일 스레드,               │
│    특정 key range를 메모리에서 관리     │
│  - Persistent Layer로 write-back cache  │
│  - 내구성: Checkpoint + WAL             │
└───────────────┬─────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────┐
│    Persistent Layer (플러그인)          │
│  - Go interface: Store                  │
│  - 사용자 구현: S3, RocksDB 등          │
│  - 저장 내용: Checkpoint + WAL          │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│    Partition Manager (PM) - Daemon      │
│  - 단일 인스턴스 (초기)                 │
│  - split/migrate 요청 수신              │
│  - 파티션 이동 조율                     │
│  - etcd에서 라우팅 테이블 관리          │
│  - 모든 클라이언트/서버에 라우팅 전파  │
└───────────────┬─────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────┐
│              etcd                       │
│  - 노드 레지스트리                      │
│  - 파티션/샤드 맵                       │
│  - 라우팅 테이블                        │
└─────────────────────────────────────────┘
```

---

## 11. 활용 예정 라이브러리

| 라이브러리 | 용도 |
|---|---|
| `google.golang.org/grpc` | 노드 간 통신 |
| `go.etcd.io/etcd/client` | 클러스터 메타데이터 저장소 및 멤버십 관리 |

---

## 12. 미결정 사항 (구현 단계 결정)

| 항목 | 내용 |
|---|---|
| PM 고가용성 | 다중 PM 인스턴스 구성 방식 |
| WAL 포맷 | 구체적인 WAL 포맷 및 구현 방식 |
| 체크포인트 주기 | 설정 방식 및 기본값 |
| Actor mailbox 크기 | 버퍼링 채널 크기 설정 |
| Actor 재시작 전략 | 크래시된 Actor의 supervisor/재시작 정책 |
| 라우팅 push 방식 | 클라이언트 대상 라우팅 테이블 push 구현 (push vs. poll) |
| 운영 툴링 | 수동 split/migrate를 위한 admin CLI/UI 세부 사항 |
