package ps

import (
	"fmt"
	"time"

	"github.com/sangchul/actorbase/provider"
)

const (
	defaultIdleTimeout            = 5 * time.Minute
	defaultEvictInterval          = 1 * time.Minute
	defaultCheckpointInterval     = 1 * time.Minute
	defaultCheckpointWALThreshold = 100
	defaultEtcdLeaseTTL           = 10 * time.Second
	defaultDrainTimeout           = 60 * time.Second
	defaultShutdownTimeout        = 30 * time.Second
)

// BaseConfig는 PS 인스턴스 전체에 공유되는 설정.
type BaseConfig struct {
	// ─── 필수 (사용자 제공) ───────────────────────────────────────

	NodeID        string
	Addr          string   // gRPC 수신 주소 ("host:port"). etcd에 등록되어 다른 노드가 접속에 사용.
	EtcdEndpoints []string // etcd 엔드포인트 목록

	// ─── 선택 (기본값 있음) ───────────────────────────────────────

	Metrics provider.Metrics // nil이면 메트릭 수집 생략

	EtcdLeaseTTL time.Duration // 노드 lease TTL. 기본값: 10초

	// EvictionScheduler 설정 (모든 actor type 공유)
	IdleTimeout   time.Duration // Actor가 이 시간 동안 메시지 없으면 evict. 기본값: 5분
	EvictInterval time.Duration // eviction 검사 주기. 기본값: 1분

	// CheckpointScheduler 설정 (모든 actor type 공유)
	CheckpointInterval time.Duration // 주기적 checkpoint 간격. 기본값: 1분

	// graceful shutdown 설정
	DrainTimeout    time.Duration // 파티션 선이전 최대 대기 시간. 기본값: 60초
	ShutdownTimeout time.Duration // EvictAll 최대 대기 시간. 기본값: 30초
}

func (c *BaseConfig) setDefaults() {
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = defaultIdleTimeout
	}
	if c.EvictInterval <= 0 {
		c.EvictInterval = defaultEvictInterval
	}
	if c.CheckpointInterval <= 0 {
		c.CheckpointInterval = defaultCheckpointInterval
	}
	if c.EtcdLeaseTTL <= 0 {
		c.EtcdLeaseTTL = defaultEtcdLeaseTTL
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = defaultDrainTimeout
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = defaultShutdownTimeout
	}
}

func (c *BaseConfig) validate() error {
	if c.NodeID == "" {
		return errorf("NodeID is required")
	}
	if c.Addr == "" {
		return errorf("Addr is required")
	}
	if len(c.EtcdEndpoints) == 0 {
		return errorf("EtcdEndpoints is required")
	}
	return nil
}

// TypeConfig는 특정 actor type의 설정.
// TypeID로 구분되며, 동일 PS에 여러 actor type을 등록할 수 있다.
type TypeConfig[Req, Resp any] struct {
	// ─── 필수 (사용자 제공) ───────────────────────────────────────

	TypeID          string                           // actor type 식별자. 라우팅 테이블과 일치해야 한다.
	Factory         provider.ActorFactory[Req, Resp] // Actor 인스턴스 생성 함수
	Codec           provider.Codec                   // SDK와 동일한 Codec 구현체를 주입
	WALStore        provider.WALStore                // WAL 저장소
	CheckpointStore provider.CheckpointStore         // Checkpoint 저장소

	// ─── 선택 (기본값 있음) ───────────────────────────────────────

	MailboxSize            int           // 기본값: engine 기본값 사용
	FlushSize              int           // WAL 배치 최대 크기. 기본값: engine 기본값 사용
	FlushInterval          time.Duration // WAL 배치 최대 대기 시간. 기본값: engine 기본값 사용
	CheckpointWALThreshold int           // 이 수만큼 WAL entry가 쌓이면 자동 checkpoint. 기본값: 100
}

func (c *TypeConfig[Req, Resp]) validate() error {
	if c.TypeID == "" {
		return errorf("TypeID is required")
	}
	if c.Factory == nil {
		return errorf("Factory is required for type %q", c.TypeID)
	}
	if c.Codec == nil {
		return errorf("Codec is required for type %q", c.TypeID)
	}
	if c.WALStore == nil {
		return errorf("WALStore is required for type %q", c.TypeID)
	}
	if c.CheckpointStore == nil {
		return errorf("CheckpointStore is required for type %q", c.TypeID)
	}
	return nil
}

func errorf(format string, args ...any) error {
	return fmt.Errorf("ps: "+format, args...)
}
