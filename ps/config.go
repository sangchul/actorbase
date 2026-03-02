package ps

import (
	"fmt"
	"time"

	"github.com/oomymy/actorbase/provider"
)

const (
	defaultIdleTimeout             = 5 * time.Minute
	defaultEvictInterval           = 1 * time.Minute
	defaultCheckpointInterval      = 1 * time.Minute
	defaultCheckpointWALThreshold  = 100
	defaultEtcdLeaseTTL            = 10 * time.Second
	defaultShutdownTimeout         = 30 * time.Second
)

// Config는 PS 생성에 필요한 모든 설정과 의존성을 담는다.
type Config[Req, Resp any] struct {
	// ─── 필수 (사용자 제공) ───────────────────────────────────────

	NodeID string // 클러스터 내 유일한 PS 식별자
	Addr   string // gRPC 수신 주소 ("host:port"). etcd에 등록되어 다른 노드가 접속에 사용.

	EtcdEndpoints []string // etcd 엔드포인트 목록

	Factory         provider.ActorFactory[Req, Resp]
	Codec           provider.Codec // SDK와 동일한 구현체를 주입해야 한다
	WALStore        provider.WALStore
	CheckpointStore provider.CheckpointStore

	// ─── 선택 (기본값 있음) ───────────────────────────────────────

	Metrics provider.Metrics // nil이면 메트릭 수집 생략

	// ActorHost 설정
	MailboxSize   int           // 기본값: engine 기본값 사용
	FlushSize     int           // WAL 배치 최대 크기. 기본값: engine 기본값 사용
	FlushInterval time.Duration // WAL 배치 최대 대기 시간. 기본값: engine 기본값 사용

	// EvictionScheduler 설정
	IdleTimeout   time.Duration // Actor가 이 시간 동안 메시지 없으면 evict. 기본값: 5분
	EvictInterval time.Duration // eviction 검사 주기. 기본값: 1분

	// CheckpointScheduler 설정
	CheckpointInterval     time.Duration // 주기적 checkpoint 간격. 기본값: 1분
	CheckpointWALThreshold int           // 이 수만큼 WAL entry가 쌓이면 자동 checkpoint. 기본값: 100

	// etcd 설정
	EtcdLeaseTTL time.Duration // 노드 lease TTL. 기본값: 10초

	// graceful shutdown 설정
	ShutdownTimeout time.Duration // EvictAll 최대 대기 시간. 기본값: 30초
}

func (c *Config[Req, Resp]) setDefaults() {
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = defaultIdleTimeout
	}
	if c.EvictInterval <= 0 {
		c.EvictInterval = defaultEvictInterval
	}
	if c.CheckpointInterval <= 0 {
		c.CheckpointInterval = defaultCheckpointInterval
	}
	if c.CheckpointWALThreshold <= 0 {
		c.CheckpointWALThreshold = defaultCheckpointWALThreshold
	}
	if c.EtcdLeaseTTL <= 0 {
		c.EtcdLeaseTTL = defaultEtcdLeaseTTL
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = defaultShutdownTimeout
	}
}

func (c *Config[Req, Resp]) validate() error {
	if c.NodeID == "" {
		return errorf("NodeID is required")
	}
	if c.Addr == "" {
		return errorf("Addr is required")
	}
	if len(c.EtcdEndpoints) == 0 {
		return errorf("EtcdEndpoints is required")
	}
	if c.Factory == nil {
		return errorf("Factory is required")
	}
	if c.Codec == nil {
		return errorf("Codec is required")
	}
	if c.WALStore == nil {
		return errorf("WALStore is required")
	}
	if c.CheckpointStore == nil {
		return errorf("CheckpointStore is required")
	}
	return nil
}

func errorf(msg string) error {
	return fmt.Errorf("ps: %s", msg)
}
