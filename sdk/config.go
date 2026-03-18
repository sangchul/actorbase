package sdk

import (
	"fmt"
	"os"
	"time"

	"github.com/sangchul/actorbase/provider"
)

const (
	defaultMaxRetries    = 3
	defaultRetryInterval = 100 * time.Millisecond
)

// Config는 Client 생성에 필요한 설정을 담는다.
type Config[Req, Resp any] struct {
	// ─── 필수 (사용자 제공) ───────────────────────────────────────

	// PMAddr: 단일 PM 모드. EtcdEndpoints가 없을 때 필수.
	PMAddr string
	// EtcdEndpoints: HA 모드. 설정 시 etcd에서 리더 PM 주소를 자동 조회한다.
	// PM 장애 시 자동으로 새 리더를 재발견하여 연결을 복구한다.
	// PMAddr와 EtcdEndpoints 중 하나만 설정하면 된다.
	EtcdEndpoints []string

	TypeID string         // 이 Client가 대상으로 하는 actor type 식별자
	Codec  provider.Codec // PS와 동일한 구현체를 주입해야 한다

	// ─── 선택 (기본값 있음) ───────────────────────────────────────

	ClientID      string        // 디버깅·로깅용 식별자. 기본값: hostname
	MaxRetries    int           // 재시도 최대 횟수. 기본값: 3
	RetryInterval time.Duration // 재시도 전 대기 시간. 기본값: 100ms
}

func (c *Config[Req, Resp]) setDefaults() {
	if c.ClientID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "sdk-client"
		}
		c.ClientID = hostname
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = defaultMaxRetries
	}
	if c.RetryInterval <= 0 {
		c.RetryInterval = defaultRetryInterval
	}
}

func (c *Config[Req, Resp]) validate() error {
	if c.PMAddr == "" && len(c.EtcdEndpoints) == 0 {
		return fmt.Errorf("sdk: PMAddr or EtcdEndpoints is required")
	}
	if c.TypeID == "" {
		return fmt.Errorf("sdk: TypeID is required")
	}
	if c.Codec == nil {
		return fmt.Errorf("sdk: Codec is required")
	}
	return nil
}

// haMode는 etcd 기반 HA 모드인지 반환한다.
func (c *Config[Req, Resp]) haMode() bool {
	return len(c.EtcdEndpoints) > 0
}
