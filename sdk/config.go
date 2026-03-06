package sdk

import (
	"fmt"
	"os"
	"time"

	"github.com/oomymy/actorbase/provider"
)

const (
	defaultMaxRetries    = 3
	defaultRetryInterval = 100 * time.Millisecond
)

// Config는 Client 생성에 필요한 설정을 담는다.
type Config[Req, Resp any] struct {
	// ─── 필수 (사용자 제공) ───────────────────────────────────────

	PMAddr string         // PM gRPC 주소 ("host:port")
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
	if c.PMAddr == "" {
		return fmt.Errorf("sdk: PMAddr is required")
	}
	if c.Codec == nil {
		return fmt.Errorf("sdk: Codec is required")
	}
	return nil
}
