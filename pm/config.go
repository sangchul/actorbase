package pm

import (
	"fmt"

	"github.com/oomymy/actorbase/pm/policy"
	"github.com/oomymy/actorbase/provider"
)

// Config는 PM 생성에 필요한 모든 설정과 의존성을 담는다.
type Config struct {
	// ─── 필수 (사용자 제공) ───────────────────────────────────────

	ListenAddr    string   // gRPC 수신 주소 ("host:port")
	EtcdEndpoints []string // etcd 엔드포인트 목록

	// ActorTypes는 bootstrap 시 생성할 actor type 목록.
	// 첫 번째 PS가 등록될 때 각 actor type마다 전체 키 범위를 담당하는 초기 파티션을 생성한다.
	// 최소 1개 이상 지정해야 한다.
	ActorTypes []string

	// ─── 선택 (기본값 있음) ───────────────────────────────────────

	Metrics provider.Metrics       // nil이면 no-op 구현체 사용
	Policy  policy.RebalancePolicy // nil이면 ManualPolicy 사용
}

func (c *Config) setDefaults() {
	if c.Policy == nil {
		c.Policy = &policy.ManualPolicy{}
	}
}

func (c *Config) validate() error {
	if c.ListenAddr == "" {
		return fmt.Errorf("pm: ListenAddr is required")
	}
	if len(c.EtcdEndpoints) == 0 {
		return fmt.Errorf("pm: EtcdEndpoints is required")
	}
	if len(c.ActorTypes) == 0 {
		return fmt.Errorf("pm: ActorTypes is required (at least one actor type)")
	}
	return nil
}
