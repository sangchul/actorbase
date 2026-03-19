package pm

import (
	"fmt"

	"github.com/sangchul/actorbase/policy"
	"github.com/sangchul/actorbase/provider"
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

	// HTTPAddr는 웹 콘솔 HTTP 서버 주소 (예: ":8080").
	// 비어 있으면 웹 콘솔을 시작하지 않는다.
	HTTPAddr string

	// ─── 선택 (기본값 있음) ───────────────────────────────────────

	Metrics provider.Metrics // nil이면 no-op 구현체 사용

	// BalancePolicy는 분배 전략 구현체.
	// nil이면 NoopBalancePolicy(아무 작업도 수행하지 않음)를 사용한다.
	// 사용자가 provider.BalancePolicy를 직접 구현하여 주입하거나,
	// abctl policy apply로 ThresholdPolicy를 런타임에 적용할 수 있다.
	BalancePolicy provider.BalancePolicy
}

func (c *Config) setDefaults() {
	if c.BalancePolicy == nil {
		c.BalancePolicy = &policy.NoopBalancePolicy{}
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
