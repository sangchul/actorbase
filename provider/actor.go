package provider

import "log/slog"

// Actor는 하나의 파티션(key range)을 담당하는 비즈니스 로직 단위.
// 파티션당 하나의 인스턴스가 생성되며, 단일 스레드로 실행된다.
// Req는 이 Actor가 수신하는 요청 타입, Resp는 응답 타입이다.
// 사용자가 직접 구현한다.
type Actor[Req, Resp any] interface {
	// Receive는 요청을 처리하고 응답과 WAL 데이터를 반환한다.
	// walEntry가 nil이면 read-only 연산이며 WAL에 기록하지 않는다.
	// walEntry가 nil이 아니면 engine이 WALStore에 기록한 뒤 응답을 반환한다.
	Receive(ctx Context, req Req) (resp Resp, walEntry []byte, err error)

	// Replay는 WAL entry 하나를 Actor 상태에 적용한다.
	// 복구 시 마지막 checkpoint 이후의 WAL entries를 순서대로 적용하는 데 사용한다.
	Replay(entry []byte) error

	// Snapshot은 현재 Actor 상태를 직렬화하여 반환한다. (checkpoint용)
	Snapshot() ([]byte, error)

	// Restore는 Snapshot 데이터로 Actor 상태를 복원한다.
	Restore(data []byte) error

	// Split은 splitKey 기준으로 상위 절반의 상태를 직렬화하여 반환하고,
	// 자신의 상태에서 해당 데이터를 제거한다.
	// engine이 split 실행 시 호출한다.
	Split(splitKey string) (upperHalf []byte, err error)
}

// ActorFactory는 파티션 ID마다 새 Actor 인스턴스를 생성하는 함수.
// 사용자가 직접 구현한다.
type ActorFactory[Req, Resp any] func(partitionID string) Actor[Req, Resp]

// Countable은 Actor가 선택적으로 구현하는 통계 인터페이스.
// 구현하면 engine이 key count를 stats에 포함한다. 구현하지 않으면 -1로 보고된다.
type Countable interface {
	KeyCount() int64
}

// SplitHinter는 Actor가 선택적으로 구현하는 split 위치 제안 인터페이스.
//
// 구현하지 않으면 engine이 partition key range의 midpoint를 사용한다 (기본 동작).
// 구현하면 Actor가 내부 상태(hotspot 등)를 기반으로 split 위치를 직접 결정할 수 있다.
//
// 반환값이 ""이면 engine이 midpoint fallback을 사용한다.
// SplitHint()는 mailbox goroutine 내에서 호출되므로 별도 동기화가 필요 없다.
type SplitHinter interface {
	SplitHint() string
}

// Context는 Actor.Receive 호출 시 프레임워크가 주입하는 런타임 정보.
// 사용자는 구현하지 않고 사용만 한다.
type Context interface {
	PartitionID() string
	Logger() *slog.Logger
}
