package provider

import "errors"

var (
	// ErrNotFound: Actor가 해당 키를 찾지 못함.
	// Actor.Receive 내부에서 반환하며, SDK가 호출자에게 전달한다.
	ErrNotFound = errors.New("not found")

	// ErrPartitionMoved: 파티션이 다른 노드로 이동됨.
	// 라우팅 테이블 자체가 outdated인 상태.
	// SDK가 PM에서 새 라우팅 테이블을 받아 재시도해야 한다.
	ErrPartitionMoved = errors.New("partition moved")

	// ErrPartitionNotOwned: 요청을 받은 PS가 해당 파티션을 담당하지 않음.
	// 라우팅 테이블의 일시적 불일치 상태.
	// SDK가 재시도해야 한다.
	ErrPartitionNotOwned = errors.New("partition not owned")

	// ErrPartitionBusy: 파티션 split/migration 진행 중.
	// 잠시 후 재시도해야 한다.
	ErrPartitionBusy = errors.New("partition busy")

	// ErrTimeout: 요청 처리 시간 초과.
	ErrTimeout = errors.New("timeout")

	// ErrActorPanicked: Actor.Receive 실행 중 panic 발생.
	// 프레임워크가 recover 후 이 에러를 반환한다.
	ErrActorPanicked = errors.New("actor panicked")
)
