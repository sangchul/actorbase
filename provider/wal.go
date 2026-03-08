package provider

import "context"

// WALEntry는 WAL의 단일 레코드.
type WALEntry struct {
	LSN  uint64 // Log Sequence Number. 단조 증가.
	Data []byte
}

// WALStore는 Write-Ahead Log 저장소의 추상화.
// append-only이며 LSN 기반으로 순차 조회한다.
// 사용자가 직접 구현한다. (Redis Stream, Kafka 등)
type WALStore interface {
	// AppendBatch는 파티션의 WAL에 여러 엔트리를 원자적으로 추가하고
	// 각 엔트리에 부여된 LSN을 순서대로 반환한다.
	// 반환 슬라이스의 길이는 data 슬라이스의 길이와 같다.
	// 일부만 성공하는 부분 실패는 허용하지 않는다 — 에러 시 전체 실패.
	AppendBatch(ctx context.Context, partitionID string, data [][]byte) (lsns []uint64, err error)

	// ReadFrom은 fromLSN 이상의 모든 WAL 엔트리를 순서대로 반환한다.
	// 복구 시 checkpoint LSN 이후부터 replay하는 데 사용한다.
	ReadFrom(ctx context.Context, partitionID string, fromLSN uint64) ([]WALEntry, error)

	// TrimBefore는 lsn 미만의 오래된 WAL 엔트리를 삭제한다.
	// checkpoint 완료 후 불필요한 엔트리를 정리하는 데 사용한다.
	TrimBefore(ctx context.Context, partitionID string, lsn uint64) error
}
