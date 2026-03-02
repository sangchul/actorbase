package provider

import "context"

// CheckpointStore는 Actor 상태 스냅샷 저장소의 추상화.
// 파티션 ID를 키로 사용하며, 파티션당 하나의 스냅샷을 유지한다.
// 사용자가 직접 구현한다. (S3, RocksDB 등)
type CheckpointStore interface {
	Save(ctx context.Context, partitionID string, data []byte) error
	Load(ctx context.Context, partitionID string) ([]byte, error)
	Delete(ctx context.Context, partitionID string) error
}
