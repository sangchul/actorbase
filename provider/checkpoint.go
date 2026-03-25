package provider

import "context"

// CheckpointStore is the abstraction for an Actor state snapshot store.
// Uses partition ID as the key and maintains one snapshot per partition.
// Implemented by the user. (e.g., S3, RocksDB)
type CheckpointStore interface {
	Save(ctx context.Context, partitionID string, data []byte) error
	Load(ctx context.Context, partitionID string) ([]byte, error)
	Delete(ctx context.Context, partitionID string) error
}
