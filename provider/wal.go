package provider

import "context"

// WALEntry is a single record in the WAL.
type WALEntry struct {
	LSN  uint64 // Log Sequence Number. Monotonically increasing.
	Data []byte
}

// WALStore is the abstraction for a Write-Ahead Log store.
// It is append-only and supports sequential reads by LSN.
// Implemented by the user. (e.g., Redis Stream, Kafka)
type WALStore interface {
	// AppendBatch atomically appends multiple entries to a partition's WAL
	// and returns the assigned LSNs in order.
	// The length of the returned slice equals the length of the data slice.
	// Partial failure is not allowed — on error, the entire batch fails.
	AppendBatch(ctx context.Context, partitionID string, data [][]byte) (lsns []uint64, err error)

	// ReadFrom returns all WAL entries with LSN >= fromLSN, in order.
	// Used during recovery to replay entries after the last checkpoint LSN.
	ReadFrom(ctx context.Context, partitionID string, fromLSN uint64) ([]WALEntry, error)

	// TrimBefore deletes stale WAL entries with LSN < lsn.
	// Used to clean up unnecessary entries after a checkpoint completes.
	TrimBefore(ctx context.Context, partitionID string, lsn uint64) error
}
