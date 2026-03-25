package provider

import "log/slog"

// Actor is the business logic unit responsible for a single partition (key range).
// One instance is created per partition and runs on a single goroutine.
// Req is the request type this Actor receives; Resp is the response type.
// Implemented by the user.
type Actor[Req, Resp any] interface {
	// Receive processes a request and returns a response along with a WAL entry.
	// If walEntry is nil, the operation is read-only and nothing is written to the WAL.
	// If walEntry is non-nil, the engine writes it to the WALStore before returning the response.
	Receive(ctx Context, req Req) (resp Resp, walEntry []byte, err error)

	// Replay applies a single WAL entry to the Actor's state.
	// Used during recovery to replay WAL entries in order after the last checkpoint.
	Replay(entry []byte) error

	// Export serializes and returns the Actor's state.
	//
	// If splitKey is "", the entire state is serialized (for checkpointing, read-only).
	// If splitKey is non-empty, state with keys >= splitKey is serialized and
	// removed from the Actor's own state (for splitting).
	Export(splitKey string) ([]byte, error)

	// Import applies serialized state data to the Actor.
	//
	// When called on an empty Actor, it restores from a checkpoint.
	// When called on an Actor with existing data, it merges the state.
	// Implementations do not need to distinguish between the two cases;
	// simply merge the received data into the current state.
	Import(data []byte) error
}

// ActorFactory is a function that creates a new Actor instance for each partition ID.
// Implemented by the user.
type ActorFactory[Req, Resp any] func(partitionID string) Actor[Req, Resp]

// Countable is an optional interface an Actor can implement for reporting statistics.
// If implemented, the engine includes the key count in stats. If not implemented, -1 is reported.
type Countable interface {
	KeyCount() int64
}

// SplitHinter is an optional interface an Actor can implement to suggest a split position.
//
// If not implemented, the engine uses the midpoint of the partition's key range (default behavior).
// If implemented, the Actor can determine the split position based on its internal state (e.g., hotspots).
//
// Returning "" causes the engine to fall back to the midpoint.
// SplitHint() is called from within the mailbox goroutine, so no additional synchronization is needed.
type SplitHinter interface {
	SplitHint() string
}

// Context holds runtime information injected by the framework when Actor.Receive is called.
// Used by the Actor implementation; not implemented by the user.
type Context interface {
	PartitionID() string
	Logger() *slog.Logger
}
