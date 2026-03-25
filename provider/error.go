package provider

import "errors"

var (
	// ErrNotFound: The Actor could not find the requested key.
	// Returned from within Actor.Receive and propagated to the caller by the SDK.
	ErrNotFound = errors.New("not found")

	// ErrPartitionMoved: The partition has moved to another node.
	// Indicates the routing table is outdated.
	// The SDK must fetch a new routing table from the PM and retry.
	ErrPartitionMoved = errors.New("partition moved")

	// ErrPartitionNotOwned: The PS that received the request does not own the partition.
	// Indicates a temporary routing table inconsistency.
	// The SDK must retry.
	ErrPartitionNotOwned = errors.New("partition not owned")

	// ErrPartitionBusy: The partition is currently being split or migrated.
	// Retry after a short delay.
	ErrPartitionBusy = errors.New("partition busy")

	// ErrTimeout: The request timed out.
	ErrTimeout = errors.New("timeout")

	// ErrActorPanicked: A panic occurred during Actor.Receive execution.
	// The framework recovers from the panic and returns this error.
	ErrActorPanicked = errors.New("actor panicked")
)
