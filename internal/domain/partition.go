package domain

// KeyRange represents the key range a partition is responsible for,
// expressed as a half-open interval [Start, End).
// An empty string ("") for End means no upper bound (all keys >= Start).
type KeyRange struct {
	Start string // inclusive
	End   string // exclusive; "" = no upper bound
}

// Contains reports whether key falls within this KeyRange.
func (r KeyRange) Contains(key string) bool {
	if key < r.Start {
		return false
	}
	if r.End == "" {
		return true
	}
	return key < r.End
}

// Overlaps reports whether two KeyRanges have any overlapping region.
// Used to validate range conflicts before split/migration.
func (r KeyRange) Overlaps(other KeyRange) bool {
	// For [r.Start, r.End) and [other.Start, other.End) to overlap:
	// r.Start < other.End && other.Start < r.End
	// End == "" is treated as +∞.
	if other.End != "" && r.Start >= other.End {
		return false
	}
	if r.End != "" && other.Start >= r.End {
		return false
	}
	return true
}

// Partition is the unit of a single partition within the cluster.
// It has a unique ID, an actor type, and a key range it is responsible for.
// Key range overlap validation is performed only within the same ActorType.
type Partition struct {
	ID        string
	ActorType string // actor type identifier; key ranges must be unique within the same ActorType.
	KeyRange  KeyRange
}
