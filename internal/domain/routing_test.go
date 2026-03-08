package domain

import "testing"

func makeEntry(partID, start, end, nodeID, addr string) RouteEntry {
	return RouteEntry{
		Partition: Partition{
			ID:        partID,
			ActorType: "test",
			KeyRange:  KeyRange{Start: start, End: end},
		},
		Node: NodeInfo{ID: nodeID, Address: addr},
	}
}

func TestNewRoutingTable_SortsEntries(t *testing.T) {
	entries := []RouteEntry{
		makeEntry("p2", "m", "z", "n1", "n1:9000"),
		makeEntry("p1", "a", "m", "n1", "n1:9000"),
	}
	rt, err := NewRoutingTable(1, entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := rt.Entries()
	if got[0].Partition.ID != "p1" || got[1].Partition.ID != "p2" {
		t.Errorf("expected sorted order [p1, p2], got [%s, %s]", got[0].Partition.ID, got[1].Partition.ID)
	}
}

func TestNewRoutingTable_RejectsDuplicatePartitionID(t *testing.T) {
	entries := []RouteEntry{
		makeEntry("p1", "a", "m", "n1", "n1:9000"),
		makeEntry("p1", "m", "z", "n1", "n1:9000"),
	}
	_, err := NewRoutingTable(1, entries)
	if err == nil {
		t.Fatal("expected error for duplicate partitionID")
	}
}

func TestNewRoutingTable_RejectsOverlappingRanges(t *testing.T) {
	// 동일 actorType 내 겹치는 범위는 에러
	entries := []RouteEntry{
		makeEntry("p1", "a", "n", "n1", "n1:9000"),
		makeEntry("p2", "m", "z", "n1", "n1:9000"),
	}
	_, err := NewRoutingTable(1, entries)
	if err == nil {
		t.Fatal("expected error for overlapping key ranges within same actor type")
	}
}

func TestNewRoutingTable_AllowsOverlappingRangesDifferentActorTypes(t *testing.T) {
	// 서로 다른 actorType 간 겹치는 범위는 허용
	entries := []RouteEntry{
		{
			Partition: Partition{ID: "p1", ActorType: "bucket", KeyRange: KeyRange{Start: "a", End: "m"}},
			Node:      NodeInfo{ID: "n1", Address: "n1:9000"},
		},
		{
			Partition: Partition{ID: "p2", ActorType: "object", KeyRange: KeyRange{Start: "a", End: "m"}},
			Node:      NodeInfo{ID: "n1", Address: "n1:9000"},
		},
	}
	_, err := NewRoutingTable(1, entries)
	if err != nil {
		t.Fatalf("unexpected error for different actor types: %v", err)
	}
}

func TestRoutingTable_Lookup(t *testing.T) {
	entries := []RouteEntry{
		makeEntry("p1", "a", "m", "n1", "n1:9000"),
		makeEntry("p2", "m", "z", "n2", "n2:9000"),
		makeEntry("p3", "z", "", "n3", "n3:9000"),
	}
	rt, err := NewRoutingTable(1, entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tests := []struct {
		key        string
		wantPartID string
		wantFound  bool
	}{
		{key: "a", wantPartID: "p1", wantFound: true},
		{key: "l", wantPartID: "p1", wantFound: true},
		{key: "m", wantPartID: "p2", wantFound: true},
		{key: "y", wantPartID: "p2", wantFound: true},
		{key: "z", wantPartID: "p3", wantFound: true},
		{key: "zzzz", wantPartID: "p3", wantFound: true},
		{key: "0", wantPartID: "", wantFound: false}, // before all ranges
	}

	for _, tt := range tests {
		e, found := rt.Lookup("test", tt.key)
		if found != tt.wantFound {
			t.Errorf("Lookup(%q): found=%v, want %v", tt.key, found, tt.wantFound)
			continue
		}
		if found && e.Partition.ID != tt.wantPartID {
			t.Errorf("Lookup(%q): partID=%s, want %s", tt.key, e.Partition.ID, tt.wantPartID)
		}
	}

	// 다른 actorType으로 조회하면 찾지 못해야 함
	_, found := rt.Lookup("other", "m")
	if found {
		t.Error("Lookup with unknown actorType should return false")
	}
}

func TestRoutingTable_LookupByPartition(t *testing.T) {
	entries := []RouteEntry{
		makeEntry("p1", "a", "m", "n1", "n1:9000"),
		makeEntry("p2", "m", "z", "n2", "n2:9000"),
	}
	rt, _ := NewRoutingTable(1, entries)

	e, ok := rt.LookupByPartition("p2")
	if !ok {
		t.Fatal("expected to find p2")
	}
	if e.Partition.ID != "p2" {
		t.Errorf("got partition %s, want p2", e.Partition.ID)
	}

	_, ok = rt.LookupByPartition("nonexistent")
	if ok {
		t.Error("expected not found for nonexistent partition")
	}
}

func TestRoutingTable_Version(t *testing.T) {
	rt, _ := NewRoutingTable(42, nil)
	if rt.Version() != 42 {
		t.Errorf("Version() = %d, want 42", rt.Version())
	}
}

func TestRoutingTable_EntriesReturnsCopy(t *testing.T) {
	entries := []RouteEntry{makeEntry("p1", "a", "z", "n1", "n1:9000")}
	rt, _ := NewRoutingTable(1, entries)

	got := rt.Entries()
	got[0].Partition.ID = "mutated"

	original := rt.Entries()
	if original[0].Partition.ID == "mutated" {
		t.Error("Entries() returned a reference to internal slice, not a copy")
	}
}

func TestRoutingTable_EntriesByType(t *testing.T) {
	entries := []RouteEntry{
		{Partition: Partition{ID: "b1", ActorType: "bucket", KeyRange: KeyRange{Start: "a", End: "m"}}, Node: NodeInfo{ID: "n1"}},
		{Partition: Partition{ID: "o1", ActorType: "object", KeyRange: KeyRange{Start: "a", End: "m"}}, Node: NodeInfo{ID: "n1"}},
		{Partition: Partition{ID: "b2", ActorType: "bucket", KeyRange: KeyRange{Start: "m", End: ""}}, Node: NodeInfo{ID: "n2"}},
	}
	rt, err := NewRoutingTable(1, entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buckets := rt.EntriesByType("bucket")
	if len(buckets) != 2 {
		t.Errorf("expected 2 bucket entries, got %d", len(buckets))
	}
	objects := rt.EntriesByType("object")
	if len(objects) != 1 {
		t.Errorf("expected 1 object entry, got %d", len(objects))
	}
	none := rt.EntriesByType("nonexistent")
	if none != nil {
		t.Error("expected nil for nonexistent actor type")
	}
}
