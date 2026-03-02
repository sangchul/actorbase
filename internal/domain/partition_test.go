package domain

import "testing"

func TestKeyRange_Contains(t *testing.T) {
	tests := []struct {
		name  string
		r     KeyRange
		key   string
		want  bool
	}{
		{name: "bounded: key in range", r: KeyRange{"b", "d"}, key: "c", want: true},
		{name: "bounded: key equals start", r: KeyRange{"b", "d"}, key: "b", want: true},
		{name: "bounded: key equals end (exclusive)", r: KeyRange{"b", "d"}, key: "d", want: false},
		{name: "bounded: key before range", r: KeyRange{"b", "d"}, key: "a", want: false},
		{name: "bounded: key after range", r: KeyRange{"b", "d"}, key: "e", want: false},
		{name: "unbounded: key at start", r: KeyRange{"b", ""}, key: "b", want: true},
		{name: "unbounded: key after start", r: KeyRange{"b", ""}, key: "zzz", want: true},
		{name: "unbounded: key before start", r: KeyRange{"b", ""}, key: "a", want: false},
		{name: "empty start unbounded: any key", r: KeyRange{"", ""}, key: "anything", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.r.Contains(tt.key); got != tt.want {
				t.Errorf("Contains(%q) = %v, want %v", tt.key, got, tt.want)
			}
		})
	}
}

func TestKeyRange_Overlaps(t *testing.T) {
	tests := []struct {
		name  string
		r     KeyRange
		other KeyRange
		want  bool
	}{
		{name: "adjacent: no overlap", r: KeyRange{"a", "b"}, other: KeyRange{"b", "c"}, want: false},
		{name: "overlapping", r: KeyRange{"a", "c"}, other: KeyRange{"b", "d"}, want: true},
		{name: "contained", r: KeyRange{"a", "d"}, other: KeyRange{"b", "c"}, want: true},
		{name: "identical", r: KeyRange{"a", "b"}, other: KeyRange{"a", "b"}, want: true},
		{name: "disjoint before", r: KeyRange{"a", "b"}, other: KeyRange{"c", "d"}, want: false},
		{name: "disjoint after", r: KeyRange{"c", "d"}, other: KeyRange{"a", "b"}, want: false},
		{name: "unbounded r overlaps bounded other", r: KeyRange{"b", ""}, other: KeyRange{"c", "d"}, want: true},
		{name: "bounded r before unbounded other", r: KeyRange{"a", "b"}, other: KeyRange{"c", ""}, want: false},
		{name: "unbounded both", r: KeyRange{"a", ""}, other: KeyRange{"b", ""}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.r.Overlaps(tt.other); got != tt.want {
				t.Errorf("Overlaps(%v) = %v, want %v", tt.other, got, tt.want)
			}
		})
	}
}
