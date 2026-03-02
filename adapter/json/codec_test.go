package json

import (
	"testing"
)

type sample struct {
	Key   string `json:"key"`
	Value int    `json:"value"`
}

func TestCodec_MarshalUnmarshal(t *testing.T) {
	c := New()

	in := sample{Key: "hello", Value: 42}
	data, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var out sample
	if err := c.Unmarshal(data, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if out.Key != in.Key || out.Value != in.Value {
		t.Errorf("roundtrip mismatch: got %+v, want %+v", out, in)
	}
}

func TestCodec_Unmarshal_InvalidJSON(t *testing.T) {
	c := New()
	var out sample
	if err := c.Unmarshal([]byte("not-json"), &out); err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}
