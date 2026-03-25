// Package json provides a JSON-based implementation of provider.Codec.
package json

import "encoding/json"

// Codec is a Codec implementation based on encoding/json.
type Codec struct{}

// New returns a JSON Codec.
func New() *Codec {
	return &Codec{}
}

func (c *Codec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (c *Codec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
