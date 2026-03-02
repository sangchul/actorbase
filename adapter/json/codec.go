// Package json provides a JSON-based implementation of provider.Codec.
package json

import "encoding/json"

// Codec은 encoding/json 기반 Codec 구현체.
type Codec struct{}

// New는 JSON Codec을 반환한다.
func New() *Codec {
	return &Codec{}
}

func (c *Codec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (c *Codec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
