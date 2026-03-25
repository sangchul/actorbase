package provider

// Codec is the abstraction for request/response serialization and deserialization.
// The same implementation is injected on both the SDK and PS sides.
// Users can implement it directly or use the default implementation in adapter/json.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}
