package provider

// Codec은 요청/응답 직렬화·역직렬화 추상화.
// SDK와 PS 양측에 동일한 구현체를 주입한다.
// 사용자가 직접 구현하거나, adapter/json 기본 구현체를 사용한다.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}
