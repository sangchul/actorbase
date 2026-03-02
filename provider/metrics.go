package provider

// Metrics는 메트릭 수집 백엔드의 추상화.
// 사용자가 직접 구현하거나, adapter/prometheus 기본 구현체를 사용한다.
type Metrics interface {
	Counter(name string, labels ...string) Counter
	Gauge(name string, labels ...string) Gauge
	Histogram(name string, labels ...string) Histogram
}

// Counter는 단조 증가하는 값. (요청 수, 에러 수 등)
type Counter interface {
	Inc(labelValues ...string)
	Add(n float64, labelValues ...string)
}

// Gauge는 증감 가능한 값. (현재 활성 Actor 수, 큐 길이 등)
type Gauge interface {
	Set(v float64, labelValues ...string)
	Inc(labelValues ...string)
	Dec(labelValues ...string)
}

// Histogram은 분포를 측정하는 값. (요청 latency, 메시지 크기 등)
type Histogram interface {
	Observe(v float64, labelValues ...string)
}
