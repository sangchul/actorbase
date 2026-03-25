package provider

// Metrics is the abstraction for a metrics collection backend.
// Users can implement it directly or use the default implementation in adapter/prometheus.
type Metrics interface {
	Counter(name string, labels ...string) Counter
	Gauge(name string, labels ...string) Gauge
	Histogram(name string, labels ...string) Histogram
}

// Counter is a monotonically increasing value. (e.g., request count, error count)
type Counter interface {
	Inc(labelValues ...string)
	Add(n float64, labelValues ...string)
}

// Gauge is a value that can go up and down. (e.g., current active Actor count, queue length)
type Gauge interface {
	Set(v float64, labelValues ...string)
	Inc(labelValues ...string)
	Dec(labelValues ...string)
}

// Histogram measures a distribution of values. (e.g., request latency, message size)
type Histogram interface {
	Observe(v float64, labelValues ...string)
}
