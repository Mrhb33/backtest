package monitoring

type Metrics struct{}

func NewMetrics(cfg any) (*Metrics, error) { return &Metrics{}, nil }

func (m *Metrics) GetMetrics() string { return "" }
