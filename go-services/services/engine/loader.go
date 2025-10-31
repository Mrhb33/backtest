package engine

// Historical loader with checksum and gap detection (scaffold)

type GapPolicy int

const (
	GapSkip GapPolicy = iota
	GapStitch
	GapFlag
)

type LoaderConfig struct {
	GapPolicy GapPolicy
}

type Loader struct {
	cfg LoaderConfig
}

func NewLoader(cfg LoaderConfig) *Loader { return &Loader{cfg: cfg} }

// Checksum is placeholder for dataset checksum (e.g., SHA-256)
func (l *Loader) Checksum(_ []byte) string { return "" }

// DetectGaps checks for missing intervals in sorted timestamps (ms)
func (l *Loader) DetectGaps(timestamps []uint64, expectedStepMs uint64) (gaps []uint64) {
	for i := 1; i < len(timestamps); i++ {
		if timestamps[i]-timestamps[i-1] > expectedStepMs {
			gaps = append(gaps, timestamps[i-1])
		}
	}
	return gaps
}
