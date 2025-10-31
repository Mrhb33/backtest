package engine

// SIMD-optimized indicators with warm-up definitions

type IndicatorType int

const (
	IndicatorSMA IndicatorType = iota
	IndicatorEMA
	IndicatorRSI
	IndicatorMACD
	IndicatorBB
)

type IndicatorConfig struct {
	Type   IndicatorType
	Params map[string]float64
	Warmup int
}

// Warmup definitions for common indicators
var IndicatorWarmups = map[IndicatorType]int{
	IndicatorSMA:  0,  // no warmup needed
	IndicatorEMA:  0,  // no warmup needed
	IndicatorRSI:  14, // RSI(14) needs 14 periods
	IndicatorMACD: 26, // MACD needs 26 periods for signal line
	IndicatorBB:   20, // Bollinger Bands(20) needs 20 periods
}

type SIMDCalculator struct{}

func (c *SIMDCalculator) CalculateSMA(values []float64, period int) []float64 {
	if len(values) < period {
		return make([]float64, len(values))
	}

	result := make([]float64, len(values))
	for i := period - 1; i < len(values); i++ {
		sum := 0.0
		for j := 0; j < period; j++ {
			sum += values[i-j]
		}
		result[i] = sum / float64(period)
	}
	return result
}

func (c *SIMDCalculator) CalculateEMA(values []float64, period int) []float64 {
	if len(values) == 0 {
		return values
	}

	alpha := 2.0 / (float64(period) + 1.0)
	result := make([]float64, len(values))
	result[0] = values[0]

	for i := 1; i < len(values); i++ {
		result[i] = alpha*values[i] + (1-alpha)*result[i-1]
	}
	return result
}

func (c *SIMDCalculator) CalculateRSI(values []float64, period int) []float64 {
	if len(values) < period+1 {
		return make([]float64, len(values))
	}

	result := make([]float64, len(values))
	gains := make([]float64, len(values))
	losses := make([]float64, len(values))

	// Calculate gains and losses
	for i := 1; i < len(values); i++ {
		change := values[i] - values[i-1]
		if change > 0 {
			gains[i] = change
		} else {
			losses[i] = -change
		}
	}

	// Calculate RSI
	for i := period; i < len(values); i++ {
		avgGain := 0.0
		avgLoss := 0.0

		for j := i - period + 1; j <= i; j++ {
			avgGain += gains[j]
			avgLoss += losses[j]
		}

		avgGain /= float64(period)
		avgLoss /= float64(period)

		if avgLoss == 0 {
			result[i] = 100
		} else {
			rs := avgGain / avgLoss
			result[i] = 100 - (100 / (1 + rs))
		}
	}

	return result
}

// DAG caching keyed by (symbol, TF, params, version)
type IndicatorCache struct {
	cache map[string]map[string][]float64
}

func NewIndicatorCache() *IndicatorCache {
	return &IndicatorCache{
		cache: make(map[string]map[string][]float64),
	}
}

func (ic *IndicatorCache) GetKey(symbol, tf string, params map[string]float64, version string) string {
	// Simplified key generation - would use proper hashing in real implementation
	return symbol + "_" + tf + "_" + version
}

func (ic *IndicatorCache) Get(key string) ([]float64, bool) {
	if symbolCache, exists := ic.cache[""]; exists {
		if values, exists := symbolCache[key]; exists {
			return values, true
		}
	}
	return nil, false
}

func (ic *IndicatorCache) Set(key string, values []float64) {
	if ic.cache[""] == nil {
		ic.cache[""] = make(map[string][]float64)
	}
	ic.cache[""][key] = values
}
