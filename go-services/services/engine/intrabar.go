package engine

// IntrabarPath defines the order in which prices are touched within a bar
type IntrabarPath int

const (
	PathOpenExtremumOtherClose IntrabarPath = iota // synthetic path
)

// Bar represents a single OHLCV bar
type Bar struct {
	Open  float64
	High  float64
	Low   float64
	Close float64
}

// FirstTouchResult indicates which level was hit first
type FirstTouchResult int

const (
	TouchNone FirstTouchResult = iota
	TouchTP
	TouchSL
)

// ResolveFirstTouch determines TP/SL hit order for a long position using synthetic path
func ResolveFirstTouchLong(bar Bar, tp, sl float64) FirstTouchResult {
	// Both hit in same bar
	both := bar.Low <= sl && bar.High >= tp
	if both {
		// Determine which extremum is closer to open (synthetic path order)
		distHigh := abs(bar.High - bar.Open)
		distLow := abs(bar.Open - bar.Low)
		if distLow < distHigh {
			return TouchSL
		}
		return TouchTP
	}
	if bar.Low <= sl {
		return TouchSL
	}
	if bar.High >= tp {
		return TouchTP
	}
	return TouchNone
}

// ResolveFirstTouchShort mirrors the long logic for shorts
func ResolveFirstTouchShort(bar Bar, tp, sl float64) FirstTouchResult {
	both := bar.High >= sl && bar.Low <= tp
	if both {
		distHigh := abs(bar.High - bar.Open)
		distLow := abs(bar.Open - bar.Low)
		if distHigh < distLow {
			return TouchSL
		}
		return TouchTP
	}
	if bar.High >= sl {
		return TouchSL
	}
	if bar.Low <= tp {
		return TouchTP
	}
	return TouchNone
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
