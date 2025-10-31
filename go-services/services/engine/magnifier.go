package engine

// MicroBar represents a 1s or tick micro-bar
type MicroBar struct {
	Ts    uint64
	Open  float64
	High  float64
	Low   float64
	Close float64
}

// BuildSyntheticPath returns ordered price touches for a bar: open -> nearer extremum -> other extremum -> close
func BuildSyntheticPath(bar Bar) []float64 {
	path := []float64{bar.Open}
	// choose which extremum is closer to open
	if abs(bar.Open-bar.Low) < abs(bar.High-bar.Open) {
		path = append(path, bar.Low, bar.High)
	} else {
		path = append(path, bar.High, bar.Low)
	}
	path = append(path, bar.Close)
	return path
}

// FirstTouchInMicros resolves TP/SL using actual microbars when available
func FirstTouchInMicros(side PositionSide, micros []MicroBar, tp, sl float64) FirstTouchResult {
	for _, mb := range micros {
		// within each microbar, assume open->extremum nearer open->other->close
		bar := Bar{Open: mb.Open, High: mb.High, Low: mb.Low, Close: mb.Close}
		if side == SideLong {
			r := ResolveFirstTouchLong(bar, tp, sl)
			if r != TouchNone {
				return r
			}
		} else if side == SideShort {
			r := ResolveFirstTouchShort(bar, tp, sl)
			if r != TouchNone {
				return r
			}
		}
	}
	return TouchNone
}
