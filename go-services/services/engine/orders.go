package engine

type OrderType int

const (
	OrderMarket OrderType = iota
	OrderLimit
	OrderStop
	OrderStopLimit
)

type TimeInForce int

const (
	TIFGTC TimeInForce = iota
	TIFIOC
	TIFFOK
)

type Order struct {
	Type     OrderType
	Side     TradeSide
	Price    float64
	Stop     float64
	Quantity float64
	TIF      TimeInForce
}

// ShouldFillLimit returns true if a limit order should fill in this bar
func ShouldFillLimit(side TradeSide, limit float64, bar Bar) bool {
	if side == TradeSideBuy {
		return bar.Low <= limit
	}
	return bar.High >= limit
}

// ShouldTriggerStop returns true if a stop should trigger in this bar
func ShouldTriggerStop(side TradeSide, stop float64, bar Bar) bool {
	if side == TradeSideBuy { // buy stop breakout up
		return bar.High >= stop
	}
	return bar.Low <= stop
}

// FillPriceLimit computes deterministic fill price for a touched limit
func FillPriceLimit(side TradeSide, limit float64, bar Bar) float64 {
	if side == TradeSideBuy {
		if bar.Low <= limit {
			if bar.Open <= limit { // gap/open through
				return bar.Open
			}
			return limit
		}
	} else {
		if bar.High >= limit {
			if bar.Open >= limit {
				return bar.Open
			}
			return limit
		}
	}
	return 0
}

// FillPriceStopMarket returns price when a stop-market triggers (next tradable)
func FillPriceStopMarket(side TradeSide, stop float64, bar Bar) float64 {
	if side == TradeSideBuy {
		if bar.High >= stop {
			if bar.Open >= stop { // gapped over
				return bar.Open
			}
			return stop
		}
	} else {
		if bar.Low <= stop {
			if bar.Open <= stop {
				return bar.Open
			}
			return stop
		}
	}
	return 0
}
