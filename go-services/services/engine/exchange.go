package engine

// Exchange filters and fees/slippage (interfaces)

type SymbolFilters struct {
	PriceTick   float64
	LotStep     float64
	NotionalMin float64
	QtyStep     float64
}

type FeeTier struct {
	Maker float64 // as fraction
	Taker float64
}

type FeeModel interface {
	Compute(side TradeSide, price, qty float64, maker bool) float64
}

type FixedFeeModel struct{ Maker, Taker float64 }

func (m FixedFeeModel) Compute(_ TradeSide, price, qty float64, maker bool) float64 {
	rate := m.Taker
	if maker {
		rate = m.Maker
	}
	return price * qty * rate
}

type SlippageModel interface {
	Apply(side TradeSide, price float64) float64
}

type FixedTicksSlippage struct{ Ticks float64 }

func (s FixedTicksSlippage) Apply(side TradeSide, price float64) float64 {
	if side == TradeSideBuy {
		return price + s.Ticks
	}
	return price - s.Ticks
}

// EnforceFilters rounds/validates order params to symbol constraints
func EnforceFilters(f SymbolFilters, price, qty float64) (float64, float64) {
	if f.PriceTick > 0 {
		price = roundStep(price, f.PriceTick)
	}
	if f.QtyStep > 0 {
		qty = roundStep(qty, f.QtyStep)
	}
	if f.NotionalMin > 0 && price*qty < f.NotionalMin {
		qty = f.NotionalMin / max(price, 1e-12)
		if f.QtyStep > 0 {
			qty = roundStep(qty, f.QtyStep)
		}
	}
	return price, qty
}

func roundStep(v, step float64) float64 {
	if step <= 0 {
		return v
	}
	n := v / step
	r := float64(int64(n+0.5)) * step
	if r == 0 {
		return 0
	}
	return r
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
