package engine

type AccountPosition struct {
	Side        PositionSide
	Quantity    float64
	AvgPrice    float64
	RealizedPnl float64
}

// ApplyFill updates position with a new fill
func (p *AccountPosition) ApplyFill(side TradeSide, price, qty float64) {
	if qty == 0 {
		return
	}
	if side == TradeSideBuy {
		if p.Side == SideShort {
			// reduce/flip
			realized := (p.AvgPrice - price) * min(absf(p.Quantity), qty)
			p.RealizedPnl += realized
			p.Quantity -= qty
			if p.Quantity < 0 {
				// flipped to long
				p.Side = SideLong
				p.AvgPrice = price
				p.Quantity = -p.Quantity
			} else if p.Quantity == 0 {
				p.Side = SideFlat
				p.AvgPrice = 0
			}
		} else {
			// add long
			newQty := p.Quantity + qty
			p.AvgPrice = weightedAvg(p.AvgPrice, p.Quantity, price, qty)
			p.Quantity = newQty
			p.Side = SideLong
		}
	} else { // sell
		if p.Side == SideLong {
			realized := (price - p.AvgPrice) * min(absf(p.Quantity), qty)
			p.RealizedPnl += realized
			p.Quantity -= qty
			if p.Quantity < 0 {
				p.Side = SideShort
				p.AvgPrice = price
				p.Quantity = -p.Quantity
			} else if p.Quantity == 0 {
				p.Side = SideFlat
				p.AvgPrice = 0
			}
		} else {
			newQty := p.Quantity + qty
			p.AvgPrice = weightedAvg(p.AvgPrice, p.Quantity, price, qty)
			p.Quantity = newQty
			p.Side = SideShort
		}
	}
}

func weightedAvg(p1, q1, p2, q2 float64) float64 {
	if q1+q2 == 0 {
		return 0
	}
	return (p1*q1 + p2*q2) / (q1 + q2)
}

func absf(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
