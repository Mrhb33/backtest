package engine

// Futures simulation with leverage, margin, funding

type FuturesConfig struct {
	Leverage          float64
	MaintenanceMargin float64
	InitialMargin     float64
	FundingRate       float64
}

type FuturesPosition struct {
	Side             PositionSide
	Quantity         float64
	EntryPrice       float64
	MarkPrice        float64
	UnrealizedPnl    float64
	MarginUsed       float64
	LiquidationPrice float64
}

func (fp *FuturesPosition) UpdateMarkPrice(markPrice float64, cfg FuturesConfig) {
	fp.MarkPrice = markPrice

	// Calculate unrealized PnL
	if fp.Side == SideLong {
		fp.UnrealizedPnl = (markPrice - fp.EntryPrice) * fp.Quantity
	} else if fp.Side == SideShort {
		fp.UnrealizedPnl = (fp.EntryPrice - markPrice) * fp.Quantity
	}

	// Calculate margin used
	notional := markPrice * fp.Quantity
	fp.MarginUsed = notional / cfg.Leverage

	// Calculate liquidation price
	if fp.Side == SideLong {
		fp.LiquidationPrice = fp.EntryPrice * (1 - (1 / cfg.Leverage) + cfg.MaintenanceMargin)
	} else if fp.Side == SideShort {
		fp.LiquidationPrice = fp.EntryPrice * (1 + (1 / cfg.Leverage) - cfg.MaintenanceMargin)
	}
}

func (fp *FuturesPosition) IsLiquidated() bool {
	if fp.Side == SideFlat {
		return false
	}

	if fp.Side == SideLong {
		return fp.MarkPrice <= fp.LiquidationPrice
	}
	return fp.MarkPrice >= fp.LiquidationPrice
}
