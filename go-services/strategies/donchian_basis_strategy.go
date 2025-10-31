package strategies

import (
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

// DonchianBasisStrategy implements TV-style Donchian basis vs EMA200 entries with fixed TP/SL
type DonchianBasisStrategy struct {
	// Params
	DonchianLen    int
	EmaLen         int // fixed 200 by default
	TpPct          decimal.Decimal
	SlPct          decimal.Decimal
	EntryMode      EntryMode
	FirstTouch     FirstTouchPolicy
	MaxHoldingBars int

	// Exchange & execution
	ExchangeRules ExchangeRules
	SlippageMode  string // NONE, TRADE_SWEEP, SYNTHETIC_BOOK

	// State
	Bars           []Bar
	B              []float64 // Donchian basis
	EMA200         []float64
	ActivePosition *ActivePosition
	Trades         []Trade
	CurrentEquity  decimal.Decimal
	PeakEquity     decimal.Decimal
	MaxDrawdown    decimal.Decimal
	CadenceMs      int64

	// Scheduling for next-open entries
	NextEntry *struct {
		ActivateAtTs int64
		TradeType    string
	}

	// Traces/exports
	EnableTraces    bool
	EnableExcel     bool
	Verbose         bool
	IndicatorTraces []IndicatorTraceEntry
	ExcelRows       []ExcelRow

	// Streak tracking
	TpStreak    int
	SlStreak    int
	MaxTpStreak int
	MaxSlStreak int
}

func NewDonchianBasisStrategy() *DonchianBasisStrategy {
	return &DonchianBasisStrategy{
		DonchianLen:    20,
		EmaLen:         200,
		TpPct:          decimal.NewFromFloat(0.026),
		SlPct:          decimal.NewFromFloat(0.01),
		EntryMode:      EntryModeSignalClose,
		FirstTouch:     FirstTouchPolicySLFirst,
		MaxHoldingBars: 0,
		SlippageMode:   "TRADE_SWEEP",
		ExchangeRules: ExchangeRules{
			TickSize:       decimal.NewFromFloat(0.01),
			LotSize:        decimal.NewFromFloat(0.00001),
			MinNotional:    decimal.NewFromFloat(10.0),
			MakerFee:       decimal.NewFromFloat(0.0001),
			TakerFee:       decimal.NewFromFloat(0.001),
			PrecisionPrice: 2,
			PrecisionQty:   8,
		},
		CurrentEquity:   decimal.NewFromFloat(10000.0),
		PeakEquity:      decimal.NewFromFloat(10000.0),
		CadenceMs:       300000,
		IndicatorTraces: make([]IndicatorTraceEntry, 0),
		ExcelRows:       make([]ExcelRow, 0),
	}
}

// LoadCSV reuses EMA/ATR loader-compatible format
func (s *DonchianBasisStrategy) LoadCSV(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	r.ReuseRecord = false
	r.LazyQuotes = true

	s.Bars = s.Bars[:0]
	idx := 0
	for {
		rec, err := r.Read()
		if err != nil {
			break
		}
		if len(rec) < 6 {
			idx++
			continue
		}
		if idx == 0 && (strings.EqualFold(rec[0], "timestamp") || strings.EqualFold(rec[0], "timestamp_ms")) {
			idx++
			continue
		}

		ts, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(rec[0], "\ufeff")), 10, 64)
		if err != nil {
			idx++
			continue
		}
		open, err1 := decimal.NewFromString(strings.TrimSpace(rec[1]))
		high, err2 := decimal.NewFromString(strings.TrimSpace(rec[2]))
		low, err3 := decimal.NewFromString(strings.TrimSpace(rec[3]))
		close, err4 := decimal.NewFromString(strings.TrimSpace(rec[4]))
		vol, err5 := decimal.NewFromString(strings.TrimSpace(rec[5]))
		if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
			idx++
			continue
		}
		if err5 != nil {
			vol = decimal.Zero
		}
		s.Bars = append(s.Bars, Bar{Timestamp: ts, Open: open, High: high, Low: low, Close: close, Volume: vol})
		idx++
	}
	if len(s.Bars) > 1 {
		sort.Slice(s.Bars, func(i, j int) bool { return s.Bars[i].Timestamp < s.Bars[j].Timestamp })
	}
	return nil
}

// CalculateIndicators computes Donchian basis N and EMA200 (TV-style EMA seeding)
func (s *DonchianBasisStrategy) CalculateIndicators() error {
	n := s.DonchianLen
	total := len(s.Bars)
	if total == 0 {
		return fmt.Errorf("no bars")
	}
	s.B = make([]float64, total)
	s.EMA200 = make([]float64, total)

	// Donchian basis: midpoint of rolling HHV/LLV over highs/lows
	// B[i] = (HHV(high,N) + LLV(low,N))/2
	if n <= 0 {
		n = 20
	}
	for i := 0; i < total; i++ {
		start := i - n + 1
		if start < 0 {
			start = 0
		}
		hhv := -math.MaxFloat64
		llv := math.MaxFloat64
		for j := start; j <= i; j++ {
			h, _ := s.Bars[j].High.Float64()
			l, _ := s.Bars[j].Low.Float64()
			if h > hhv {
				hhv = h
			}
			if l < llv {
				llv = l
			}
		}
		s.B[i] = (hhv + llv) / 2.0
	}

	// EMA200 on close, seed with SMA(200)
	p := s.EmaLen
	if p < 1 {
		p = 200
	}
	if total >= p {
		var sma float64
		for i := 0; i < p; i++ {
			c, _ := s.Bars[i].Close.Float64()
			sma += c
		}
		sma /= float64(p)
		s.EMA200[p-1] = sma
		alpha := 2.0 / float64(p+1)
		oneMinus := 1.0 - alpha
		for i := p; i < total; i++ {
			c, _ := s.Bars[i].Close.Float64()
			s.EMA200[i] = c*alpha + s.EMA200[i-1]*oneMinus
		}
	}
	return nil
}

// Run processes bars sequentially with TV-like sequencing
func (s *DonchianBasisStrategy) Run() error {
	slow := s.EmaLen
	if slow < 1 {
		slow = 200
	}
	warmup := max(slow, s.DonchianLen) * 3
	if len(s.Bars) < warmup+1 {
		return fmt.Errorf("insufficient bars: need %d", warmup+1)
	}

	log.Printf("Starting Donchian-Basis backtest: warmup=%d, bars=%d", warmup, len(s.Bars))

	for i := 0; i < len(s.Bars); i++ {
		bar := s.Bars[i]

		// indicator traces
		if s.EnableTraces {
			state := "flat"
			if s.ActivePosition != nil {
				state = strings.ToLower(s.ActivePosition.TradeType)
			}
			s.IndicatorTraces = append(s.IndicatorTraces, IndicatorTraceEntry{
				Timestamp: bar.Timestamp,
				Open:      bar.Open, High: bar.High, Low: bar.Low, Close: bar.Close,
				EmaFast: s.B[i], EmaSlow: s.EMA200[i], Atr: 0, // repurpose fields to carry B and EMA200
				State: state,
			})
		}

		if i < warmup {
			continue
		}

		// Entry scheduling for next-open
		if s.ActivePosition == nil && s.NextEntry != nil && s.NextEntry.ActivateAtTs == bar.Timestamp {
			if s.NextEntry.TradeType == "Long" {
				s.enterOnBar(i, true)
			} else {
				s.enterOnBar(i, false)
			}
			s.NextEntry = nil
		}

		// Build per-candle main bar row, then SCAN and CONDITION rows
		s.logMainBarRow(i)
		s.logScanRow(i)
		s.appendConditionScannerRow(i)

		// If flat, check entry at bar close
		if s.ActivePosition == nil {
			s.checkEntry(i)
		}

		// If in position, resolve exits with first-touch using bar H/L
		if s.ActivePosition != nil {
			s.resolveExit(i)
		}

		s.updateEquity()
	}

	if s.ActivePosition != nil {
		s.closePosition(len(s.Bars)-1, "EndOfData")
	}
	return nil
}

func (s *DonchianBasisStrategy) checkEntry(i int) {
	bar := s.Bars[i]
	b := decimal.NewFromFloat(s.B[i])
	ema := decimal.NewFromFloat(s.EMA200[i])
	if s.EMA200[i] == 0 {
		return
	}

	// Long: B > EMA200 and open < B and close > B
	long := b.GreaterThan(ema) && bar.Open.LessThan(b) && bar.Close.GreaterThan(b)
	// Short: B < EMA200 and open > B and close < B
	short := b.LessThan(ema) && bar.Open.GreaterThan(b) && bar.Close.LessThan(b)

	if !long && !short {
		return
	}

	if s.EntryMode == EntryModeSignalClose {
		// add an explicit signal row for TXT parity
		s.appendSignalRow(i, long)
		s.enterOnBar(i, long)
		return
	}
	// schedule next bar open
	if i+1 < len(s.Bars) {
		s.NextEntry = &struct {
			ActivateAtTs int64
			TradeType    string
		}{ActivateAtTs: s.Bars[i+1].Timestamp, TradeType: map[bool]string{true: "Long", false: "Short"}[long]}
		s.appendSignalRow(i, long)
	}
}

func (s *DonchianBasisStrategy) enterOnBar(i int, isLong bool) {
	bar := s.Bars[i]
	// entry at close for signal_close, or open for next_open (already aligned when called)
	price := bar.Close
	side := "Long"
	if !isLong {
		side = "Short"
	}
	slip := s.calculateSlippage(price, side)
	exec := price.Add(slip)

	// fixed TP/SL percentages
	var tp, sl decimal.Decimal
	if isLong {
		tp = exec.Mul(decimal.NewFromFloat(1.0).Add(s.TpPct))
		sl = exec.Mul(decimal.NewFromFloat(1.0).Sub(s.SlPct))
	} else {
		tp = exec.Mul(decimal.NewFromFloat(1.0).Sub(s.TpPct))
		sl = exec.Mul(decimal.NewFromFloat(1.0).Add(s.SlPct))
	}

	// position size: use $1000 notional like EMA/ATR default RiskAmount
	notional := decimal.NewFromFloat(1000.0)
	qtyPre := notional.Div(exec)
	px, qty, ok := s.applyExchangeFilters(exec, qtyPre)
	if !ok {
		return
	}
	fee := s.calculateFee(px.Mul(qty), false)
	var ttl int64
	if s.MaxHoldingBars > 0 {
		ttl = int64(s.MaxHoldingBars) * s.CadenceMs
	} else {
		ttl = math.MaxInt64
	}

	s.ActivePosition = &ActivePosition{
		Symbol:        "BTCUSDT",
		TradeType:     map[bool]string{true: "Long", false: "Short"}[isLong],
		EntryTime:     bar.Timestamp,
		EntryPrice:    px,
		Quantity:      qty,
		TakeProfit:    tp.Div(exec).Mul(px), // re-quantization not strictly necessary but keep consistent
		StopLoss:      sl.Div(exec).Mul(px),
		TimeToLive:    bar.Timestamp + ttl,
		EntryFee:      fee,
		SizeUsd:       px.Mul(qty),
		EntryBarIndex: i,
	}

	if s.Verbose {
		log.Printf("Entered %s @ %s, TP=%s SL=%s Qty=%s Size=$%s", s.ActivePosition.TradeType, px.StringFixed(2), s.ActivePosition.TakeProfit.StringFixed(2), s.ActivePosition.StopLoss.StringFixed(2), qty.StringFixed(6), s.ActivePosition.SizeUsd.StringFixed(2))
	}

	if s.EnableExcel {
		ts := time.UnixMilli(bar.Timestamp).UTC().Format("2006-01-02 15:04:05")
		s.ExcelRows = append(s.ExcelRows, ExcelRow{DateTime: ts, Type: s.ActivePosition.TradeType, Entry: px.StringFixed(2), Exit: "Waiting", PnL: "Waiting", PnLPct: "Waiting", Reason: "ENTRY", TP: s.ActivePosition.TakeProfit.StringFixed(2), SL: s.ActivePosition.StopLoss.StringFixed(2), Open: bar.Open.StringFixed(2), High: bar.High.StringFixed(2), Low: bar.Low.StringFixed(2), Close: bar.Close.StringFixed(2), Volume: bar.Volume.StringFixed(2), EMA26: s.B[i], EMA100: s.EMA200[i]})
	}
}

func (s *DonchianBasisStrategy) resolveExit(i int) {
	if s.ActivePosition == nil {
		return
	}
	bar := s.Bars[i]

	// timeout (only when MaxHoldingBars > 0)
	if s.MaxHoldingBars > 0 && bar.Timestamp >= s.ActivePosition.TimeToLive {
		s.closePosition(i, "Timeout")
		return
	}

	if s.ActivePosition.TradeType == "Long" {
		hitTP := bar.High.GreaterThanOrEqual(s.ActivePosition.TakeProfit)
		hitSL := bar.Low.LessThanOrEqual(s.ActivePosition.StopLoss)
		if hitTP || hitSL {
			// SL_FIRST default
			if hitSL && (!hitTP || s.FirstTouch == FirstTouchPolicySLFirst) {
				s.closePositionAtPrice(i, s.ActivePosition.StopLoss, "StopLoss", "StopLoss")
			} else {
				s.closePositionAtPrice(i, s.ActivePosition.TakeProfit, "TakeProfit", "TakeProfit")
			}
		}
	} else {
		hitTP := bar.Low.LessThanOrEqual(s.ActivePosition.TakeProfit)
		hitSL := bar.High.GreaterThanOrEqual(s.ActivePosition.StopLoss)
		if hitTP || hitSL {
			if hitSL && (!hitTP || s.FirstTouch == FirstTouchPolicySLFirst) {
				s.closePositionAtPrice(i, s.ActivePosition.StopLoss, "StopLoss", "StopLoss")
			} else {
				s.closePositionAtPrice(i, s.ActivePosition.TakeProfit, "TakeProfit", "TakeProfit")
			}
		}
	}
}

func (s *DonchianBasisStrategy) closePosition(i int, reason string) {
	s.closePositionAtPrice(i, s.Bars[i].Close, reason, "None")
}

func (s *DonchianBasisStrategy) closePositionAtPrice(i int, exitPrice decimal.Decimal, reason, hitTpSl string) {
	if s.ActivePosition == nil {
		return
	}
	bar := s.Bars[i]
	exitFee := s.calculateFee(s.ActivePosition.Quantity.Mul(exitPrice), false)
	totalFees := s.ActivePosition.EntryFee.Add(exitFee)
	var pnl decimal.Decimal
	if s.ActivePosition.TradeType == "Long" {
		pnl = exitPrice.Sub(s.ActivePosition.EntryPrice).Mul(s.ActivePosition.Quantity).Sub(totalFees)
	} else {
		pnl = s.ActivePosition.EntryPrice.Sub(exitPrice).Mul(s.ActivePosition.Quantity).Sub(totalFees)
	}
	pnlPct := pnl.Div(s.ActivePosition.SizeUsd).Mul(decimal.NewFromFloat(100))
	barsHeld := i - s.ActivePosition.EntryBarIndex + 1

	// Update streaks based on hitTpSl
	if hitTpSl == "TakeProfit" {
		s.TpStreak++
		s.SlStreak = 0
		if s.TpStreak > s.MaxTpStreak {
			s.MaxTpStreak = s.TpStreak
		}
	} else if hitTpSl == "StopLoss" {
		s.SlStreak++
		s.TpStreak = 0
		if s.SlStreak > s.MaxSlStreak {
			s.MaxSlStreak = s.SlStreak
		}
	} else {
		s.TpStreak = 0
		s.SlStreak = 0
	}

	s.Trades = append(s.Trades, Trade{
		Date:       time.UnixMilli(bar.Timestamp).UTC().Format("2006-01-02"),
		Type:       s.ActivePosition.TradeType,
		EntryPrice: s.ActivePosition.EntryPrice,
		EntryTime:  time.UnixMilli(s.ActivePosition.EntryTime).UTC().Format("2006-01-02T15:04:05.000Z"),
		ExitPrice:  exitPrice,
		ExitTime:   time.UnixMilli(bar.Timestamp).UTC().Format("2006-01-02T15:04:05.000Z"),
		ExitReason: reason,
		HitTpSl:    hitTpSl,
		SizeUsd:    s.ActivePosition.SizeUsd,
		Qty:        s.ActivePosition.Quantity,
		FeesUsd:    totalFees,
		PnlUsd:     pnl,
		PnlPct:     pnlPct,
		Symbol:     s.ActivePosition.Symbol,
		TpPrice:    s.ActivePosition.TakeProfit,
		SlPrice:    s.ActivePosition.StopLoss,
		BarsHeld:   barsHeld,
	})

	if s.Verbose {
		log.Printf("Closed %s @ %s reason=%s PnL=%s (%s%%)", s.ActivePosition.TradeType, exitPrice.StringFixed(2), reason, pnl.StringFixed(2), pnlPct.StringFixed(2))
	}
	if s.EnableExcel {
		ts := time.UnixMilli(bar.Timestamp).UTC().Format("2006-01-02 15:04:05")
		// Augment reason with streak info for TXT export without changing columns
		reasonWithStreak := fmt.Sprintf("%s | TP_Streak=%d SL_Streak=%d", reason, s.TpStreak, s.SlStreak)
		s.ExcelRows = append(s.ExcelRows, ExcelRow{DateTime: ts, Type: s.ActivePosition.TradeType, Entry: s.ActivePosition.EntryPrice.StringFixed(2), Exit: exitPrice.StringFixed(2), PnL: pnl.StringFixed(2), PnLPct: pnlPct.StringFixed(2), Reason: reasonWithStreak, TP: s.ActivePosition.TakeProfit.StringFixed(2), SL: s.ActivePosition.StopLoss.StringFixed(2), Open: bar.Open.StringFixed(2), High: bar.High.StringFixed(2), Low: bar.Low.StringFixed(2), Close: bar.Close.StringFixed(2), Volume: bar.Volume.StringFixed(2), EMA26: s.B[i], EMA100: s.EMA200[i]})
	}
	s.ActivePosition = nil
}

func (s *DonchianBasisStrategy) updateEquity() {
	realized := decimal.Zero
	for _, t := range s.Trades {
		realized = realized.Add(t.PnlUsd)
	}
	s.CurrentEquity = decimal.NewFromFloat(10000.0).Add(realized)
	if s.CurrentEquity.GreaterThan(s.PeakEquity) {
		s.PeakEquity = s.CurrentEquity
	}
	dd := s.PeakEquity.Sub(s.CurrentEquity).Div(s.PeakEquity)
	if dd.GreaterThan(s.MaxDrawdown) {
		s.MaxDrawdown = dd
	}
}

// logScanRow appends a per-candle conditions row to ExcelRows
func (s *DonchianBasisStrategy) logScanRow(i int) {
	bar := s.Bars[i]
	b := decimal.NewFromFloat(s.B[i])
	ema := decimal.NewFromFloat(s.EMA200[i])
	if s.EMA200[i] == 0 {
		return
	}
	long := b.GreaterThan(ema) && bar.Open.LessThan(b) && bar.Close.GreaterThan(b)
	short := b.LessThan(ema) && bar.Open.GreaterThan(b) && bar.Close.LessThan(b)
	condStr := fmt.Sprintf("L[B>EMA %t & O<B %t & C>B %t] S[B<EMA %t & O>B %t & C<B %t]",
		b.GreaterThan(ema), bar.Open.LessThan(b), bar.Close.GreaterThan(b),
		b.LessThan(ema), bar.Open.GreaterThan(b), bar.Close.LessThan(b))
	ts := time.UnixMilli(bar.Timestamp).UTC().Format("2006-01-02 15:04:05")
	s.ExcelRows = append(s.ExcelRows, ExcelRow{
		DateTime:   ts,
		Type:       "SCAN",
		Entry:      "-",
		Exit:       "-",
		PnL:        "-",
		PnLPct:     "-",
		Reason:     condStr,
		TP:         "-",
		SL:         "-",
		Open:       bar.Open.StringFixed(2),
		High:       bar.High.StringFixed(2),
		Low:        bar.Low.StringFixed(2),
		Close:      bar.Close.StringFixed(2),
		Volume:     bar.Volume.StringFixed(2),
		EMA26:      s.B[i],
		EMA100:     s.EMA200[i],
		ATR:        0,
		BodyPct:    "",
		Conditions: fmt.Sprintf("L:%t S:%t", long, short),
	})
}

// appendSignalRow logs an explicit signal for readability in TXT export
func (s *DonchianBasisStrategy) appendSignalRow(i int, isLong bool) {
	bar := s.Bars[i]
	ts := time.UnixMilli(bar.Timestamp).UTC().Format("2006-01-02 15:04:05")
	sig := "SHORT_SIGNAL"
	if isLong {
		sig = "LONG_SIGNAL"
	}
	s.ExcelRows = append(s.ExcelRows, ExcelRow{
		DateTime: ts,
		Type:     "SIGNAL",
		Entry:    "-",
		Exit:     "-",
		PnL:      "-",
		PnLPct:   "-",
		Reason:   sig,
		TP:       "-",
		SL:       "-",
		Open:     bar.Open.StringFixed(2),
		High:     bar.High.StringFixed(2),
		Low:      bar.Low.StringFixed(2),
		Close:    bar.Close.StringFixed(2),
		Volume:   bar.Volume.StringFixed(2),
		EMA26:    s.B[i],
		EMA100:   s.EMA200[i],
		ATR:      0,
		BodyPct:  "",
	})
}

// logMainBarRow writes a per-candle summary row with state, OHLCV, B, EMA200, TP/SL
func (s *DonchianBasisStrategy) logMainBarRow(i int) {
	bar := s.Bars[i]
	ts := time.UnixMilli(bar.Timestamp).UTC().Format("2006-01-02 15:04:05")
	state := "Flat"
	entry := ""
	exit := ""
	pnl := ""
	pnlPct := ""
	reason := "NO_SIGNAL"
	tp := ""
	sl := ""

	if s.ActivePosition != nil {
		state = s.ActivePosition.TradeType
		entry = s.ActivePosition.EntryPrice.StringFixed(2)
		tp = s.ActivePosition.TakeProfit.StringFixed(2)
		sl = s.ActivePosition.StopLoss.StringFixed(2)
		reason = "Waiting"
	} else {
		// compute if this candle has a signal on close
		b := decimal.NewFromFloat(s.B[i])
		ema := decimal.NewFromFloat(s.EMA200[i])
		if s.EMA200[i] != 0 {
			long := b.GreaterThan(ema) && bar.Open.LessThan(b) && bar.Close.GreaterThan(b)
			short := b.LessThan(ema) && bar.Open.GreaterThan(b) && bar.Close.LessThan(b)
			if long {
				reason = "LONG_SIGNAL"
			}
			if short {
				reason = "SHORT_SIGNAL"
			}
			if !long && !short {
				reason = "NO_SIGNAL"
			}
		}
	}

	s.ExcelRows = append(s.ExcelRows, ExcelRow{
		DateTime: ts,
		Type:     state,
		Entry:    entry,
		Exit:     exit,
		PnL:      pnl,
		PnLPct:   pnlPct,
		Reason:   reason,
		TP:       tp,
		SL:       sl,
		Open:     bar.Open.StringFixed(2),
		High:     bar.High.StringFixed(2),
		Low:      bar.Low.StringFixed(2),
		Close:    bar.Close.StringFixed(2),
		Volume:   bar.Volume.StringFixed(2),
		// Reuse columns: EMA26 -> Donchian B, EMA100 -> EMA200
		EMA26:   s.B[i],
		EMA100:  s.EMA200[i],
		ATR:     0,
		BodyPct: "",
	})
}

// appendConditionScannerRow adds a compact boolean scanner row right under candle data
func (s *DonchianBasisStrategy) appendConditionScannerRow(i int) {
	bar := s.Bars[i]
	b := decimal.NewFromFloat(s.B[i])
	ema := decimal.NewFromFloat(s.EMA200[i])
	if s.EMA200[i] == 0 {
		return
	}
	condBgt := b.GreaterThan(ema)
	condOlt := bar.Open.LessThan(b)
	condCgt := bar.Close.GreaterThan(b)
	condBlt := b.LessThan(ema)
	condOgt := bar.Open.GreaterThan(b)
	condClt := bar.Close.LessThan(b)
	long := condBgt && condOlt && condCgt
	short := condBlt && condOgt && condClt

	ts := time.UnixMilli(bar.Timestamp).UTC().Format("2006-01-02 15:04:05")
	reason := fmt.Sprintf(
		"B=%.2f EMA200=%.2f | B>EMA=%t O<B=%t C>B=%t | LONG_SIGNAL=%t | SHORT_SIGNAL=%t",
		s.B[i], s.EMA200[i], condBgt, condOlt, condCgt, long, short,
	)

	s.ExcelRows = append(s.ExcelRows, ExcelRow{
		DateTime:   ts,
		Type:       "CONDITION",
		Entry:      "-",
		Exit:       "-",
		PnL:        "-",
		PnLPct:     "-",
		Reason:     reason,
		TP:         "-",
		SL:         "-",
		Open:       bar.Open.StringFixed(2),
		High:       "-",
		Low:        "-",
		Close:      bar.Close.StringFixed(2),
		Volume:     "-",
		EMA26:      0,
		EMA100:     0,
		ATR:        0,
		BodyPct:    "",
		Conditions: reason,
	})
}

// Helpers reused from EMA/ATR implementation
func (s *DonchianBasisStrategy) applyExchangeFilters(price, quantity decimal.Decimal) (decimal.Decimal, decimal.Decimal, bool) {
	filteredPrice := price.Div(s.ExchangeRules.TickSize).Round(0).Mul(s.ExchangeRules.TickSize)
	filteredQty := quantity.Div(s.ExchangeRules.LotSize).Round(0).Mul(s.ExchangeRules.LotSize)
	if filteredPrice.Mul(filteredQty).LessThan(s.ExchangeRules.MinNotional) {
		return filteredPrice, filteredQty, false
	}
	return filteredPrice, filteredQty, true
}

func (s *DonchianBasisStrategy) calculateSlippage(basePrice decimal.Decimal, side string) decimal.Decimal {
	switch s.SlippageMode {
	case "NONE":
		return decimal.Zero
	case "TRADE_SWEEP":
		rate := decimal.NewFromFloat(0.0001)
		if side == "Long" {
			return basePrice.Mul(rate)
		}
		return basePrice.Mul(rate.Neg())
	case "SYNTHETIC_BOOK":
		rate := decimal.NewFromFloat(0.0005)
		if side == "Long" {
			return basePrice.Mul(rate)
		}
		return basePrice.Mul(rate.Neg())
	default:
		return decimal.Zero
	}
}

func (s *DonchianBasisStrategy) calculateFee(notional decimal.Decimal, isMaker bool) decimal.Decimal {
	feeRate := s.ExchangeRules.TakerFee
	if isMaker {
		feeRate = s.ExchangeRules.MakerFee
	}
	return notional.Mul(feeRate)
}
