package strategies

import (
    "bufio"
    "encoding/csv"
    "fmt"
    "io"
    "log"
    "os"
    "sort"
    "strconv"
    "strings"
    "time"

    "github.com/shopspring/decimal"
)

// IchimokuBaselineStrategy trades close vs Kijun-sen baseline with fixed TP/SL
type IchimokuBaselineStrategy struct {
    // Params
    KijunLen        int // default 26
    WarmupBars      int // default 3x KijunLen (78)
    EntryMode       EntryMode
    FirstTouch      FirstTouchPolicy
    IntraExitsOnEntry bool // for next-open entries: allow exits on same bar
    TpPct           decimal.Decimal // +1.9% long, -1.9% short
    SlPct           decimal.Decimal // -0.8% long, +0.8% short

    // Exchange & execution
    ExchangeRules   ExchangeRules
    SlippageMode    string // NONE, TRADE_SWEEP, SYNTHETIC_BOOK

    // State
    Bars            []Bar
    Kijun           []float64
    ActivePosition  *ActivePosition
    Trades          []Trade
    InitialEquity   decimal.Decimal
    CurrentEquity   decimal.Decimal
    PeakEquity      decimal.Decimal
    MaxDrawdown     decimal.Decimal
    CadenceMs       int64
    RiskFraction    decimal.Decimal // portion of equity to trade each entry (0..1)
    EquityCurve     []decimal.Decimal

    // Next-entry scheduling
    NextEntry *struct {
        ActivateAtTs int64
        TradeType    string // Long/Short
    }

    // Streaks
    TpStreak    int
    SlStreak    int
    MaxTpStreak int
    MaxSlStreak int

    // Traces/exports
    EnableTraces bool
    EnableExcel  bool
    Verbose      bool
    ExcelRows    []ExcelRow
}

func NewIchimokuBaselineStrategy() *IchimokuBaselineStrategy {
    return &IchimokuBaselineStrategy{
        KijunLen:   26,
        WarmupBars: 78,
        EntryMode:  EntryModeNextBarOpen,
        FirstTouch: FirstTouchPolicySLFirst,
        IntraExitsOnEntry: false,
        TpPct:      decimal.NewFromFloat(0.019),
        SlPct:      decimal.NewFromFloat(0.009),
        ExchangeRules: ExchangeRules{
            TickSize:       decimal.NewFromFloat(0.01),
            LotSize:        decimal.NewFromFloat(0.00001),
            MinNotional:    decimal.NewFromFloat(10.0),
            MakerFee:       decimal.NewFromFloat(0.0001),
            TakerFee:       decimal.NewFromFloat(0.001),
            PrecisionPrice: 2,
            PrecisionQty:   8,
        },
        SlippageMode:  "TRADE_SWEEP",
        InitialEquity: decimal.NewFromFloat(1000.0),
        CurrentEquity: decimal.NewFromFloat(1000.0),
        PeakEquity:    decimal.NewFromFloat(1000.0),
        CadenceMs:     300000,
        RiskFraction:  decimal.NewFromFloat(1.0),
        EquityCurve:   make([]decimal.Decimal, 0, 256),
        ExcelRows:     make([]ExcelRow, 0),
    }
}

// LoadCSV loads OHLCV data (timestamp,open,high,low,close,volume)
func (s *IchimokuBaselineStrategy) LoadCSV(filename string) error {
    f, err := os.Open(filename)
    if err != nil { return err }
    defer f.Close()
    r := csv.NewReader(bufio.NewReader(f))
    r.FieldsPerRecord = -1
    r.ReuseRecord = false
    r.LazyQuotes = true

    s.Bars = s.Bars[:0]
    idx := 0
    for {
        rec, err := r.Read()
        if err == io.EOF { break }
        if err != nil { idx++; continue }
        if len(rec) < 6 { idx++; continue }
        if idx == 0 && (strings.EqualFold(rec[0], "timestamp") || strings.EqualFold(rec[0], "timestamp_ms")) { idx++; continue }

        ts, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(rec[0], "\ufeff")), 10, 64)
        if err != nil { idx++; continue }
        o, e1 := decimal.NewFromString(strings.TrimSpace(rec[1]))
        h, e2 := decimal.NewFromString(strings.TrimSpace(rec[2]))
        l, e3 := decimal.NewFromString(strings.TrimSpace(rec[3]))
        c, e4 := decimal.NewFromString(strings.TrimSpace(rec[4]))
        v, e5 := decimal.NewFromString(strings.TrimSpace(rec[5]))
        if e1 != nil || e2 != nil || e3 != nil || e4 != nil { idx++; continue }
        if e5 != nil { v = decimal.Zero }
        s.Bars = append(s.Bars, Bar{Timestamp: ts, Open: o, High: h, Low: l, Close: c, Volume: v})
        idx++
    }
    if len(s.Bars) > 1 {
        sort.Slice(s.Bars, func(i, j int) bool { return s.Bars[i].Timestamp < s.Bars[j].Timestamp })
    }
    return nil
}

// CalculateIndicators computes Kijun-sen over an inclusive 26-bar window
func (s *IchimokuBaselineStrategy) CalculateIndicators() error {
    if s.KijunLen <= 0 { s.KijunLen = 26 }
    n := s.KijunLen
    total := len(s.Bars)
    if total < n {
        return fmt.Errorf("insufficient bars for Kijun: need >= %d", n)
    }
    s.Kijun = make([]float64, total)
    for i := 0; i < total; i++ {
        start := i - n + 1
        if start < 0 { start = 0 }
        hhv := -1.0e300
        llv := 1.0e300
        for j := start; j <= i; j++ {
            hh, _ := s.Bars[j].High.Float64()
            ll, _ := s.Bars[j].Low.Float64()
            if hh > hhv { hhv = hh }
            if ll < llv { llv = ll }
        }
        s.Kijun[i] = (hhv + llv) / 2.0
    }
    return nil
}

// Run executes bar-by-bar with warm-up gating and TP/SL tracker
func (s *IchimokuBaselineStrategy) Run() error {
    warmup := s.WarmupBars
    if warmup < s.KijunLen*3 { warmup = s.KijunLen * 3 }
    if len(s.Bars) < warmup+1 { return fmt.Errorf("insufficient bars: need %d", warmup+1) }

    log.Printf("Starting Ichimoku Baseline backtest: warmup=%d, bars=%d", warmup, len(s.Bars))
    loggedWarmupComplete := false
    for i := 0; i < len(s.Bars); i++ {
        bar := s.Bars[i]

        // Warm-up phase: compute indicators only, show progress, no table rows
        if i < warmup {
            if i%1000 == 0 || i == 0 || i == warmup-1 {
                pct := float64(i+1) / float64(warmup) * 100.0
                log.Printf("WARM-UP: %d/%d (%.1f%%) - Time: %s Close: %s Kijun: %.2f",
                    i+1, warmup,
                    pct,
                    time.UnixMilli(bar.Timestamp).UTC().Format("2006-01-02 15:04:05"),
                    bar.Close.StringFixed(2), s.Kijun[i])
            }
            continue
        }

        if !loggedWarmupComplete {
            log.Printf("✅ Warm-up complete. Entering trading phase at bar %d.", i)
            if s.Verbose {
                log.Printf("┌─────────────────────┬──────┬──────────┬──────────┬──────────┬────────┬──────────────┬──────────┬──────────┬────────┬────────┬────────┬────────┬────────┬─────────────┬─────────────┬──────────┬────────┐")
                log.Printf("│ Date & Time         │ Type │ Entry    │ Exit     │ PnL      │ PnL%%  │ Reason       │ TP       │ SL       │ Open   │ High   │ Low    │ Close  │ Volume │ Kijun       │ -           │ -        │  -     │")
                log.Printf("├─────────────────────┼──────┼──────────┼──────────┼──────────┼────────┼──────────────┼──────────┼──────────┼────────┼────────┼────────┼────────┼────────┼─────────────┼─────────────┼──────────┼────────┤")
            }
            loggedWarmupComplete = true
        }

        // Build table rows only during trading phase
        s.logMainBarRow(i)
        s.logScanRow(i)

        // Entry scheduling
        if s.ActivePosition == nil && s.NextEntry != nil && s.NextEntry.ActivateAtTs == bar.Timestamp {
            s.enterOnBar(i, s.NextEntry.TradeType == "Long")
            s.NextEntry = nil
        }

        // Entry signal on close if flat
        if s.ActivePosition == nil {
            kij := decimal.NewFromFloat(s.Kijun[i])
            longSig := bar.Open.LessThan(kij) && bar.Close.GreaterThan(kij)
            shortSig := bar.Open.GreaterThan(kij) && bar.Close.LessThan(kij)
            if longSig {
                if s.EntryMode == EntryModeSignalClose {
                    s.enterOnBar(i, true)
                } else {
                    if i+1 < len(s.Bars) {
                        s.NextEntry = &struct{ ActivateAtTs int64; TradeType string }{ActivateAtTs: s.Bars[i+1].Timestamp, TradeType: "Long"}
                    }
                }
            } else if shortSig {
                if s.EntryMode == EntryModeSignalClose {
                    s.enterOnBar(i, false)
                } else {
                    if i+1 < len(s.Bars) {
                        s.NextEntry = &struct{ ActivateAtTs int64; TradeType string }{ActivateAtTs: s.Bars[i+1].Timestamp, TradeType: "Short"}
                    }
                }
            }
        }

        // Exit resolution when in position
        if s.ActivePosition != nil {
            s.resolveExit(i)
        }

        s.updateEquity()
    }

    if s.ActivePosition != nil {
        s.closePosition(len(s.Bars)-1, "EndOfData")
    }
    if s.Verbose {
        log.Printf("└─────────────────────┴──────┴──────────┴──────────┴──────────┴────────┴──────────────┴──────────┴──────────┴────────┴────────┴────────┴────────┴────────┴─────────────┴─────────────┴──────────┴────────┘")
    }
    return nil
}

func (s *IchimokuBaselineStrategy) logMainBarRow(i int) {
    if !s.EnableExcel { return }
    bar := s.Bars[i]
    ts := time.UnixMilli(bar.Timestamp).UTC().Format("2006-01-02 15:04:05")
    state := "Flat"
    entry, exit, pnl, pnlPct := "", "", "", ""
    reason := "NO_SIGNAL"
    tp, sl := "", ""
    // Build condition string for visibility
    kij := decimal.NewFromFloat(s.Kijun[i])
    condStr := fmt.Sprintf("O<Kij:%t C>Kij:%t | O>Kij:%t C<Kij:%t", bar.Open.LessThan(kij), bar.Close.GreaterThan(kij), bar.Open.GreaterThan(kij), bar.Close.LessThan(kij))

    if s.ActivePosition != nil {
        state = s.ActivePosition.TradeType
        entry = s.ActivePosition.EntryPrice.StringFixed(2)
        tp = s.ActivePosition.TakeProfit.StringFixed(2)
        sl = s.ActivePosition.StopLoss.StringFixed(2)
        reason = "Waiting"
    } else {
        o := s.Bars[i].Open
        c := s.Bars[i].Close
        if o.LessThan(kij) && c.GreaterThan(kij) { reason = "LONG_SIGNAL" }
        if o.GreaterThan(kij) && c.LessThan(kij) { reason = "SHORT_SIGNAL" }
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
        EMA26:    s.Kijun[i], // reuse column to display Kijun
        EMA100:   0,
        ATR:      0,
        BodyPct:  "",
        Conditions: condStr,
    })

    if s.Verbose {
        // Print console row similar to EMA/ATR format, reusing slots
        log.Printf("| %s | %-4s | %-8s | %-8s | %-8s | %-6s | %-12s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | Kijun: %7.2f | Cond: %-24s | -         |",
            ts, state, entry, exit, pnl, pnlPct, reason, tp, sl,
            bar.Open.StringFixed(2), bar.High.StringFixed(2), bar.Low.StringFixed(2), bar.Close.StringFixed(2), bar.Volume.StringFixed(2),
            s.Kijun[i], condStr)
    }
}

// logScanRow appends and prints a condition row under the main bar row
func (s *IchimokuBaselineStrategy) logScanRow(i int) {
    if !s.EnableExcel { return }
    bar := s.Bars[i]
    ts := ""
    // Conditions (crossover on current bar)
    kij := decimal.NewFromFloat(s.Kijun[i])
    condStr := fmt.Sprintf("L[O<Kij %t & C>Kij %t] S[O>Kij %t & C<Kij %t]", bar.Open.LessThan(kij), bar.Close.GreaterThan(kij), bar.Open.GreaterThan(kij), bar.Close.LessThan(kij))

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
        Open:       "-",
        High:       "-",
        Low:        "-",
        Close:      "-",
        Volume:     "-",
        EMA26:      0,
        EMA100:     0,
        ATR:        0,
        BodyPct:    "",
        Conditions: condStr,
    })

    if s.Verbose {
        log.Printf("| %-19s | %-4s | %-8s | %-8s | %-8s | %-6s | %-12s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | %13s | %13s | %10s | %6s |",
            "", "SCAN", "-", "-", "-", "-", condStr, "-", "-",
            "-", "-", "-", "-", "-", "-", "-", "-", "")
    }
}

func (s *IchimokuBaselineStrategy) enterOnBar(i int, isLong bool) {
    bar := s.Bars[i]
    price := bar.Close
    if s.EntryMode == EntryModeNextBarOpen { price = bar.Open }
    side := "Long"
    if !isLong { side = "Short" }

    // slippage
    slip := s.calculateSlippage(price, side)
    exec := price.Add(slip)

    // TP/SL absolute targets
    var tp, sl decimal.Decimal
    if isLong {
        tp = exec.Mul(decimal.NewFromFloat(1).Add(s.TpPct))
        sl = exec.Mul(decimal.NewFromFloat(1).Sub(s.SlPct))
    } else {
        tp = exec.Mul(decimal.NewFromFloat(1).Sub(s.TpPct))
        sl = exec.Mul(decimal.NewFromFloat(1).Add(s.SlPct))
    }

    // position size based on current equity and risk fraction (compounding)
    notional := s.CurrentEquity.Mul(s.RiskFraction)
    qtyPre := notional.Div(exec)
    px, qty, ok := s.applyExchangeFilters(exec, qtyPre)
    if !ok { return }
    fee := s.calculateFee(px.Mul(qty), false)

    // quantize TP/SL to tick
    tp = tp.Div(s.ExchangeRules.TickSize).Round(0).Mul(s.ExchangeRules.TickSize)
    sl = sl.Div(s.ExchangeRules.TickSize).Round(0).Mul(s.ExchangeRules.TickSize)

    s.ActivePosition = &ActivePosition{
        Symbol:        "BTCUSDT",
        TradeType:     side,
        EntryTime:     bar.Timestamp,
        EntryPrice:    px,
        Quantity:      qty,
        TakeProfit:    tp,
        StopLoss:      sl,
        TimeToLive:    mathMaxInt64(),
        EntryFee:      fee,
        SizeUsd:       px.Mul(qty),
        EntryBarIndex: i,
    }
}

func (s *IchimokuBaselineStrategy) resolveExit(i int) {
    if s.ActivePosition == nil { return }
    bar := s.Bars[i]
    // Same-bar guard for next-open entries
    if s.EntryMode == EntryModeNextBarOpen && !s.IntraExitsOnEntry && i == s.ActivePosition.EntryBarIndex { return }

    if s.ActivePosition.TradeType == "Long" {
        hitTP := bar.High.GreaterThanOrEqual(s.ActivePosition.TakeProfit)
        hitSL := bar.Low.LessThanOrEqual(s.ActivePosition.StopLoss)
        if hitTP || hitSL {
            if hitSL && (!hitTP || s.FirstTouch == FirstTouchPolicySLFirst) {
                s.closePositionAtPrice(i, s.ActivePosition.StopLoss, "StopLoss")
            } else {
                s.closePositionAtPrice(i, s.ActivePosition.TakeProfit, "TakeProfit")
            }
        }
    } else { // Short
        hitTP := bar.Low.LessThanOrEqual(s.ActivePosition.TakeProfit)
        hitSL := bar.High.GreaterThanOrEqual(s.ActivePosition.StopLoss)
        if hitTP || hitSL {
            if hitSL && (!hitTP || s.FirstTouch == FirstTouchPolicySLFirst) {
                s.closePositionAtPrice(i, s.ActivePosition.StopLoss, "StopLoss")
            } else {
                s.closePositionAtPrice(i, s.ActivePosition.TakeProfit, "TakeProfit")
            }
        }
    }
}

func (s *IchimokuBaselineStrategy) closePosition(i int, reason string) {
    s.closePositionAtPrice(i, s.Bars[i].Close, reason)
}

func (s *IchimokuBaselineStrategy) closePositionAtPrice(i int, exitPrice decimal.Decimal, reason string) {
    if s.ActivePosition == nil { return }
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

    // Update streaks
    if reason == "TakeProfit" {
        s.TpStreak++; s.SlStreak = 0
        if s.TpStreak > s.MaxTpStreak { s.MaxTpStreak = s.TpStreak }
    } else if reason == "StopLoss" {
        s.SlStreak++; s.TpStreak = 0
        if s.SlStreak > s.MaxSlStreak { s.MaxSlStreak = s.SlStreak }
    } else {
        s.TpStreak = 0; s.SlStreak = 0
    }

    s.Trades = append(s.Trades, Trade{
        Date:       time.UnixMilli(bar.Timestamp).UTC().Format("2006-01-02"),
        Type:       s.ActivePosition.TradeType,
        EntryPrice: s.ActivePosition.EntryPrice,
        EntryTime:  time.UnixMilli(s.ActivePosition.EntryTime).UTC().Format("2006-01-02T15:04:05.000Z"),
        ExitPrice:  exitPrice,
        ExitTime:   time.UnixMilli(bar.Timestamp).UTC().Format("2006-01-02T15:04:05.000Z"),
        ExitReason: reason,
        HitTpSl:    reason,
        SizeUsd:    s.ActivePosition.SizeUsd,
        Qty:        s.ActivePosition.Quantity,
        FeesUsd:    totalFees,
        PnlUsd:     pnl,
        PnlPct:     pnlPct,
        Symbol:     s.ActivePosition.Symbol,
        TpPrice:    s.ActivePosition.TakeProfit,
        SlPrice:    s.ActivePosition.StopLoss,
        BarsHeld:   barsHeld,
        AtrAtEntry: decimal.Zero,
    })

    // Compute compounding amount change for logging
    amountBefore := s.CurrentEquity
    amountAfter := amountBefore.Mul(decimal.NewFromFloat(1).Add(pnlPct.Div(decimal.NewFromFloat(100))))

    if s.Verbose && s.EnableExcel {
        ts := time.Now().UTC().Format("2006/01/02 15:04:05")
        hitMsg := ""
        if reason == "TakeProfit" { hitMsg = "TP HIT" } else if reason == "StopLoss" { hitMsg = "SL HIT" } else { hitMsg = reason }
        s.ExcelRows = append(s.ExcelRows, ExcelRow{DateTime: ts, Type: "EVENT", Reason: fmt.Sprintf("%s @ %s | TP=%s SL=%s | TP_Streak=%d SL_Streak=%d | Amount: $%s → $%s", hitMsg, exitPrice.String(), s.ActivePosition.TakeProfit.String(), s.ActivePosition.StopLoss.String(), s.TpStreak, s.SlStreak, amountBefore.StringFixed(2), amountAfter.StringFixed(2))})
    }

    // Update compounding equity: NewEquity = CurrentEquity * (1 + pnlPct/100)
    s.CurrentEquity = amountAfter
    s.EquityCurve = append(s.EquityCurve, s.CurrentEquity)
    if s.CurrentEquity.GreaterThan(s.PeakEquity) { s.PeakEquity = s.CurrentEquity }
    dd := s.PeakEquity.Sub(s.CurrentEquity).Div(s.PeakEquity)
    if dd.GreaterThan(s.MaxDrawdown) { s.MaxDrawdown = dd }

    s.ActivePosition = nil
}

func (s *IchimokuBaselineStrategy) updateEquity() {
    // In compounding mode, equity is updated on trade close; keep drawdown tracking live
    if s.CurrentEquity.GreaterThan(s.PeakEquity) { s.PeakEquity = s.CurrentEquity }
    dd := s.PeakEquity.Sub(s.CurrentEquity).Div(s.PeakEquity)
    if dd.GreaterThan(s.MaxDrawdown) { s.MaxDrawdown = dd }
}

// Helpers
func (s *IchimokuBaselineStrategy) applyExchangeFilters(price, quantity decimal.Decimal) (decimal.Decimal, decimal.Decimal, bool) {
    filteredPrice := price.Div(s.ExchangeRules.TickSize).Round(0).Mul(s.ExchangeRules.TickSize)
    filteredQty := quantity.Div(s.ExchangeRules.LotSize).Round(0).Mul(s.ExchangeRules.LotSize)
    if filteredPrice.Mul(filteredQty).LessThan(s.ExchangeRules.MinNotional) { return filteredPrice, filteredQty, false }
    return filteredPrice, filteredQty, true
}

func (s *IchimokuBaselineStrategy) calculateSlippage(basePrice decimal.Decimal, side string) decimal.Decimal {
    switch s.SlippageMode {
    case "NONE":
        return decimal.Zero
    case "TRADE_SWEEP":
        rate := decimal.NewFromFloat(0.0001)
        if side == "Long" { return basePrice.Mul(rate) }
        return basePrice.Mul(rate.Neg())
    case "SYNTHETIC_BOOK":
        rate := decimal.NewFromFloat(0.0005)
        if side == "Long" { return basePrice.Mul(rate) }
        return basePrice.Mul(rate.Neg())
    default:
        return decimal.Zero
    }
}

func (s *IchimokuBaselineStrategy) calculateFee(notional decimal.Decimal, isMaker bool) decimal.Decimal {
    feeRate := s.ExchangeRules.TakerFee
    if isMaker { feeRate = s.ExchangeRules.MakerFee }
    return notional.Mul(feeRate)
}

func mathMaxInt64() int64 { return 1<<63 - 1 }


