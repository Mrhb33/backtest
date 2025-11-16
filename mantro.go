package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"backtest/go-services/services/config"
)

const (
	rsiPeriod            = 14
	defaultRSIMin        = 20.0 // percent
	defaultRSIMax        = 80.0 // percent
	defaultWickMinPct    = 1.5  // percent
	defaultWickMaxPct    = 6.0  // percent
	defaultBodyMinPct    = 0.5  // percent
	defaultBodyMaxPct    = 2.0  // percent
	takeProfitPct        = 2.5  // percent
	stopLossPct          = 0.6  // percent
	defaultCooldownBars  = 0
	requiredWarmupBars   = rsiPeriod
	defaultStatesLogFile = "mantro_strategy.log"
)

type candle struct {
	timestamp time.Time
	open      float64
	high      float64
	low       float64
	close     float64
}

type tradeRecord struct {
	SignalTimestamp time.Time
	SignalIndex     int
	EntryTimestamp  time.Time
	EntryIndex      int
	EntryPrice      float64
	TP              float64
	SL              float64
	ExitTimestamp   time.Time
	ExitIndex       int
	ExitPrice       float64
	ExitReason      string
	Result          string
	DurationBars    int
	PnL             float64
	RMultiple       float64
	BodyPct         float64
	LowerWickPct    float64
	RangePct        float64
	RSI             float64
}

type eventRecord struct {
	Event        string
	BarIndex     int
	Timestamp    time.Time
	Details      string
	BodyPct      float64
	LowerWickPct float64
	RangePct     float64
	RSI          float64
}

type stateRecord struct {
	BarIndex  int
	Timestamp time.Time
	Statuses  []string
}

type pendingEntry struct {
	entryIdx     int
	signalIdx    int
	signalTime   time.Time
	bodyPct      float64
	lowerWickPct float64
	rangePct     float64
	rsi          float64
}

type activeTrade struct {
	tradeIdx   int
	entryIdx   int
	entryPrice float64
	tp         float64
	sl         float64
}

type strategyConfig struct {
	cooldownBars int
	printStates  bool
	wickMinPct   float64
	wickMaxPct   float64
	bodyMinPct   float64
	bodyMaxPct   float64
	rsiMin       float64
	rsiMax       float64
}

func main() {
	inputPath := flag.String("input", "", "Path to OHLCV CSV")
	tradesOut := flag.String("trades-out", "mantro_trades.csv", "Output CSV for trades")
	eventsOut := flag.String("events-out", "mantro_events.csv", "Output CSV for events")
	statesOut := flag.String("states-out", "mantro_states.csv", "Output CSV for per-bar states")
	cooldownBars := flag.Int("cooldown-bars", defaultCooldownBars, "Bars to wait after exiting a trade")
	wickMin := flag.Float64("wick-min-pct", defaultWickMinPct, "Minimum acceptable lower wick percent")
	wickMax := flag.Float64("wick-max-pct", defaultWickMaxPct, "Maximum acceptable lower wick percent")
	bodyMin := flag.Float64("body-min-pct", defaultBodyMinPct, "Minimum acceptable body percent")
	bodyMax := flag.Float64("body-max-pct", defaultBodyMaxPct, "Maximum acceptable body percent")
	rsiMin := flag.Float64("rsi-min", defaultRSIMin, "Minimum acceptable RSI value")
	rsiMax := flag.Float64("rsi-max", defaultRSIMax, "Maximum acceptable RSI value")
	printStates := flag.Bool("print-states", false, "Print per-bar state transitions")
	flag.Parse()

	if strings.TrimSpace(*inputPath) == "" {
		fmt.Fprintln(os.Stderr, "error: --input is required")
		os.Exit(1)
	}
	if *cooldownBars < 0 {
		fmt.Fprintln(os.Stderr, "error: --cooldown-bars must be >= 0")
		os.Exit(1)
	}
	if *wickMin < 0 {
		fmt.Fprintln(os.Stderr, "error: --wick-min-pct must be >= 0")
		os.Exit(1)
	}
	if *wickMax <= *wickMin {
		fmt.Fprintln(os.Stderr, "error: --wick-max-pct must be greater than --wick-min-pct")
		os.Exit(1)
	}
	if *bodyMin < 0 {
		fmt.Fprintln(os.Stderr, "error: --body-min-pct must be >= 0")
		os.Exit(1)
	}
	if *bodyMax <= *bodyMin {
		fmt.Fprintln(os.Stderr, "error: --body-max-pct must be greater than --body-min-pct")
		os.Exit(1)
	}
	if *rsiMin < 0 || *rsiMin > 100 {
		fmt.Fprintln(os.Stderr, "error: --rsi-min must be within [0, 100]")
		os.Exit(1)
	}
	if *rsiMax <= *rsiMin || *rsiMax > 100 {
		fmt.Fprintln(os.Stderr, "error: --rsi-max must be within (rsi-min, 100]")
		os.Exit(1)
	}

	cfg := strategyConfig{
		cooldownBars: *cooldownBars,
		printStates:  *printStates,
		wickMinPct:   *wickMin,
		wickMaxPct:   *wickMax,
		bodyMinPct:   *bodyMin,
		bodyMaxPct:   *bodyMax,
		rsiMin:       *rsiMin,
		rsiMax:       *rsiMax,
	}

	candles, err := loadCandles(*inputPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error loading candles:", err)
		os.Exit(1)
	}
	if len(candles) < requiredWarmupBars {
		fmt.Fprintf(os.Stderr, "error: need at least %d candles (got %d)\n", requiredWarmupBars, len(candles))
		os.Exit(1)
	}

	rsiValues := computeRSI(candles, rsiPeriod)
	if len(rsiValues) != len(candles) {
		fmt.Fprintln(os.Stderr, "error computing RSI: mismatched lengths")
		os.Exit(1)
	}

	loggerFile, err := setupLogger(defaultStatesLogFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error creating logger:", err)
		os.Exit(1)
	}
	defer loggerFile.Close()

	logger := log.New(loggerFile, "", log.LstdFlags|log.Lmicroseconds)

	trades, events, states := runStrategy(candles, rsiValues, cfg, logger)

	if err := writeTrades(*tradesOut, trades); err != nil {
		fmt.Fprintln(os.Stderr, "error writing trades:", err)
		os.Exit(1)
	}
	if err := writeEvents(*eventsOut, events); err != nil {
		fmt.Fprintln(os.Stderr, "error writing events:", err)
		os.Exit(1)
	}
	if err := writeStates(*statesOut, states); err != nil {
		fmt.Fprintln(os.Stderr, "error writing states:", err)
		os.Exit(1)
	}

	initialCapital := 1000.0
	kpi := computeKPIs(trades, initialCapital)
	kpi.InputFile = *inputPath
	kpi.GeneratedAt = time.Now().UTC()

	reportPath := filepath.Join("reports", "mantro_report.json")
	if err := writeKPIsReport(reportPath, kpi); err != nil {
		fmt.Fprintln(os.Stderr, "error writing KPI report:", err)
		os.Exit(1)
	}

	fmt.Printf("Trades generated: %d\n", kpi.TotalTrades)
	fmt.Printf("Closed trades -> TP: %d SL: %d Other: %d WinRate: %.2f%%\n", kpi.TPTrades, kpi.SLTrades, kpi.OtherTrades, kpi.WinRatePct)
	fmt.Printf("Expectancy (R): %.4f Avg Return: %.4f%% Sharpe: %.4f MaxDD: %.2f%%\n", kpi.ExpectancyR, kpi.AverageReturnPct, kpi.SharpeRatio, kpi.MaxDrawdownPct)
	fmt.Printf("Equity summary: start=%.2f end=%.2f change=%.2f%%\n", kpi.EquityStart, kpi.EquityEnd, kpi.EquityChangePct)
	fmt.Printf("KPI report saved to %s (returns=%d)\n", reportPath, kpi.ReturnObservations)
}

func setupLogger(fileName string) (*os.File, error) {
	path, err := config.GetLogFilePath(fileName)
	if err != nil {
		return nil, err
	}
	return os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
}

func loadCandles(path string) ([]candle, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) <= 1 {
		return nil, errors.New("input CSV missing rows")
	}

	header := records[0]
	colIdx := map[string]int{}
	for idx, col := range header {
		colIdx[strings.ToLower(strings.TrimSpace(col))] = idx
	}

	requiredCols := []string{"open", "high", "low", "close"}
	for _, col := range requiredCols {
		if _, ok := colIdx[col]; !ok {
			return nil, fmt.Errorf("missing column %q", col)
		}
	}

	timestampIdx := -1
	if idx, ok := colIdx["time_utc"]; ok {
		timestampIdx = idx
	} else if idx, ok := colIdx["timestamp"]; ok {
		timestampIdx = idx
	}
	if timestampIdx == -1 {
		return nil, errors.New("missing timestamp or time_utc column")
	}

	candles := make([]candle, 0, len(records)-1)
	for i := 1; i < len(records); i++ {
		rec := records[i]
		if len(rec) <= timestampIdx {
			continue
		}

		tsStr := strings.TrimSpace(rec[timestampIdx])
		var ts time.Time
		if _, ok := colIdx["time_utc"]; ok {
			parsed, parseErr := time.Parse(time.RFC3339, tsStr)
			if parseErr != nil {
				return nil, fmt.Errorf("row %d timestamp: %w", i, parseErr)
			}
			ts = parsed
		} else {
			ms, parseErr := strconv.ParseInt(tsStr, 10, 64)
			if parseErr != nil {
				return nil, fmt.Errorf("row %d timestamp: %w", i, parseErr)
			}
			ts = time.UnixMilli(ms).UTC()
		}

		openPrice, err := strconv.ParseFloat(strings.TrimSpace(rec[colIdx["open"]]), 64)
		if err != nil {
			return nil, fmt.Errorf("row %d open: %w", i, err)
		}
		highPrice, err := strconv.ParseFloat(strings.TrimSpace(rec[colIdx["high"]]), 64)
		if err != nil {
			return nil, fmt.Errorf("row %d high: %w", i, err)
		}
		lowPrice, err := strconv.ParseFloat(strings.TrimSpace(rec[colIdx["low"]]), 64)
		if err != nil {
			return nil, fmt.Errorf("row %d low: %w", i, err)
		}
		closePrice, err := strconv.ParseFloat(strings.TrimSpace(rec[colIdx["close"]]), 64)
		if err != nil {
			return nil, fmt.Errorf("row %d close: %w", i, err)
		}

		candles = append(candles, candle{
			timestamp: ts,
			open:      openPrice,
			high:      highPrice,
			low:       lowPrice,
			close:     closePrice,
		})
	}

	return candles, nil
}

func runStrategy(candles []candle, rsiValues []float64, cfg strategyConfig, logger *log.Logger) ([]tradeRecord, []eventRecord, []stateRecord) {
	total := len(candles)

	var (
		trades        []tradeRecord
		events        []eventRecord
		states        []stateRecord
		pending       *pendingEntry
		position      *activeTrade
		cooldownUntil = -1
	)

	logEvent := func(ev eventRecord) {
		events = append(events, ev)
		if logger != nil {
			logger.Printf("%s bar=%d details=%s body_pct=%.6f lower_wick_pct=%.6f range_pct=%.6f rsi=%.6f",
				ev.Event, ev.BarIndex, ev.Details, ev.BodyPct, ev.LowerWickPct, ev.RangePct, ev.RSI)
		}
	}

	closeTrade := func(idx int, exitIdx int, exitPrice float64, reason string, result string) {
		tr := &trades[idx]
		tr.ExitIndex = exitIdx
		tr.ExitTimestamp = candles[exitIdx].timestamp
		tr.ExitPrice = exitPrice
		tr.ExitReason = reason
		tr.Result = result
		tr.DurationBars = exitIdx - position.entryIdx + 1
		tr.PnL = exitPrice - tr.EntryPrice

		risk := tr.EntryPrice - tr.SL
		if risk != 0 {
			tr.RMultiple = tr.PnL / math.Abs(risk)
		} else {
			tr.RMultiple = math.NaN()
		}

		logEvent(eventRecord{
			Event:        "trade_exit",
			BarIndex:     exitIdx,
			Timestamp:    candles[exitIdx].timestamp,
			Details:      fmt.Sprintf("reason=%s result=%s exit=%.8f", reason, result, exitPrice),
			BodyPct:      tr.BodyPct,
			LowerWickPct: tr.LowerWickPct,
			RangePct:     tr.RangePct,
			RSI:          tr.RSI,
		})

		position = nil
		if cfg.cooldownBars > 0 {
			cooldownUntil = exitIdx + cfg.cooldownBars
		} else {
			cooldownUntil = -1
		}
	}

	for i := 0; i < total; i++ {
		bar := candles[i]
		var rsiVal float64
		if i < len(rsiValues) {
			rsiVal = rsiValues[i]
		} else {
			rsiVal = math.NaN()
		}
		statuses := make([]string, 0, 4)
		addStatus := func(label string) {
			for _, existing := range statuses {
				if existing == label {
					return
				}
			}
			statuses = append(statuses, label)
		}

		finalized := false
		finalize := func() {
			if finalized {
				return
			}
			if len(statuses) == 0 {
				if position != nil {
					addStatus("holding")
				} else {
					addStatus("flat")
				}
			}
			entry := stateRecord{
				BarIndex:  i,
				Timestamp: bar.timestamp,
				Statuses:  make([]string, len(statuses)),
			}
			copy(entry.Statuses, statuses)
			states = append(states, entry)
			if cfg.printStates {
				fmt.Printf("[bar %d] %s\n", i, strings.Join(entry.Statuses, "|"))
			}
			finalized = true
		}

		if pending != nil && pending.entryIdx == i {
			entryPrice := bar.open
			if !isValidPrice(entryPrice) {
				logEvent(eventRecord{
					Event:     "entry_failed",
					BarIndex:  i,
					Timestamp: bar.timestamp,
					Details:   "reason=invalid_entry_price",
					RSI:       pending.rsi,
				})
				pending = nil
			} else {
				tp := entryPrice * (1.0 + takeProfitPct/100.0)
				sl := entryPrice * (1.0 - stopLossPct/100.0)

				trades = append(trades, tradeRecord{
					SignalTimestamp: pending.signalTime,
					SignalIndex:     pending.signalIdx,
					EntryTimestamp:  bar.timestamp,
					EntryIndex:      i,
					EntryPrice:      entryPrice,
					TP:              tp,
					SL:              sl,
					ExitIndex:       -1,
					BodyPct:         pending.bodyPct,
					LowerWickPct:    pending.lowerWickPct,
					RangePct:        pending.rangePct,
					RSI:             pending.rsi,
				})

				position = &activeTrade{
					tradeIdx:   len(trades) - 1,
					entryIdx:   i,
					entryPrice: entryPrice,
					tp:         tp,
					sl:         sl,
				}

				logEvent(eventRecord{
					Event:        "trade_entry",
					BarIndex:     i,
					Timestamp:    bar.timestamp,
					Details:      fmt.Sprintf("entry=%.8f tp=%.8f sl=%.8f", entryPrice, tp, sl),
					BodyPct:      pending.bodyPct,
					LowerWickPct: pending.lowerWickPct,
					RangePct:     pending.rangePct,
					RSI:          pending.rsi,
				})

				addStatus("enter")
				pending = nil
			}
		}

		if position != nil {
			trIdx := position.tradeIdx
			openPrice := bar.open
			exitTriggered := false

			if isValidPrice(openPrice) && openPrice >= position.tp {
				closeTrade(trIdx, i, openPrice, "target_gap_open", "TP")
				addStatus("exit_tp")
				exitTriggered = true
			} else if isValidPrice(openPrice) && openPrice <= position.sl {
				closeTrade(trIdx, i, openPrice, "stop_gap_open", "SL")
				addStatus("exit_sl")
				exitTriggered = true
			} else if bar.high >= position.tp {
				closeTrade(trIdx, i, position.tp, "target_hit", "TP")
				addStatus("exit_tp")
				exitTriggered = true
			} else if bar.low <= position.sl {
				closeTrade(trIdx, i, position.sl, "stop_hit", "SL")
				addStatus("exit_sl")
				exitTriggered = true
			}

			if exitTriggered {
				finalize()
				continue
			}
		}

		if i < requiredWarmupBars {
			addStatus("warmup")
			finalize()
			continue
		}

		if cfg.cooldownBars > 0 && cooldownUntil >= 0 && i <= cooldownUntil {
			addStatus("cooldown")
			finalize()
			continue
		}

		if pending != nil {
			addStatus("pending")
			finalize()
			continue
		}

		if position != nil {
			addStatus("holding")
			finalize()
			continue
		}

		openVal := bar.open
		closeVal := bar.close
		lowVal := bar.low
		highVal := bar.high
		if !isValidPrice(openVal) || !isValidPrice(closeVal) || !isValidPrice(lowVal) || !isValidPrice(highVal) {
			finalize()
			continue
		}

		if math.IsNaN(rsiVal) {
			addStatus("rsi_unavailable")
			finalize()
			continue
		}
		if rsiVal < cfg.rsiMin || rsiVal > cfg.rsiMax {
			addStatus("rsi_filter")
			finalize()
			continue
		}

		if closeVal <= openVal {
			finalize()
			continue
		}

		bodyMove := closeVal - openVal
		bodyPct := (bodyMove / openVal) * 100.0
		if bodyPct < cfg.bodyMinPct || bodyPct > cfg.bodyMaxPct {
			finalize()
			continue
		}

		priceRange := highVal - lowVal
		if priceRange <= 0 {
			finalize()
			continue
		}

		bodyLow := math.Min(openVal, closeVal)
		if bodyLow <= 0 {
			finalize()
			continue
		}

		lowerWick := bodyLow - lowVal
		if lowerWick <= 0 {
			finalize()
			continue
		}

		lowerWickPct := (lowerWick / bodyLow) * 100.0
		if lowerWickPct < cfg.wickMinPct || lowerWickPct > cfg.wickMaxPct {
			finalize()
			continue
		}

		rangePct := 0.0
		if openVal > 0 {
			rangePct = (priceRange / openVal) * 100.0
		}

		entryIdx := i + 1
		if entryIdx >= total {
			logEvent(eventRecord{
				Event:        "signal_skipped",
				BarIndex:     i,
				Timestamp:    bar.timestamp,
				Details:      "reason=end_of_data",
				BodyPct:      bodyPct,
				LowerWickPct: lowerWickPct,
				RangePct:     rangePct,
				RSI:          rsiVal,
			})
			addStatus("signal")
			finalize()
			continue
		}

		pending = &pendingEntry{
			entryIdx:     entryIdx,
			signalIdx:    i,
			signalTime:   bar.timestamp,
			bodyPct:      bodyPct,
			lowerWickPct: lowerWickPct,
			rangePct:     rangePct,
			rsi:          rsiVal,
		}

		logEvent(eventRecord{
			Event:        "signal_detected",
			BarIndex:     i,
			Timestamp:    bar.timestamp,
			Details:      fmt.Sprintf("entry_idx=%d", entryIdx),
			BodyPct:      bodyPct,
			LowerWickPct: lowerWickPct,
			RangePct:     rangePct,
			RSI:          rsiVal,
		})

		addStatus("signal")
		addStatus("pending")
		finalize()
	}

	return trades, events, states
}

func computeRSI(candles []candle, period int) []float64 {
	if period <= 0 {
		return make([]float64, len(candles))
	}

	n := len(candles)
	values := make([]float64, n)
	for i := range values {
		values[i] = math.NaN()
	}
	if n == 0 || n <= period {
		return values
	}

	var gainSum, lossSum float64
	for i := 1; i <= period; i++ {
		change := candles[i].close - candles[i-1].close
		if change >= 0 {
			gainSum += change
		} else {
			lossSum += -change
		}
	}

	avgGain := gainSum / float64(period)
	avgLoss := lossSum / float64(period)

	if avgLoss == 0 {
		values[period] = 100.0
	} else {
		rs := avgGain / avgLoss
		values[period] = 100.0 - (100.0 / (1.0 + rs))
	}

	for i := period + 1; i < n; i++ {
		change := candles[i].close - candles[i-1].close
		var gain, loss float64
		if change > 0 {
			gain = change
		} else {
			loss = -change
		}

		avgGain = (avgGain*float64(period-1) + gain) / float64(period)
		avgLoss = (avgLoss*float64(period-1) + loss) / float64(period)

		if avgLoss == 0 {
			values[i] = 100.0
		} else {
			rs := avgGain / avgLoss
			values[i] = 100.0 - (100.0 / (1.0 + rs))
		}
	}

	return values
}

func writeTrades(path string, trades []tradeRecord) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	headers := []string{
		"signal_timestamp", "signal_index", "entry_timestamp", "entry_index", "entry_price",
		"tp", "sl", "exit_timestamp", "exit_index", "exit_price", "exit_reason", "result",
		"duration_bars", "pnl", "r_multiple", "body_pct", "lower_wick_pct", "range_pct", "rsi",
	}
	if err := w.Write(headers); err != nil {
		return err
	}

	for _, tr := range trades {
		row := []string{
			tr.SignalTimestamp.Format(time.RFC3339),
			strconv.Itoa(tr.SignalIndex),
			tr.EntryTimestamp.Format(time.RFC3339),
			strconv.Itoa(tr.EntryIndex),
			formatFloat(tr.EntryPrice),
			formatFloat(tr.TP),
			formatFloat(tr.SL),
			"",
			"",
			"",
			tr.ExitReason,
			tr.Result,
			strconv.Itoa(tr.DurationBars),
			formatFloat(tr.PnL),
			formatFloat(tr.RMultiple),
			formatFloat(tr.BodyPct),
			formatFloat(tr.LowerWickPct),
			formatFloat(tr.RangePct),
			formatFloat(tr.RSI),
		}

		if tr.ExitIndex >= 0 {
			row[7] = tr.ExitTimestamp.Format(time.RFC3339)
			row[8] = strconv.Itoa(tr.ExitIndex)
			row[9] = formatFloat(tr.ExitPrice)
		}

		if err := w.Write(row); err != nil {
			return err
		}
	}

	return nil
}

func writeEvents(path string, events []eventRecord) error {
	if len(events) == 0 {
		return nil
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	headers := []string{"event", "bar_index", "timestamp", "details", "body_pct", "lower_wick_pct", "range_pct", "rsi"}
	if err := w.Write(headers); err != nil {
		return err
	}

	for _, ev := range events {
		row := []string{
			ev.Event,
			strconv.Itoa(ev.BarIndex),
			ev.Timestamp.Format(time.RFC3339),
			ev.Details,
			formatFloat(ev.BodyPct),
			formatFloat(ev.LowerWickPct),
			formatFloat(ev.RangePct),
			formatFloat(ev.RSI),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}

	return nil
}

func writeStates(path string, states []stateRecord) error {
	if len(states) == 0 {
		return nil
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write([]string{"bar_index", "timestamp", "states"}); err != nil {
		return err
	}

	for _, st := range states {
		stateValue := strings.Join(st.Statuses, "|")
		row := []string{
			strconv.Itoa(st.BarIndex),
			st.Timestamp.Format(time.RFC3339),
			stateValue,
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}

	return nil
}

type kpiReport struct {
	GeneratedAt        time.Time `json:"generated_at"`
	InputFile          string    `json:"input_file"`
	TotalTrades        int       `json:"total_trades"`
	ClosedTrades       int       `json:"closed_trades"`
	TPTrades           int       `json:"tp_trades"`
	SLTrades           int       `json:"sl_trades"`
	OtherTrades        int       `json:"other_trades"`
	WinRatePct         float64   `json:"win_rate_pct"`
	ExpectancyR        float64   `json:"expectancy_r"`
	AverageReturnPct   float64   `json:"average_return_pct"`
	MaxDrawdownPct     float64   `json:"max_drawdown_pct"`
	SharpeRatio        float64   `json:"sharpe_ratio"`
	EquityStart        float64   `json:"equity_start"`
	EquityEnd          float64   `json:"equity_end"`
	EquityChangePct    float64   `json:"equity_change_pct"`
	ReturnObservations int       `json:"return_observations"`
}

func computeKPIs(trades []tradeRecord, initial float64) kpiReport {
	report := kpiReport{
		TotalTrades: len(trades),
		EquityStart: initial,
		EquityEnd:   initial,
	}

	var (
		sumR        float64
		countR      float64
		sumReturn   float64
		returns     []float64
		capital     = initial
		peak        = initial
		maxDrawdown float64
	)

	for _, tr := range trades {
		switch strings.ToUpper(tr.Result) {
		case "TP":
			report.TPTrades++
		case "SL":
			report.SLTrades++
		}

		if !math.IsNaN(tr.RMultiple) {
			sumR += tr.RMultiple
			countR++
		}

		if tr.ExitIndex < 0 || !isValidPrice(tr.EntryPrice) || !isValidPrice(tr.ExitPrice) {
			continue
		}

		report.ClosedTrades++
		ret := (tr.ExitPrice - tr.EntryPrice) / tr.EntryPrice
		returns = append(returns, ret)
		sumReturn += ret

		if capital > 0 {
			capital *= 1.0 + ret
		} else {
			capital = tr.ExitPrice
		}
		if capital > peak {
			peak = capital
		}
		if peak > 0 {
			drawdown := (peak - capital) / peak
			if drawdown > maxDrawdown {
				maxDrawdown = drawdown
			}
		}
	}

	report.OtherTrades = report.ClosedTrades - report.TPTrades - report.SLTrades
	if report.OtherTrades < 0 {
		report.OtherTrades = 0
	}

	if winsDenom := report.TPTrades + report.SLTrades; winsDenom > 0 {
		report.WinRatePct = (float64(report.TPTrades) / float64(winsDenom)) * 100.0
	}

	if countR > 0 {
		report.ExpectancyR = sumR / countR
	}

	report.ReturnObservations = len(returns)
	if len(returns) > 0 {
		meanReturn := sumReturn / float64(len(returns))
		report.AverageReturnPct = meanReturn * 100.0

		if len(returns) > 1 {
			var sumSq float64
			for _, r := range returns {
				diff := r - meanReturn
				sumSq += diff * diff
			}
			variance := sumSq / float64(len(returns)-1)
			if variance > 0 {
				stdDev := math.Sqrt(variance)
				report.SharpeRatio = (meanReturn / stdDev) * math.Sqrt(float64(len(returns)))
			}
		}
	}

	report.MaxDrawdownPct = maxDrawdown * 100.0
	report.EquityEnd = capital
	if initial > 0 {
		report.EquityChangePct = (capital/initial - 1.0) * 100.0
	}

	return report
}

func writeKPIsReport(path string, report kpiReport) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, append(data, '\n'), 0o644)
}

func formatFloat(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return ""
	}
	return fmt.Sprintf("%.8f", v)
}

func isValidPrice(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v > 0
}
