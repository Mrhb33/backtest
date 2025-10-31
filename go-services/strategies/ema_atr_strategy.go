//! EMA/ATR Strategy Implementation
//!
//! Implements EMA26/EMA100 + ATR(14) + Body% strategy with dynamic ATR-based TP/SL
//! and first-touch exit resolution, integrated with the existing engine structure.

package strategies

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

// Bar represents OHLCV data
type Bar struct {
	Timestamp int64
	Open      decimal.Decimal
	High      decimal.Decimal
	Low       decimal.Decimal
	Close     decimal.Decimal
	Volume    decimal.Decimal
}

// Trade represents a completed trade
type Trade struct {
	Date       string
	Type       string
	EntryPrice decimal.Decimal
	EntryTime  string
	ExitPrice  decimal.Decimal
	ExitTime   string
	ExitReason string
	HitTpSl    string
	SizeUsd    decimal.Decimal
	Qty        decimal.Decimal
	FeesUsd    decimal.Decimal
	PnlUsd     decimal.Decimal
	PnlPct     decimal.Decimal
	Symbol     string
	TpPrice    decimal.Decimal
	SlPrice    decimal.Decimal
	BarsHeld   int
	AtrAtEntry decimal.Decimal
}

// TradeSummary contains aggregated statistics
type TradeSummary struct {
	TotalTrades         int
	Wins                int
	Losses              int
	WinRate             decimal.Decimal
	NetPnlUsd           decimal.Decimal
	AvgWinUsd           decimal.Decimal
	AvgLossUsd          decimal.Decimal
	Expectancy          decimal.Decimal
	MaxDrawdown         decimal.Decimal
	ProfitFactor        decimal.Decimal
	AvgHoldingTimeHours decimal.Decimal
}

// ActivePosition represents an open position
type ActivePosition struct {
	Symbol        string
	TradeType     string
	EntryTime     int64
	EntryPrice    decimal.Decimal
	Quantity      decimal.Decimal
	TakeProfit    decimal.Decimal
	StopLoss      decimal.Decimal
	TimeToLive    int64
	EntryFee      decimal.Decimal
	SizeUsd       decimal.Decimal
	AtrAtEntry    decimal.Decimal
	EntryBarIndex int
}

// EntryMode defines when to enter trades
type EntryMode int

const (
	EntryModeNextBarOpen EntryMode = iota // Enter on next bar's open (default)
	EntryModeSignalClose                  // Enter on signal bar's close
)

// FirstTouchPolicy defines how to resolve when both TP and SL are hit in same bar
type FirstTouchPolicy int

const (
	FirstTouchPolicySLFirst FirstTouchPolicy = iota // Stop loss has priority (current behavior)
	FirstTouchPolicyTPFirst                         // Take profit has priority
	FirstTouchPolicyChart                           // Chart order: entry first, then whichever touched first
)

// SizingMode defines how to calculate position size
type SizingMode int

const (
	SizingModeNotional SizingMode = iota // Fixed notional amount (current behavior)
	SizingModeRisk                       // Risk-based sizing using ATR distance to SL
)

// CandidateReason explains why a signal candidate was filtered
type CandidateReason string

const (
	CandidateReasonNone        CandidateReason = "none"
	CandidateReasonBodyPctLow  CandidateReason = "body_pct_too_low"
	CandidateReasonBodyPctHigh CandidateReason = "body_pct_too_high"
	CandidateReasonNoCrossover CandidateReason = "no_crossover"
	CandidateReasonInWarmup    CandidateReason = "in_warmup"
	CandidateReasonHasPosition CandidateReason = "has_position"
)

// SignalCandidate represents a potential trade signal before filters
type SignalCandidate struct {
	BarIndex   int
	Timestamp  int64
	Bar        Bar
	EmaFast    float64
	EmaSlow    float64
	Atr        float64
	BodyPct    decimal.Decimal
	TradeType  string
	Reason     CandidateReason
	EntryPrice decimal.Decimal
	SlPrice    decimal.Decimal
	TpPrice    decimal.Decimal
}

// DebugBar contains per-bar debug information
type DebugBar struct {
	Timestamp     int64
	Open          decimal.Decimal
	High          decimal.Decimal
	Low           decimal.Decimal
	Close         decimal.Decimal
	Ema26         float64
	Ema100        float64
	Atr14         float64
	BodyPct       decimal.Decimal
	LongCond      bool
	ShortCond     bool
	ReasonBlocked CandidateReason
	EntryPrice    decimal.Decimal
	SlPrice       decimal.Decimal
	TpPrice       decimal.Decimal
}

// ExchangeRules defines exchange trading rules for order filtering
type ExchangeRules struct {
	TickSize       decimal.Decimal // Minimum price increment
	LotSize        decimal.Decimal // Minimum quantity increment
	MinNotional    decimal.Decimal // Minimum order value
	MakerFee       decimal.Decimal // Maker fee rate (e.g., 0.001 = 0.1%)
	TakerFee       decimal.Decimal // Taker fee rate
	PrecisionPrice uint8           // Price decimal places
	PrecisionQty   uint8           // Quantity decimal places
}

// BacktestRequest defines a reproducible backtest run configuration
type BacktestRequest struct {
	JobID            string
	Symbols          []string
	Timeframe        string
	StartTime        int64
	EndTime          int64
	IntrabarPolicy   string // EXACT_TRADES, ONE_SECOND_BARS, LINEAR_INTERPOLATION
	SlippageMode     string // NONE, TRADE_SWEEP, SYNTHETIC_BOOK
	FeeVersion       string
	StrategyHash     string
	SnapshotID       string
	WarmupMultiplier int // Multiplier for slowest lookback (default 3-5)
}

// RunManifest stores complete run configuration for reproducibility
type RunManifest struct {
	Request         BacktestRequest
	CreatedAt       int64
	EngineVersion   string
	StrategyParams  map[string]string
	ExchangeRules   ExchangeRules
	WarmupBars      int
	FirstTradingBar int
	LastTradingBar  int
}

// IndicatorTraceEntry logs every candle's indicator values
type IndicatorTraceEntry struct {
	Timestamp int64
	Open      decimal.Decimal
	High      decimal.Decimal
	Low       decimal.Decimal
	Close     decimal.Decimal
	EmaFast   float64
	EmaSlow   float64
	Atr       float64
	State     string // "flat", "long", "short"
}

// SignalTraceEntry logs decision points only
type SignalTraceEntry struct {
	Timestamp            int64
	ReasonCode           string
	EmaCross             bool
	BodyPct              decimal.Decimal
	Atr                  float64
	EntrySide            string
	EntryPricePreFilter  decimal.Decimal
	EntryPricePostFilter decimal.Decimal
	QtyPreFilter         decimal.Decimal
	QtyPostFilter        decimal.Decimal
	Slippage             decimal.Decimal
	Fees                 decimal.Decimal
}

// TradeTraceEntry logs from entry until exit
type TradeTraceEntry struct {
	TsEntry        int64
	TpPrice        decimal.Decimal
	SlPrice        decimal.Decimal
	IntrabarPolicy string
	FirstTouch     string // "TP", "SL", or "none"
	TsExit         int64
	ExitPrice      decimal.Decimal
	Fees           decimal.Decimal
	RealizedPnl    decimal.Decimal
	BarsHeld       int
}

// ExcelRow represents a row in the Excel export
type ExcelRow struct {
	DateTime   string  `json:"date_time"`
	Type       string  `json:"type"`
	Entry      string  `json:"entry"`
	Exit       string  `json:"exit"`
	PnL        string  `json:"pnl"`
	PnLPct     string  `json:"pnl_pct"`
	Reason     string  `json:"reason"`
	TP         string  `json:"tp"`
	SL         string  `json:"sl"`
	Open       string  `json:"open"`
	High       string  `json:"high"`
	Low        string  `json:"low"`
	Close      string  `json:"close"`
	Volume     string  `json:"volume"`
	EMA26      float64 `json:"ema26"`
	EMA100     float64 `json:"ema100"`
	ATR        float64 `json:"atr"`
	BodyPct    string  `json:"body_pct"`
	Conditions string  `json:"conditions"`
}

// PerformanceMetrics tracks execution performance
type PerformanceMetrics struct {
	StartTime     time.Time
	EndTime       time.Time
	BarsProcessed int
	BarsPerSecond float64
	MemoryUsageMB float64
	LatencyP50    time.Duration
	LatencyP95    time.Duration
	LatencyP99    time.Duration
}

// EMAATRStrategy implements the EMA/ATR strategy
type EMAATRStrategy struct {
	// Strategy parameters
	EmaFastPeriod int
	EmaSlowPeriod int
	AtrPeriod     int
	// Body percentage thresholds relative to price (|close-open|/close) - INCLUSIVE
	BodyPctMinLong    decimal.Decimal // e.g., 0.0005 for 0.05%
	BodyPctMaxLong    decimal.Decimal // e.g., 0.008 for 0.8%
	BodyPctMinShort   decimal.Decimal // e.g., 0.0005 for 0.05%
	BodyPctMaxShort   decimal.Decimal // e.g., 0.008 for 0.8%
	AtpMultiplier     decimal.Decimal
	SlMultiplier      decimal.Decimal
	RiskAmount        decimal.Decimal
	WarmupBars        int
	EntryMode         EntryMode
	MaxHoldingBars    int // Max bars to hold position before timeout
	FirstTouchPolicy  FirstTouchPolicy
	SizingMode        SizingMode
	ATRTiming         string // "signal" or "entry" - which bar's ATR to use
	IntraExitsOnEntry bool   // Allow exits on same bar as entry (for next-open mode)
	IntrabarPolicy    string // EXACT_TRADES, ONE_SECOND_BARS, LINEAR_INTERPOLATION
	SlippageMode      string // NONE, TRADE_SWEEP, SYNTHETIC_BOOK

	// Exchange rules
	ExchangeRules ExchangeRules

	// Backtest configuration
	BacktestRequest *BacktestRequest
	RunManifest     *RunManifest

	// Trace outputs
	EnableTraces    bool
	Verbose         bool // Enable verbose logging (shows every candle's calculations)
	EnableExcel     bool // Export detailed table to Excel file
	IndicatorTraces []IndicatorTraceEntry
	SignalTraces    []SignalTraceEntry
	TradeTraces     []TradeTraceEntry
	ExcelRows       []ExcelRow // Store data for Excel export

	// Performance metrics
	PerfMetrics PerformanceMetrics

	// Debug outputs
	EnableDebug bool
	DebugBars   []DebugBar
	Candidates  []SignalCandidate

	// State
	Bars           []Bar
	EmaFast        []float64
	EmaSlow        []float64
	Atr            []float64
	ActivePosition *ActivePosition
	Trades         []Trade
	RejectedTrades []string
	CurrentEquity  decimal.Decimal
	PeakEquity     decimal.Decimal
	MaxDrawdown    decimal.Decimal

	// Detected input bar cadence in milliseconds (e.g., 60000 for 1m, 300000 for 5m)
	CadenceMs int64

	// Streak tracking
	TpStreak    int
	SlStreak    int
	MaxTpStreak int
	MaxSlStreak int

	NextEntry *struct {
		ActivateAtTs int64
		TradeType    string
		Atr          decimal.Decimal
	}
}

// NewEMAATRStrategy creates a new strategy instance
func NewEMAATRStrategy() *EMAATRStrategy {
	return &EMAATRStrategy{
		EmaFastPeriod: 26,
		EmaSlowPeriod: 100,
		AtrPeriod:     14,
		// defaults: long 0.2%..0.8%, short 0.2%..0.8% (wider, more realistic)
		BodyPctMinLong:    decimal.NewFromFloat(0.002),  // +0.2%
		BodyPctMaxLong:    decimal.NewFromFloat(0.008),  // +0.8%
		BodyPctMinShort:   decimal.NewFromFloat(-0.008), // -0.8%
		BodyPctMaxShort:   decimal.NewFromFloat(-0.002), // -0.2%
		AtpMultiplier:     decimal.NewFromFloat(1.8),    // ATR * 1.8 = more realistic TP
		SlMultiplier:      decimal.NewFromFloat(2.5),    // ATR * 2.5 = more realistic SL
		RiskAmount:        decimal.NewFromFloat(1000.0), // $1000 default
		WarmupBars:        100,                          // Need enough bars for EMA100
		EntryMode:         EntryModeNextBarOpen,
		MaxHoldingBars:    72,                     // 72 bars = 6 hours on 5m (more realistic)
		FirstTouchPolicy:  FirstTouchPolicyChart,  // Chart order: entry first, then whichever touched first
		SizingMode:        SizingModeNotional,     // Fixed notional amount
		ATRTiming:         "signal",               // Use ATR from signal bar
		IntraExitsOnEntry: false,                  // Conservative: no exits on same bar as entry
		IntrabarPolicy:    "LINEAR_INTERPOLATION", // Default intrabar policy
		SlippageMode:      "TRADE_SWEEP",          // Default slippage mode
		EnableTraces:      false,
		EnableDebug:       false,
		CurrentEquity:     decimal.NewFromFloat(10000.0),
		PeakEquity:        decimal.NewFromFloat(10000.0),
		MaxDrawdown:       decimal.Zero,
		CadenceMs:         300000,
		// Default exchange rules for BTCUSDT on Binance
		ExchangeRules: ExchangeRules{
			TickSize:       decimal.NewFromFloat(0.01),    // $0.01 tick size
			LotSize:        decimal.NewFromFloat(0.00001), // 0.00001 BTC lot size
			MinNotional:    decimal.NewFromFloat(10.0),    // $10 minimum
			MakerFee:       decimal.NewFromFloat(0.0001),  // 0.01% maker fee
			TakerFee:       decimal.NewFromFloat(0.001),   // 0.1% taker fee
			PrecisionPrice: 2,
			PrecisionQty:   8,
		},
		IndicatorTraces: make([]IndicatorTraceEntry, 0),
		SignalTraces:    make([]SignalTraceEntry, 0),
		TradeTraces:     make([]TradeTraceEntry, 0),
	}
}

// LoadCSV loads OHLCV data from CSV file
func (s *EMAATRStrategy) LoadCSV(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// CSV reader handles quoted fields robustly
	r := csv.NewReader(bufio.NewReader(file))
	r.ReuseRecord = false
	r.FieldsPerRecord = -1
	r.LazyQuotes = true

	s.Bars = make([]Bar, 0, 1_000)
	lineIndex := 0
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			lineIndex++
			continue
		}
		if len(rec) < 6 {
			lineIndex++
			continue
		}

		// Skip header if present
		if lineIndex == 0 && (strings.EqualFold(rec[0], "timestamp") || strings.EqualFold(rec[0], "timestamp_ms")) {
			lineIndex++
			continue
		}

		// Parse fields
		tsStr := strings.TrimSpace(rec[0])
		tsStr = strings.TrimPrefix(tsStr, "\ufeff")
		timestamp, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			lineIndex++
			continue
		}

		open, err := decimal.NewFromString(strings.TrimSpace(rec[1]))
		if err != nil {
			lineIndex++
			continue
		}
		high, err := decimal.NewFromString(strings.TrimSpace(rec[2]))
		if err != nil {
			lineIndex++
			continue
		}
		low, err := decimal.NewFromString(strings.TrimSpace(rec[3]))
		if err != nil {
			lineIndex++
			continue
		}
		close, err := decimal.NewFromString(strings.TrimSpace(rec[4]))
		if err != nil {
			lineIndex++
			continue
		}
		volume, err := decimal.NewFromString(strings.TrimSpace(rec[5]))
		if err != nil {
			volume = decimal.Zero
		}

		s.Bars = append(s.Bars, Bar{
			Timestamp: timestamp,
			Open:      open,
			High:      high,
			Low:       low,
			Close:     close,
			Volume:    volume,
		})
		lineIndex++
	}

	// Sort by timestamp and deduplicate identical timestamps (keep last)
	if len(s.Bars) > 1 {
		sort.Slice(s.Bars, func(i, j int) bool { return s.Bars[i].Timestamp < s.Bars[j].Timestamp })
		uniq := make([]Bar, 0, len(s.Bars))
		var lastTs int64 = -1
		for _, b := range s.Bars {
			if b.Timestamp == lastTs {
				// overwrite last
				uniq[len(uniq)-1] = b
				continue
			}
			uniq = append(uniq, b)
			lastTs = b.Timestamp
		}
		s.Bars = uniq
	}

	// Detect cadence (most common delta between consecutive bars)
	if len(s.Bars) >= 2 {
		deltaCount := make(map[int64]int)
		limit := len(s.Bars)
		if limit > 2000 {
			limit = 2000
		}
		for i := 1; i < limit; i++ {
			d := s.Bars[i].Timestamp - s.Bars[i-1].Timestamp
			if d > 0 && d < int64(60*60*1000) { // under 1 hour
				deltaCount[d]++
			}
		}
		var bestDelta int64 = s.CadenceMs
		bestCount := -1
		for d, c := range deltaCount {
			if c > bestCount {
				bestCount = c
				bestDelta = d
			}
		}
		if bestCount > 0 {
			s.CadenceMs = bestDelta
		}
		log.Printf("⏱️  Detected cadence: %d ms", s.CadenceMs)
		lineIndex++
	}
	log.Printf("Parsed %d bars from CSV", len(s.Bars))

	// Validate data quality before proceeding
	if err := s.validateDataQuality(); err != nil {
		return fmt.Errorf("data validation failed: %w", err)
	}

	return nil
}

// validateDataQuality performs comprehensive data validation checks
func (s *EMAATRStrategy) validateDataQuality() error {
	if len(s.Bars) == 0 {
		return fmt.Errorf("no data loaded")
	}

	log.Printf("🔍 Starting data quality validation on %d bars...", len(s.Bars))

	// 1. Time alignment check: ensure timestamps are aligned to cadence
	// For 1m bars: (open_time_ms % 60000) = 0
	var misaligned int
	for i := 0; i < len(s.Bars); i++ {
		if s.Bars[i].Timestamp%int64(s.CadenceMs) != 0 {
			misaligned++
		}
	}
	if misaligned > 0 {
		log.Printf("⚠️  WARNING: %d bars have misaligned timestamps (not aligned to %dms cadence)", misaligned, s.CadenceMs)
	}

	// 2. Missing minute check: detect gaps in data
	var missingMinutes []int64
	for i := 1; i < len(s.Bars); i++ {
		expectedNext := s.Bars[i-1].Timestamp + int64(s.CadenceMs)
		if s.Bars[i].Timestamp > expectedNext {
			// Gap detected
			for ts := expectedNext; ts < s.Bars[i].Timestamp; ts += int64(s.CadenceMs) {
				missingMinutes = append(missingMinutes, ts)
			}
		}
	}
	if len(missingMinutes) > 0 {
		log.Printf("⚠️  WARNING: %d missing bars detected (gaps in data)", len(missingMinutes))
		if len(missingMinutes) <= 10 {
			for _, ts := range missingMinutes {
				log.Printf("  Missing bar at: %s", time.UnixMilli(ts).UTC().Format("2006-01-02 15:04:05"))
			}
		} else {
			log.Printf("  First 10 missing bars:")
			for i := 0; i < 10; i++ {
				log.Printf("  Missing bar at: %s", time.UnixMilli(missingMinutes[i]).UTC().Format("2006-01-02 15:04:05"))
			}
			log.Printf("  ... and %d more", len(missingMinutes)-10)
		}
	}

	// 3. Monotonic timestamps & cadence validation (softened to allow gaps)
	var badOrder, badCadence, gaps int
	for i := 1; i < len(s.Bars); i++ {
		if s.Bars[i].Timestamp <= s.Bars[i-1].Timestamp {
			badOrder++
		}
		delta := s.Bars[i].Timestamp - s.Bars[i-1].Timestamp
		if delta != s.CadenceMs {
			if delta > s.CadenceMs {
				gaps++ // Gap detected
			} else {
				badCadence++ // Invalid short interval
			}
		}
	}
	log.Printf("📊 Timestamp validation: badOrder=%d badCadence=%d gaps=%d misaligned=%d (cadence=%dms)", badOrder, badCadence, gaps, misaligned, s.CadenceMs)

	// 2. Price sanity & wild jumps validation
	minC, maxC := math.MaxFloat64, 0.0
	jumps := 0
	var jumpDetails []string

	for i := 1; i < len(s.Bars); i++ {
		c, _ := s.Bars[i].Close.Float64()
		p, _ := s.Bars[i-1].Close.Float64()

		if c < minC {
			minC = c
		}
		if c > maxC {
			maxC = c
		}

		if p > 0 && math.Abs(c/p-1) > 0.2 { // >20% jump bar-to-bar
			jumps++
			jumpTime := time.UnixMilli(s.Bars[i].Timestamp).UTC()
			jumpDetails = append(jumpDetails, fmt.Sprintf("⚠️  >20%% jump %s -> %s at %s",
				fmt.Sprintf("%.2f", p), fmt.Sprintf("%.2f", c), jumpTime.Format("2006-01-02 15:04:05")))
		}
	}

	log.Printf("💰 Price validation: minClose=%.2f maxClose=%.2f jumps>20%%=%d", minC, maxC, jumps)

	// Log first few jump details
	if len(jumpDetails) > 0 {
		log.Printf("🚨 Wild price jumps detected:")
		for i, detail := range jumpDetails {
			if i >= 5 { // Show only first 5 jumps
				log.Printf("... and %d more jumps", len(jumpDetails)-5)
				break
			}
			log.Printf("  %s", detail)
		}
	}

	// 3. Sample data logging for verification
	log.Printf("📋 Sample data (first 3 bars):")
	for i := 0; i < 3 && i < len(s.Bars); i++ {
		bar := s.Bars[i]
		log.Printf("  Bar %d: %s | O:%.2f H:%.2f L:%.2f C:%.2f V:%.2f",
			i,
			time.UnixMilli(bar.Timestamp).UTC().Format("2006-01-02 15:04:05"),
			bar.Open.InexactFloat64(), bar.High.InexactFloat64(),
			bar.Low.InexactFloat64(), bar.Close.InexactFloat64(), bar.Volume.InexactFloat64())
	}

	log.Printf("📋 Sample data (last 3 bars):")
	start := len(s.Bars) - 3
	if start < 0 {
		start = 0
	}
	for i := start; i < len(s.Bars); i++ {
		bar := s.Bars[i]
		log.Printf("  Bar %d: %s | O:%.2f H:%.2f L:%.2f C:%.2f V:%.2f",
			i,
			time.UnixMilli(bar.Timestamp).UTC().Format("2006-01-02 15:04:05"),
			bar.Open.InexactFloat64(), bar.High.InexactFloat64(),
			bar.Low.InexactFloat64(), bar.Close.InexactFloat64(), bar.Volume.InexactFloat64())
	}

	// 4. Hard guards - refuse bad data files
	if badOrder > 0 {
		return fmt.Errorf("❌ REFUSED: %d bars have non-monotonic timestamps (out-of-order data)", badOrder)
	}

	// Only fail on bad cadence if it's excessive (>5% of bars)
	cadenceErrorRatio := float64(badCadence) / float64(len(s.Bars))
	if cadenceErrorRatio > 0.05 {
		return fmt.Errorf("❌ REFUSED: %.1f%% of bars have incorrect cadence (%.0f/%d bars) - data appears corrupted", cadenceErrorRatio*100, float64(badCadence), len(s.Bars))
	}

	// Log gaps but don't fail
	if gaps > 0 {
		log.Printf("⚠️  WARNING: %d gaps detected in data (this is normal for real market data)", gaps)
	}

	// Check for reasonable BTC price range (relaxed to warning for long periods)
	// For broader backtests (e.g., 5y), BTC may range outside 10k-200k; warn instead of refusing
	if minC < 10000 || maxC > 200000 {
		log.Printf("⚠️  WARNING: price range %.2f-%.2f is outside [10k,200k] heuristic; continuing due to extended period", minC, maxC)
	}

	// Check for excessive wild jumps (>5% of bars with >20% jumps)
	jumpRatio := float64(jumps) / float64(len(s.Bars))
	if jumpRatio > 0.05 { // More than 5% of bars have wild jumps
		return fmt.Errorf("❌ REFUSED: %.1f%% of bars have >20%% price jumps (%.0f/%d bars) - data appears corrupted", jumpRatio*100, float64(jumps), len(s.Bars))
	}

	// 5. Data period validation
	firstBar := s.Bars[0]
	lastBar := s.Bars[len(s.Bars)-1]
	firstTime := time.UnixMilli(firstBar.Timestamp).UTC()
	lastTime := time.UnixMilli(lastBar.Timestamp).UTC()

	log.Printf("📅 Data period: %s to %s (%d days)",
		firstTime.Format("2006-01-02 15:04:05"),
		lastTime.Format("2006-01-02 15:04:05"),
		int(lastTime.Sub(firstTime).Hours()/24))

	// Verify we're looking at the expected period (Aug-Oct 2025)
	expectedStart := time.Date(2025, 7, 30, 0, 0, 0, 0, time.UTC)
	expectedEnd := time.Date(2025, 10, 27, 23, 59, 59, 0, time.UTC)

	if firstTime.Before(expectedStart) || lastTime.After(expectedEnd) {
		log.Printf("⚠️  WARNING: Data period %s to %s is outside expected Aug-Oct 2025 range",
			firstTime.Format("2006-01-02"), lastTime.Format("2006-01-02"))
	}

	log.Printf("✅ Data validation passed - file appears to contain clean BTCUSDT 5-minute bars")
	return nil
}

// CalculateIndicators calculates EMA and ATR indicators
func (s *EMAATRStrategy) CalculateIndicators() error {
	if len(s.Bars) < s.WarmupBars {
		return fmt.Errorf("insufficient data: need at least %d bars", s.WarmupBars)
	}

	// Initialize indicator arrays
	s.EmaFast = make([]float64, len(s.Bars))
	s.EmaSlow = make([]float64, len(s.Bars))
	s.Atr = make([]float64, len(s.Bars))

	// Calculate EMAs
	s.calculateEMA(s.EmaFastPeriod, s.EmaFast)
	s.calculateEMA(s.EmaSlowPeriod, s.EmaSlow)

	// Calculate ATR
	s.calculateATR(s.AtrPeriod, s.Atr)

	return nil
}

// calculateEMA calculates Exponential Moving Average (TradingView-style)
// Seeds EMA with SMA of first N bars, then uses α=2/(N+1) smoothing
func (s *EMAATRStrategy) calculateEMA(period int, result []float64) {
	if len(s.Bars) < period {
		return
	}

	// TradingView-style: Seed EMA with SMA of first N bars (not N-1)
	var sma float64
	for i := 0; i < period; i++ {
		close, _ := s.Bars[i].Close.Float64()
		sma += close
	}
	sma /= float64(period)
	result[period-1] = sma

	// Precompute constants: α = 2/(N+1)
	alpha := 2.0 / float64(period+1)
	oneMinusAlpha := 1.0 - alpha

	total := len(s.Bars) - period
	for i := period; i < len(s.Bars); i++ {
		close, _ := s.Bars[i].Close.Float64()
		// EMA = close * α + prevEMA * (1-α)
		ema := close*alpha + result[i-1]*oneMinusAlpha
		result[i] = ema

		// Progress log every ~10%% of this EMA pass
		done := i - period + 1
		if total > 0 {
			if done%max(1, total/10) == 0 {
				pct := float64(done) / float64(total) * 100.0
				log.Printf("EMA(period=%d) progress: %.0f%%", period, pct)
			}
		}
	}
}

// calculateEMAUpToBar calculates EMA up to a specific bar index (for manual backtest)
func (s *EMAATRStrategy) calculateEMAUpToBar(period int, result []float64, barIndex int) {
	if barIndex < period-1 {
		return // Not enough data yet
	}

	// If this is the first valid bar for this EMA, calculate the seed
	if barIndex == period-1 {
		var sma float64
		for i := 0; i < period; i++ {
			close, _ := s.Bars[i].Close.Float64()
			sma += close
		}
		sma /= float64(period)
		result[barIndex] = sma
		return
	}

	// Calculate EMA for current bar using previous bar's EMA
	alpha := 2.0 / float64(period+1)
	oneMinusAlpha := 1.0 - alpha
	close, _ := s.Bars[barIndex].Close.Float64()
	ema := close*alpha + result[barIndex-1]*oneMinusAlpha
	result[barIndex] = ema
}

// calculateATRUpToBar calculates ATR up to a specific bar index (for manual backtest)
func (s *EMAATRStrategy) calculateATRUpToBar(period int, result []float64, barIndex int) {
	if barIndex < period {
		return // Not enough data yet
	}

	// If this is the first valid bar for ATR, calculate the seed
	if barIndex == period {
		var atr float64
		for i := 1; i <= period; i++ {
			high, _ := s.Bars[i].High.Float64()
			low, _ := s.Bars[i].Low.Float64()
			prevClose, _ := s.Bars[i-1].Close.Float64()

			tr1 := high - low
			tr2 := math.Abs(high - prevClose)
			tr3 := math.Abs(low - prevClose)

			tr := math.Max(tr1, math.Max(tr2, tr3))
			atr += tr
		}
		atr /= float64(period)
		result[barIndex] = atr
		return
	}

	// Calculate ATR for current bar using Wilder's smoothing
	high, _ := s.Bars[barIndex].High.Float64()
	low, _ := s.Bars[barIndex].Low.Float64()
	prevClose, _ := s.Bars[barIndex-1].Close.Float64()

	tr1 := high - low
	tr2 := math.Abs(high - prevClose)
	tr3 := math.Abs(low - prevClose)

	tr := math.Max(tr1, math.Max(tr2, tr3))

	// Wilder's smoothing: RMA = (RMA * (N-1) + TR) / N
	periodMinus1 := float64(period - 1)
	periodFloat := float64(period)
	atr := (result[barIndex-1]*periodMinus1 + tr) / periodFloat
	result[barIndex] = atr
}

// calculateATR calculates Average True Range using Wilder's smoothing (RMA)
// Seeds RMA with SMA of first N True Range values, then uses Wilder's smoothing
func (s *EMAATRStrategy) calculateATR(period int, result []float64) {
	if len(s.Bars) < period+1 {
		return
	}

	// Calculate True Range for each bar
	tr := make([]float64, len(s.Bars))
	for i := 1; i < len(s.Bars); i++ {
		high, _ := s.Bars[i].High.Float64()
		low, _ := s.Bars[i].Low.Float64()
		prevClose, _ := s.Bars[i-1].Close.Float64()

		tr1 := high - low
		tr2 := math.Abs(high - prevClose)
		tr3 := math.Abs(low - prevClose)

		tr[i] = math.Max(tr1, math.Max(tr2, tr3))
	}

	// Wilder's RMA: Seed with SMA of first N True Range values
	var atr float64
	for i := 1; i <= period; i++ {
		atr += tr[i]
	}
	atr /= float64(period)
	result[period] = atr

	// Wilder's smoothing: RMA = (RMA * (N-1) + TR) / N
	periodMinus1 := float64(period - 1)
	periodFloat := float64(period)

	total := len(s.Bars) - (period + 1)
	for i := period + 1; i < len(s.Bars); i++ {
		atr = (atr*periodMinus1 + tr[i]) / periodFloat
		result[i] = atr

		// Progress log every ~10%% of ATR smoothing
		done := i - (period + 1) + 1
		if total > 0 {
			if done%max(1, total/10) == 0 {
				pct := float64(done) / float64(total) * 100.0
				log.Printf("ATR(period=%d) progress: %.0f%%", period, pct)
			}
		}
	}
}

// max returns the larger of a and b
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// calculateBodyPctOfPrice returns (close-open)/open (signed fraction; 0.002 = +0.2%)
func (s *EMAATRStrategy) calculateBodyPctOfPrice(bar Bar) decimal.Decimal {
	if bar.Open.IsZero() {
		return decimal.Zero
	}
	body := bar.Close.Sub(bar.Open)
	return body.Div(bar.Open)
}

// ResolveFirstTouchLong determines which exit (TP or SL) was hit first for a long position
func (s *EMAATRStrategy) ResolveFirstTouchLong(bar Bar, takeProfit, stopLoss decimal.Decimal) (hitTP, hitSL bool, exitPrice decimal.Decimal, reason string) {
	hitTP = bar.High.GreaterThanOrEqual(takeProfit)
	hitSL = bar.Low.LessThanOrEqual(stopLoss)

	if !hitTP && !hitSL {
		return false, false, decimal.Zero, "none"
	}

	if !hitTP {
		return false, true, stopLoss, "StopLoss"
	}
	if !hitSL {
		return true, false, takeProfit, "TakeProfit"
	}

	// Both hit - resolve by policy
	switch s.FirstTouchPolicy {
	case FirstTouchPolicySLFirst:
		return false, true, stopLoss, "StopLoss"
	case FirstTouchPolicyTPFirst:
		return true, false, takeProfit, "TakeProfit"
	case FirstTouchPolicyChart:
		// Chart order: use OHLC path to determine which level is hit first
		// If candle closes up: Open → Low → High → Close
		// If candle closes down: Open → High → Low → Close
		if bar.Close.GreaterThanOrEqual(bar.Open) {
			// Up candle: Open → Low → High → Close
			// Check if SL (below entry) is hit before TP (above entry)
			if takeProfit.GreaterThan(bar.Open) && stopLoss.LessThan(bar.Open) {
				// Both levels are within the bar range
				if bar.Low.LessThanOrEqual(stopLoss) {
					return false, true, stopLoss, "StopLoss" // SL hit first on path
				}
				if bar.High.GreaterThanOrEqual(takeProfit) {
					return true, false, takeProfit, "TakeProfit" // TP hit after SL check
				}
			}
		} else {
			// Down candle: Open → High → Low → Close
			// Check if TP (above entry) is hit before SL (below entry)
			if takeProfit.GreaterThan(bar.Open) && stopLoss.LessThan(bar.Open) {
				// Both levels are within the bar range
				if bar.High.GreaterThanOrEqual(takeProfit) {
					return true, false, takeProfit, "TakeProfit" // TP hit first on path
				}
				if bar.Low.LessThanOrEqual(stopLoss) {
					return false, true, stopLoss, "StopLoss" // SL hit after TP check
				}
			}
		}
		// Fallback to distance if levels are outside bar range
		entryPrice := s.ActivePosition.EntryPrice
		tpDistance := takeProfit.Sub(entryPrice).Abs()
		slDistance := entryPrice.Sub(stopLoss).Abs()
		if tpDistance.LessThanOrEqual(slDistance) {
			return true, false, takeProfit, "TakeProfit"
		}
		return false, true, stopLoss, "StopLoss"
	default:
		return false, true, stopLoss, "StopLoss"
	}
}

// ResolveFirstTouchShort determines which exit (TP or SL) was hit first for a short position
func (s *EMAATRStrategy) ResolveFirstTouchShort(bar Bar, takeProfit, stopLoss decimal.Decimal) (hitTP, hitSL bool, exitPrice decimal.Decimal, reason string) {
	hitTP = bar.Low.LessThanOrEqual(takeProfit)
	hitSL = bar.High.GreaterThanOrEqual(stopLoss)

	if !hitTP && !hitSL {
		return false, false, decimal.Zero, "none"
	}

	if !hitTP {
		return false, true, stopLoss, "StopLoss"
	}
	if !hitSL {
		return true, false, takeProfit, "TakeProfit"
	}

	// Both hit - resolve by policy
	switch s.FirstTouchPolicy {
	case FirstTouchPolicySLFirst:
		return false, true, stopLoss, "StopLoss"
	case FirstTouchPolicyTPFirst:
		return true, false, takeProfit, "TakeProfit"
	case FirstTouchPolicyChart:
		// Chart order: use OHLC path to determine which level is hit first
		// If candle closes up: Open → Low → High → Close
		// If candle closes down: Open → High → Low → Close
		if bar.Close.GreaterThanOrEqual(bar.Open) {
			// Up candle: Open → Low → High → Close
			// For shorts: TP is below entry, SL is above entry
			if takeProfit.LessThan(bar.Open) && stopLoss.GreaterThan(bar.Open) {
				// Both levels are within the bar range
				if bar.High.GreaterThanOrEqual(stopLoss) {
					return false, true, stopLoss, "StopLoss" // SL hit first on path
				}
				if bar.Low.LessThanOrEqual(takeProfit) {
					return true, false, takeProfit, "TakeProfit" // TP hit after SL check
				}
			}
		} else {
			// Down candle: Open → High → Low → Close
			// For shorts: TP is below entry, SL is above entry
			if takeProfit.LessThan(bar.Open) && stopLoss.GreaterThan(bar.Open) {
				// Both levels are within the bar range
				if bar.Low.LessThanOrEqual(takeProfit) {
					return true, false, takeProfit, "TakeProfit" // TP hit first on path
				}
				if bar.High.GreaterThanOrEqual(stopLoss) {
					return false, true, stopLoss, "StopLoss" // SL hit after TP check
				}
			}
		}
		// Fallback to distance if levels are outside bar range
		entryPrice := s.ActivePosition.EntryPrice
		tpDistance := entryPrice.Sub(takeProfit).Abs()
		slDistance := stopLoss.Sub(entryPrice).Abs()
		if tpDistance.LessThanOrEqual(slDistance) {
			return true, false, takeProfit, "TakeProfit"
		}
		return false, true, stopLoss, "StopLoss"
	default:
		return false, true, stopLoss, "StopLoss"
	}
}

// Run executes the strategy with proper manual backtest sequencing
func (s *EMAATRStrategy) Run() error {
	s.PerfMetrics.StartTime = time.Now()

	// Initialize indicator arrays
	s.EmaFast = make([]float64, len(s.Bars))
	s.EmaSlow = make([]float64, len(s.Bars))
	s.Atr = make([]float64, len(s.Bars))

	// Validate warm-up: ensure we have enough bars for all indicators
	slowestLookback := s.EmaSlowPeriod    // EMA100 is slowest
	requiredWarmup := slowestLookback * 3 // 3x multiplier for convergence
	if s.WarmupBars < requiredWarmup {
		log.Printf("⚠️  WARNING: Warmup bars (%d) less than recommended (3x slowest lookback = %d)", s.WarmupBars, requiredWarmup)
	}

	log.Printf("Starting manual backtest on %d bars", len(s.Bars))
	log.Printf("Warm-up period: bars 0 to %d (calculating indicators)", s.WarmupBars-1)
	log.Printf("Trading period: bars %d to %d (scanning every candle)", s.WarmupBars, len(s.Bars)-1)

	// MANUAL BACKTEST FLOW:
	// 1. WARM-UP: Calculate indicators for warm-up period (no trading)
	// 2. SCAN: For each candle, recalculate indicators, check conditions, track TP/SL

	// Step 1: Warm-up period (calculate indicators, no trading)
	log.Printf("🔄 WARM-UP: Calculating indicators for bars 0 to %d...", s.WarmupBars-1)
	for i := 0; i < s.WarmupBars; i++ {
		// Calculate indicators up to current bar
		s.calculateEMAUpToBar(s.EmaFastPeriod, s.EmaFast, i)
		s.calculateEMAUpToBar(s.EmaSlowPeriod, s.EmaSlow, i)
		s.calculateATRUpToBar(s.AtrPeriod, s.Atr, i)
	}
	log.Printf("✅ Warm-up complete. Indicators ready for trading.")

	// Step 2: Trading period (scan every candle)
	log.Printf("🔍 SCANNING: Processing bars %d to %d (every candle)...", s.WarmupBars, len(s.Bars)-1)

	// Print table header for verbose mode
	if s.Verbose {
		log.Printf("┌─────────────────────┬──────┬──────────┬──────────┬──────────┬────────┬──────────────┬──────────┬──────────┬────────┬────────┬────────┬────────┬────────┬─────────────┬─────────────┬──────────┬────────┐")
		log.Printf("│ Date & Time         │ Type │ Entry    │ Exit     │ PnL      │ PnL%%  │ Reason       │ TP       │ SL       │ Open   │ High   │ Low    │ Close  │ Volume │ EMA26       │ EMA100      │ ATR       │ Body%  │")
		log.Printf("├─────────────────────┼──────┼──────────┼──────────┼──────────┼────────┼──────────────┼──────────┼──────────┼────────┼────────┼────────┼────────┼────────┼─────────────┼─────────────┼──────────┼────────┤")
	}

	for i := s.WarmupBars; i < len(s.Bars); i++ {
		bar := s.Bars[i]

		// ┌─────────────────────────────────────────────────────────────────┐
		// │ MANUAL BACKTEST FLOW - PROCESS EACH CANDLE SEQUENTIALLY         │
		// └─────────────────────────────────────────────────────────────────┘

		// STEP 1: Calculate indicators for current candle
		// (This simulates what happens in live trading - indicators update on each bar)
		s.calculateEMAUpToBar(s.EmaFastPeriod, s.EmaFast, i)
		s.calculateEMAUpToBar(s.EmaSlowPeriod, s.EmaSlow, i)
		s.calculateATRUpToBar(s.AtrPeriod, s.Atr, i)

		// Record indicator trace for every candle (for validation)
		if s.EnableTraces {
			state := "flat"
			if s.ActivePosition != nil {
				state = strings.ToLower(s.ActivePosition.TradeType)
			}
			s.IndicatorTraces = append(s.IndicatorTraces, IndicatorTraceEntry{
				Timestamp: bar.Timestamp,
				Open:      bar.Open,
				High:      bar.High,
				Low:       bar.Low,
				Close:     bar.Close,
				EmaFast:   s.EmaFast[i],
				EmaSlow:   s.EmaSlow[i],
				Atr:       s.Atr[i],
				State:     state,
			})
		}

		// STEP 2: Check if entry conditions are met (evaluate strategy logic)
		// If conditions met, proceed to STEP 3 to calculate TP/SL
		if s.ActivePosition == nil {
			// Execute pending entry at this bar open if scheduled
			if s.NextEntry != nil && s.NextEntry.ActivateAtTs == bar.Timestamp {
				s.executePendingEntry(bar, i)
				// After executePendingEntry, TP/SL is calculated and position is opened
			} else {
				// Check for new entry signals
				s.checkEntry(bar, i)
				// If checkEntry finds a signal, it schedules NextEntry with TP/SL calculated
			}
		}

		// STEP 3: TP/SL calculation (done automatically when entry happens)
		// This happens inside enterLongAtOpen() / enterShortAtOpen() functions
		// TP/SL are calculated based on ATR and stored in ActivePosition

		// STEP 4: Track price movement to see if TP/SL is hit
		// This happens on EVERY candle after a position is opened
		// We analyze the bar's High/Low to determine if TP or SL was hit first
		if s.ActivePosition != nil {
			s.trackPriceMovementForTP_SL(bar, i)
		}

		// Update equity and drawdown
		s.updateEquity()
		s.PerfMetrics.BarsProcessed++

		// Log candle calculations based on verbosity level
		if s.Verbose {
			// VERBOSE MODE: Show detailed table for every candle
			s.logCandleDetails(bar, i)
		} else {
			// SUMMARY MODE: Log progress every 1000th bar
			if i%1000 == 0 || i == s.WarmupBars {
				state := "flat"
				if s.ActivePosition != nil {
					state = strings.ToLower(s.ActivePosition.TradeType)
				}
				log.Printf("📊 Processed bar %d/%d (%.1f%%) - Time: %s, Close: %s, EMA26: %.2f, EMA100: %.2f, ATR: %.2f, State: %s",
					i, len(s.Bars)-1,
					float64(i-s.WarmupBars)/float64(len(s.Bars)-s.WarmupBars)*100,
					time.UnixMilli(bar.Timestamp).UTC().Format("2006-01-02 15:04:05"),
					bar.Close.StringFixed(2),
					s.EmaFast[i], s.EmaSlow[i], s.Atr[i], state)
			}
		}

		// LOOP CONTINUES: Repeat steps 1-4 for next candle
	}

	// Print table footer for verbose mode
	if s.Verbose {
		log.Printf("└─────────────────────┴──────┴──────────┴──────────┴──────────┴────────┴──────────────┴──────────┴──────────┴────────┴────────┴────────┴────────┴────────┴─────────────┴─────────────┴──────────┴────────┘")
	}

	// Close any remaining position
	if s.ActivePosition != nil {
		s.closePosition(s.Bars[len(s.Bars)-1], len(s.Bars)-1, "EndOfData")
	}

	s.PerfMetrics.EndTime = time.Now()
	duration := s.PerfMetrics.EndTime.Sub(s.PerfMetrics.StartTime)
	s.PerfMetrics.BarsPerSecond = float64(s.PerfMetrics.BarsProcessed) / duration.Seconds()

	log.Printf("✅ Manual backtest completed: %d bars processed in %v (%.1f bars/sec)",
		s.PerfMetrics.BarsProcessed, duration, s.PerfMetrics.BarsPerSecond)

	return nil
}

// logCandleDetails logs detailed candle information in table format
func (s *EMAATRStrategy) logCandleDetails(bar Bar, barIndex int) {
	// Format timestamp with date and time
	timestamp := time.UnixMilli(bar.Timestamp).UTC()
	dateTime := timestamp.Format("2006-01-02 15:04:05")

	// Determine trade type and entry/exit prices
	tradeType := "Flat"
	entryPrice := ""
	exitPrice := ""
	pnl := ""
	pnlPct := ""
	reason := ""
	tpPrice := ""
	slPrice := ""

	if s.ActivePosition != nil {
		tradeType = s.ActivePosition.TradeType
		entryPrice = s.ActivePosition.EntryPrice.StringFixed(2)
		exitPrice = "Waiting"
		pnl = "Waiting"
		pnlPct = "Waiting"
		reason = "Waiting"
		tpPrice = s.ActivePosition.TakeProfit.StringFixed(2)
		slPrice = s.ActivePosition.StopLoss.StringFixed(2)
	}

	// Check for entry signals
	if s.ActivePosition == nil && barIndex > 0 {
		emaFast := decimal.NewFromFloat(s.EmaFast[barIndex])
		emaSlow := decimal.NewFromFloat(s.EmaSlow[barIndex])
		prevEmaFast := decimal.NewFromFloat(s.EmaFast[barIndex-1])
		prevEmaSlow := decimal.NewFromFloat(s.EmaSlow[barIndex-1])

		longCond := emaFast.GreaterThan(emaSlow) && prevEmaFast.LessThanOrEqual(prevEmaSlow)
		shortCond := emaFast.LessThan(emaSlow) && prevEmaFast.GreaterThanOrEqual(prevEmaSlow)

		if longCond {
			reason = "LONG_SIGNAL"
		} else if shortCond {
			reason = "SHORT_SIGNAL"
		} else {
			reason = "NO_SIGNAL"
		}
	} else if s.ActivePosition != nil {
		// Check if we're waiting for TP/SL or if it was hit
		reason = "Waiting"
	}

	// Compute Body% signed for display
	bodyPct := s.calculateBodyPctOfPrice(bar).Mul(decimal.NewFromFloat(100)).StringFixed(2)

	// Log in table format (main bar row)
	log.Printf("| %s | %-4s | %-8s | %-8s | %-8s | %-6s | %-12s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | EMA26: %7.2f | EMA100: %7.2f | ATR: %6.2f | %6s |",
		dateTime, tradeType, entryPrice, exitPrice, pnl, pnlPct, reason, tpPrice, slPrice,
		bar.Open.StringFixed(2), bar.High.StringFixed(2), bar.Low.StringFixed(2), bar.Close.StringFixed(2), bar.Volume.StringFixed(2),
		s.EmaFast[barIndex], s.EmaSlow[barIndex], s.Atr[barIndex], bodyPct)

	// Compute scanner conditions
	emaFastD := decimal.NewFromFloat(s.EmaFast[barIndex])
	emaSlowD := decimal.NewFromFloat(s.EmaSlow[barIndex])
	bodyFrac := s.calculateBodyPctOfPrice(bar) // signed fraction
	longEma := emaFastD.GreaterThan(emaSlowD)
	longClose := bar.Close.GreaterThan(emaFastD)
	longBody := bodyFrac.GreaterThanOrEqual(s.BodyPctMinLong) && bodyFrac.LessThanOrEqual(s.BodyPctMaxLong)
	shortEma := emaFastD.LessThan(emaSlowD)
	shortClose := bar.Close.LessThan(emaFastD)
	shortBody := bodyFrac.GreaterThanOrEqual(s.BodyPctMinShort) && bodyFrac.LessThanOrEqual(s.BodyPctMaxShort)
	condStr := fmt.Sprintf("L[ema>%t close>%t body%t] S[ema<%t close<%t body%t] B=%s%%",
		longEma, longClose, longBody, shortEma, shortClose, shortBody, bodyPct)

	// Secondary row: show conditions scanner under the main row (use Reason column to display)
	log.Printf("| %-19s | %-4s | %-8s | %-8s | %-8s | %-6s | %-12s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | %13s | %13s | %10s | %6s |",
		"", "SCAN", "-", "-", "-", "-", condStr, "-", "-",
		"-", "-", "-", "-", "-", "-", "-", "-", "")

	// Store data for Excel export
	if s.EnableExcel {
		// Main row for bar
		s.ExcelRows = append(s.ExcelRows, ExcelRow{
			DateTime: dateTime,
			Type:     tradeType,
			Entry:    entryPrice,
			Exit:     exitPrice,
			PnL:      pnl,
			PnLPct:   pnlPct,
			Reason:   reason,
			TP:       tpPrice,
			SL:       slPrice,
			Open:     bar.Open.StringFixed(2),
			High:     bar.High.StringFixed(2),
			Low:      bar.Low.StringFixed(2),
			Close:    bar.Close.StringFixed(2),
			Volume:   bar.Volume.StringFixed(2),
			EMA26:    s.EmaFast[barIndex],
			EMA100:   s.EmaSlow[barIndex],
			ATR:      s.Atr[barIndex],
			BodyPct:  bodyPct,
		})
		// Scanner row directly after
		s.ExcelRows = append(s.ExcelRows, ExcelRow{
			DateTime:   "",
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
	}
}

// checkEntry checks for entry signals with inclusive thresholds and entry mode support
func (s *EMAATRStrategy) checkEntry(bar Bar, barIndex int) {
	// Need at least 2 bars for comparison
	if barIndex < 1 {
		return
	}

	// Calculate body percentage relative to price
	bodyPct := s.calculateBodyPctOfPrice(bar)

	// Get current indicator values
	emaFast := decimal.NewFromFloat(s.EmaFast[barIndex])
	emaSlow := decimal.NewFromFloat(s.EmaSlow[barIndex])
	atr := decimal.NewFromFloat(s.Atr[barIndex])

	// Track debug info
	if s.EnableDebug {
		longCond := emaFast.GreaterThan(emaSlow)
		shortCond := emaFast.LessThan(emaSlow)
		reasonBlocked := CandidateReasonNone
		var entryPrice, slPrice, tpPrice decimal.Decimal

		if longCond || shortCond {
			entryPrice = bar.Close
			if longCond {
				slPrice = entryPrice.Sub(atr.Mul(s.SlMultiplier))
				tpPrice = entryPrice.Add(entryPrice.Sub(slPrice).Mul(s.AtpMultiplier))
			} else {
				slPrice = entryPrice.Add(atr.Mul(s.SlMultiplier))
				tpPrice = entryPrice.Sub(slPrice.Sub(entryPrice).Mul(s.AtpMultiplier))
			}
		}

		s.DebugBars = append(s.DebugBars, DebugBar{
			Timestamp:     bar.Timestamp,
			Open:          bar.Open,
			High:          bar.High,
			Low:           bar.Low,
			Close:         bar.Close,
			Ema26:         s.EmaFast[barIndex],
			Ema100:        s.EmaSlow[barIndex],
			Atr14:         s.Atr[barIndex],
			BodyPct:       bodyPct,
			LongCond:      longCond,
			ShortCond:     shortCond,
			ReasonBlocked: reasonBlocked,
			EntryPrice:    entryPrice,
			SlPrice:       slPrice,
			TpPrice:       tpPrice,
		})
	}

	// Log detailed analysis every 1000th bar
	if barIndex%1000 == 0 {
		log.Printf("  Bar %d: Close=%s, EMA26=%s, EMA100=%s, ATR=%s, Body%%=%.5f%%",
			barIndex, bar.Close.StringFixed(2),
			emaFast.StringFixed(2), emaSlow.StringFixed(2),
			atr.StringFixed(2), bodyPct.Mul(decimal.NewFromFloat(100)).InexactFloat64())
	}

	// Check for long signal on bar close per rules:
	// EMA26 > EMA100 AND Close > EMA26 AND Body% in [minLong, maxLong]
	longCond := emaFast.GreaterThan(emaSlow) && bar.Close.GreaterThan(emaFast) &&
		bodyPct.GreaterThanOrEqual(s.BodyPctMinLong) && bodyPct.LessThanOrEqual(s.BodyPctMaxLong)
	if longCond {
		// INCLUSIVE thresholds already checked above
		bodyPctOK := true

		candidate := SignalCandidate{
			BarIndex:  barIndex,
			Timestamp: bar.Timestamp,
			Bar:       bar,
			EmaFast:   s.EmaFast[barIndex],
			EmaSlow:   s.EmaSlow[barIndex],
			Atr:       s.Atr[barIndex],
			BodyPct:   bodyPct,
			TradeType: "Long",
		}

		if !bodyPctOK {
			if bodyPct.LessThan(s.BodyPctMinLong) {
				candidate.Reason = CandidateReasonBodyPctLow
			} else {
				candidate.Reason = CandidateReasonBodyPctHigh
			}
			if s.EnableDebug {
				s.Candidates = append(s.Candidates, candidate)
			}
			return
		}

		// Entry mode: on_signal_close or next_bar_open
		if s.EntryMode == EntryModeSignalClose {
			s.enterLongAtOpen(bar, atr, barIndex)
			log.Printf("🟢 LONG SIGNAL at bar %d -> entering at close %s", barIndex,
				time.UnixMilli(bar.Timestamp).UTC().Format("2006-01-02 15:04:05"))
		} else {
			if barIndex+1 < len(s.Bars) {
				// Use ATR from signal bar or entry bar based on ATRTiming setting
				entryAtr := atr
				if s.ATRTiming == "entry" && barIndex+1 < len(s.Bars) {
					entryAtr = decimal.NewFromFloat(s.Atr[barIndex+1])
				}
				s.NextEntry = &struct {
					ActivateAtTs int64
					TradeType    string
					Atr          decimal.Decimal
				}{
					ActivateAtTs: s.Bars[barIndex+1].Timestamp,
					TradeType:    "Long",
					Atr:          entryAtr,
				}
				log.Printf("🟢 LONG SIGNAL at bar %d -> entering next open at %s", barIndex,
					time.UnixMilli(s.Bars[barIndex+1].Timestamp).UTC().Format("2006-01-02 15:04:05"))
			}
		}

		if s.EnableDebug {
			candidate.Reason = CandidateReasonNone
			stopLoss := bar.Close.Sub(atr.Mul(s.SlMultiplier))
			takeProfit := bar.Close.Add(bar.Close.Sub(stopLoss).Mul(s.AtpMultiplier))
			candidate.EntryPrice = bar.Close
			candidate.SlPrice = stopLoss
			candidate.TpPrice = takeProfit
			s.Candidates = append(s.Candidates, candidate)
		}
		return
	}

	// Check for short signal on bar close per rules:
	// EMA26 < EMA100 AND Close < EMA26 AND Body% in [minShort, maxShort] (negative range)
	shortCond := emaFast.LessThan(emaSlow) && bar.Close.LessThan(emaFast) &&
		bodyPct.GreaterThanOrEqual(s.BodyPctMinShort) && bodyPct.LessThanOrEqual(s.BodyPctMaxShort)
	if shortCond {
		bodyPctOK := true

		candidate := SignalCandidate{
			BarIndex:  barIndex,
			Timestamp: bar.Timestamp,
			Bar:       bar,
			EmaFast:   s.EmaFast[barIndex],
			EmaSlow:   s.EmaSlow[barIndex],
			Atr:       s.Atr[barIndex],
			BodyPct:   bodyPct,
			TradeType: "Short",
		}

		if !bodyPctOK {
			if bodyPct.LessThan(s.BodyPctMinShort) {
				candidate.Reason = CandidateReasonBodyPctLow
			} else {
				candidate.Reason = CandidateReasonBodyPctHigh
			}
			if s.EnableDebug {
				s.Candidates = append(s.Candidates, candidate)
			}
			return
		}

		// Entry mode: on_signal_close or next_bar_open
		if s.EntryMode == EntryModeSignalClose {
			s.enterShortAtOpen(bar, atr, barIndex)
			log.Printf("🔴 SHORT SIGNAL at bar %d -> entering at close %s", barIndex,
				time.UnixMilli(bar.Timestamp).UTC().Format("2006-01-02 15:04:05"))
		} else {
			if barIndex+1 < len(s.Bars) {
				// Use ATR from signal bar or entry bar based on ATRTiming setting
				entryAtr := atr
				if s.ATRTiming == "entry" && barIndex+1 < len(s.Bars) {
					entryAtr = decimal.NewFromFloat(s.Atr[barIndex+1])
				}
				s.NextEntry = &struct {
					ActivateAtTs int64
					TradeType    string
					Atr          decimal.Decimal
				}{
					ActivateAtTs: s.Bars[barIndex+1].Timestamp,
					TradeType:    "Short",
					Atr:          entryAtr,
				}
				log.Printf("🔴 SHORT SIGNAL at bar %d -> entering next open at %s", barIndex,
					time.UnixMilli(s.Bars[barIndex+1].Timestamp).UTC().Format("2006-01-02 15:04:05"))
			}
		}

		if s.EnableDebug {
			candidate.Reason = CandidateReasonNone
			stopLoss := bar.Close.Add(atr.Mul(s.SlMultiplier))
			takeProfit := bar.Close.Sub(stopLoss.Sub(bar.Close).Mul(s.AtpMultiplier))
			candidate.EntryPrice = bar.Close
			candidate.SlPrice = stopLoss
			candidate.TpPrice = takeProfit
			s.Candidates = append(s.Candidates, candidate)
		}
	}
}

// applyExchangeFilters applies exchange rules to order price and quantity
// Returns filtered price, quantity, and whether order passes filters
func (s *EMAATRStrategy) applyExchangeFilters(price, quantity decimal.Decimal) (filteredPrice, filteredQty decimal.Decimal, passes bool) {
	// 1. Quantize price to tick size
	filteredPrice = price.Div(s.ExchangeRules.TickSize).Round(0).Mul(s.ExchangeRules.TickSize)

	// 2. Quantize quantity to lot size
	filteredQty = quantity.Div(s.ExchangeRules.LotSize).Round(0).Mul(s.ExchangeRules.LotSize)

	// 3. Check minimum notional
	notional := filteredPrice.Mul(filteredQty)
	if notional.LessThan(s.ExchangeRules.MinNotional) {
		return filteredPrice, filteredQty, false
	}

	return filteredPrice, filteredQty, true
}

// calculateSlippage applies slippage based on slippage mode
func (s *EMAATRStrategy) calculateSlippage(basePrice decimal.Decimal, side string) decimal.Decimal {
	switch s.SlippageMode {
	case "NONE":
		return decimal.Zero
	case "TRADE_SWEEP":
		// 0.01% - 0.1% slippage
		slippageRate := decimal.NewFromFloat(0.0001) // 0.01%
		if side == "Buy" {
			return basePrice.Mul(slippageRate) // Add slippage for buys
		}
		return basePrice.Mul(slippageRate.Neg()) // Subtract slippage for sells
	case "SYNTHETIC_BOOK":
		// 0.05% - 0.5% slippage
		slippageRate := decimal.NewFromFloat(0.0005) // 0.05%
		if side == "Buy" {
			return basePrice.Mul(slippageRate)
		}
		return basePrice.Mul(slippageRate.Neg())
	default:
		return decimal.Zero
	}
}

// calculateFee calculates trading fee based on notional and fee version
func (s *EMAATRStrategy) calculateFee(notional decimal.Decimal, isMaker bool) decimal.Decimal {
	feeRate := s.ExchangeRules.TakerFee
	if isMaker {
		feeRate = s.ExchangeRules.MakerFee
	}
	return notional.Mul(feeRate)
}

func (s *EMAATRStrategy) executePendingEntry(bar Bar, barIndex int) {
	if s.NextEntry == nil {
		return
	}
	atr := s.NextEntry.Atr
	if s.NextEntry.TradeType == "Long" {
		s.enterLongAtOpen(bar, atr, barIndex)
	} else {
		s.enterShortAtOpen(bar, atr, barIndex)
	}
	s.NextEntry = nil
}

// enterLong enters a long position
func (s *EMAATRStrategy) enterLongAtOpen(bar Bar, atr decimal.Decimal, barIndex int) {
	// Use close price if EntryModeSignalClose, otherwise use open
	entryPricePreFilter := bar.Open
	if s.EntryMode == EntryModeSignalClose {
		entryPricePreFilter = bar.Close
	}

	// Apply slippage
	slippage := s.calculateSlippage(entryPricePreFilter, "Long")
	entryPriceWithSlippage := entryPricePreFilter.Add(slippage)

	// Calculate TP and SL: TP = SL * 2.2, SL = ATR * multiplier
	stopLoss := entryPriceWithSlippage.Sub(atr.Mul(s.SlMultiplier))
	takeProfit := entryPriceWithSlippage.Add(entryPriceWithSlippage.Sub(stopLoss).Mul(s.AtpMultiplier))

	// Calculate position size based on sizing mode
	var quantityPreFilter decimal.Decimal
	if s.SizingMode == SizingModeRisk {
		// Risk-based sizing: quantity = riskUsd / |entry - SL|
		riskDistance := entryPriceWithSlippage.Sub(stopLoss).Abs()
		if riskDistance.GreaterThan(decimal.Zero) {
			quantityPreFilter = s.RiskAmount.Div(riskDistance)
		} else {
			quantityPreFilter = s.RiskAmount.Div(entryPriceWithSlippage)
		}
	} else {
		// Notional-based sizing (current behavior)
		quantityPreFilter = s.RiskAmount.Div(entryPriceWithSlippage)
	}

	// Apply exchange filters (tick size, lot size, min notional)
	filteredPrice, filteredQty, passes := s.applyExchangeFilters(entryPriceWithSlippage, quantityPreFilter)
	if !passes {
		s.RejectedTrades = append(s.RejectedTrades, fmt.Sprintf("LONG rejected at bar %d: min notional not met", barIndex))
		log.Printf("⚠️  LONG ENTRY REJECTED at bar %d: min notional not met (pre-filter: %s, post-filter: %s)",
			barIndex, entryPriceWithSlippage.StringFixed(2), filteredPrice.StringFixed(2))
		return
	}

	// Calculate entry fee with filtered values
	entryFee := s.calculateFee(filteredPrice.Mul(filteredQty), false) // Assume taker

	// Calculate TTL based on max holding bars
	ttlMs := int64(s.MaxHoldingBars) * s.CadenceMs

	// Record signal trace
	if s.EnableTraces {
		s.SignalTraces = append(s.SignalTraces, SignalTraceEntry{
			Timestamp:            bar.Timestamp,
			ReasonCode:           "ema_cross_long",
			EmaCross:             true,
			BodyPct:              s.calculateBodyPctOfPrice(bar),
			Atr:                  atr.InexactFloat64(),
			EntrySide:            "Long",
			EntryPricePreFilter:  entryPricePreFilter,
			EntryPricePostFilter: filteredPrice,
			QtyPreFilter:         quantityPreFilter,
			QtyPostFilter:        filteredQty,
			Slippage:             slippage,
			Fees:                 entryFee,
		})
	}

	s.ActivePosition = &ActivePosition{
		Symbol:        "BTCUSDT",
		TradeType:     "Long",
		EntryTime:     bar.Timestamp,
		EntryPrice:    filteredPrice,
		Quantity:      filteredQty,
		TakeProfit:    takeProfit,
		StopLoss:      stopLoss,
		TimeToLive:    bar.Timestamp + ttlMs,
		EntryFee:      entryFee,
		SizeUsd:       filteredPrice.Mul(filteredQty),
		AtrAtEntry:    atr,
		EntryBarIndex: barIndex,
	}

	log.Printf("Entered LONG at %s (pre-filter: %s, slippage: %s), TP: %s, SL: %s, Qty: %s (pre-filter: %s), Size: $%s",
		filteredPrice.StringFixed(2), entryPricePreFilter.StringFixed(2), slippage.StringFixed(6),
		takeProfit.StringFixed(2), stopLoss.StringFixed(2),
		filteredQty.StringFixed(6), quantityPreFilter.StringFixed(6),
		filteredPrice.Mul(filteredQty).StringFixed(2))
}

// enterShort enters a short position
func (s *EMAATRStrategy) enterShortAtOpen(bar Bar, atr decimal.Decimal, barIndex int) {
	// Use close price if EntryModeSignalClose, otherwise use open
	entryPricePreFilter := bar.Open
	if s.EntryMode == EntryModeSignalClose {
		entryPricePreFilter = bar.Close
	}

	// Apply slippage
	slippage := s.calculateSlippage(entryPricePreFilter, "Short")
	entryPriceWithSlippage := entryPricePreFilter.Add(slippage)

	// Calculate TP and SL: TP = SL * 2.2, SL = ATR * multiplier
	stopLoss := entryPriceWithSlippage.Add(atr.Mul(s.SlMultiplier))
	takeProfit := entryPriceWithSlippage.Sub(stopLoss.Sub(entryPriceWithSlippage).Mul(s.AtpMultiplier))

	// Calculate position size based on sizing mode
	var quantityPreFilter decimal.Decimal
	if s.SizingMode == SizingModeRisk {
		// Risk-based sizing: quantity = riskUsd / |entry - SL|
		riskDistance := stopLoss.Sub(entryPriceWithSlippage).Abs()
		if riskDistance.GreaterThan(decimal.Zero) {
			quantityPreFilter = s.RiskAmount.Div(riskDistance)
		} else {
			quantityPreFilter = s.RiskAmount.Div(entryPriceWithSlippage)
		}
	} else {
		// Notional-based sizing (current behavior)
		quantityPreFilter = s.RiskAmount.Div(entryPriceWithSlippage)
	}

	// Apply exchange filters (tick size, lot size, min notional)
	filteredPrice, filteredQty, passes := s.applyExchangeFilters(entryPriceWithSlippage, quantityPreFilter)
	if !passes {
		s.RejectedTrades = append(s.RejectedTrades, fmt.Sprintf("SHORT rejected at bar %d: min notional not met", barIndex))
		log.Printf("⚠️  SHORT ENTRY REJECTED at bar %d: min notional not met (pre-filter: %s, post-filter: %s)",
			barIndex, entryPriceWithSlippage.StringFixed(2), filteredPrice.StringFixed(2))
		return
	}

	// Calculate entry fee with filtered values
	entryFee := s.calculateFee(filteredPrice.Mul(filteredQty), false) // Assume taker

	// Calculate TTL based on max holding bars
	ttlMs := int64(s.MaxHoldingBars) * s.CadenceMs

	// Record signal trace
	if s.EnableTraces {
		s.SignalTraces = append(s.SignalTraces, SignalTraceEntry{
			Timestamp:            bar.Timestamp,
			ReasonCode:           "ema_cross_short",
			EmaCross:             true,
			BodyPct:              s.calculateBodyPctOfPrice(bar),
			Atr:                  atr.InexactFloat64(),
			EntrySide:            "Short",
			EntryPricePreFilter:  entryPricePreFilter,
			EntryPricePostFilter: filteredPrice,
			QtyPreFilter:         quantityPreFilter,
			QtyPostFilter:        filteredQty,
			Slippage:             slippage,
			Fees:                 entryFee,
		})
	}

	s.ActivePosition = &ActivePosition{
		Symbol:        "BTCUSDT",
		TradeType:     "Short",
		EntryTime:     bar.Timestamp,
		EntryPrice:    filteredPrice,
		Quantity:      filteredQty,
		TakeProfit:    takeProfit,
		StopLoss:      stopLoss,
		TimeToLive:    bar.Timestamp + ttlMs,
		EntryFee:      entryFee,
		SizeUsd:       filteredPrice.Mul(filteredQty),
		AtrAtEntry:    atr,
		EntryBarIndex: barIndex,
	}

	log.Printf("Entered SHORT at %s (pre-filter: %s, slippage: %s), TP: %s, SL: %s, Qty: %s (pre-filter: %s), Size: $%s",
		filteredPrice.StringFixed(2), entryPricePreFilter.StringFixed(2), slippage.StringFixed(6),
		takeProfit.StringFixed(2), stopLoss.StringFixed(2),
		filteredQty.StringFixed(6), quantityPreFilter.StringFixed(6),
		filteredPrice.Mul(filteredQty).StringFixed(2))
}

// trackPriceMovementForTP_SL is STEP 4 of manual backtest flow
// This function tracks price movement on EVERY candle after a position is opened
// It analyzes the bar's High/Low to determine if TP or SL was hit first
// Using first-touch resolution based on configured intrabar policy
func (s *EMAATRStrategy) trackPriceMovementForTP_SL(bar Bar, barIndex int) {
	if s.ActivePosition == nil {
		return
	}

	// Log position status every 1000th bar
	if barIndex%1000 == 0 {
		log.Printf("  📍 Tracking Position: %s at %s, TP: %s, SL: %s",
			s.ActivePosition.TradeType, s.ActivePosition.EntryPrice.StringFixed(2),
			s.ActivePosition.TakeProfit.StringFixed(2), s.ActivePosition.StopLoss.StringFixed(2))
	}

	// Check timeout
	if s.MaxHoldingBars > 0 && bar.Timestamp >= s.ActivePosition.TimeToLive {
		log.Printf("⏰ Position TIMEOUT at bar %d", barIndex)
		s.closePosition(bar, barIndex, "Timeout")
		return
	}

	// For next-open entries, optionally suppress exits on the same bar as entry
	if s.EntryMode == EntryModeNextBarOpen && !s.IntraExitsOnEntry && barIndex == s.ActivePosition.EntryBarIndex {
		return
	}

	// STEP 4: Analyze price movement within this bar to check if TP/SL is hit
	// This uses first-touch resolution based on intrabar policy (EXACT_TRADES, ONE_SECOND_BARS, LINEAR_INTERPOLATION)
	if s.ActivePosition.TradeType == "Long" {
		hitTP, hitSL, exitPrice, reason := s.ResolveFirstTouchLong(bar, s.ActivePosition.TakeProfit, s.ActivePosition.StopLoss)
		if hitTP || hitSL {
			if hitSL {
				msg := fmt.Sprintf("🛑 LONG SL HIT at bar %d! Low: %s <= SL: %s", barIndex, bar.Low.StringFixed(2), s.ActivePosition.StopLoss.StringFixed(2))
				log.Printf(msg)
				if s.EnableExcel {
					s.ExcelRows = append(s.ExcelRows, ExcelRow{DateTime: time.Now().UTC().Format("2006/01/02 15:04:05"), Type: "EVENT", Reason: msg})
				}
			} else {
				msg := fmt.Sprintf("🎯 LONG TP HIT at bar %d! High: %s >= TP: %s", barIndex, bar.High.StringFixed(2), s.ActivePosition.TakeProfit.StringFixed(2))
				log.Printf(msg)
				if s.EnableExcel {
					s.ExcelRows = append(s.ExcelRows, ExcelRow{DateTime: time.Now().UTC().Format("2006/01/02 15:04:05"), Type: "EVENT", Reason: msg})
				}
			}
			s.closePositionAtPrice(bar, barIndex, exitPrice, reason, reason)
		}
	} else {
		hitTP, hitSL, exitPrice, reason := s.ResolveFirstTouchShort(bar, s.ActivePosition.TakeProfit, s.ActivePosition.StopLoss)
		if hitTP || hitSL {
			if hitSL {
				msg := fmt.Sprintf("🛑 SHORT SL HIT at bar %d! High: %s >= SL: %s", barIndex, bar.High.StringFixed(2), s.ActivePosition.StopLoss.StringFixed(2))
				log.Printf(msg)
				if s.EnableExcel {
					s.ExcelRows = append(s.ExcelRows, ExcelRow{DateTime: time.Now().UTC().Format("2006/01/02 15:04:05"), Type: "EVENT", Reason: msg})
				}
			} else {
				msg := fmt.Sprintf("🎯 SHORT TP HIT at bar %d! Low: %s <= TP: %s", barIndex, bar.Low.StringFixed(2), s.ActivePosition.TakeProfit.StringFixed(2))
				log.Printf(msg)
				if s.EnableExcel {
					s.ExcelRows = append(s.ExcelRows, ExcelRow{DateTime: time.Now().UTC().Format("2006/01/02 15:04:05"), Type: "EVENT", Reason: msg})
				}
			}
			s.closePositionAtPrice(bar, barIndex, exitPrice, reason, reason)
		}
	}
}

// checkExit is a legacy alias for trackPriceMovementForTP_SL (kept for compatibility)
func (s *EMAATRStrategy) checkExit(bar Bar, barIndex int) {
	s.trackPriceMovementForTP_SL(bar, barIndex)
}

// closePosition closes position at current bar close
func (s *EMAATRStrategy) closePosition(bar Bar, barIndex int, reason string) {
	s.closePositionAtPrice(bar, barIndex, bar.Close, reason, "None")
}

// closePositionAtPrice closes position at specific price
func (s *EMAATRStrategy) closePositionAtPrice(bar Bar, barIndex int, exitPrice decimal.Decimal, reason, hitTpSl string) {
	if s.ActivePosition == nil {
		return
	}

	// Calculate exit fee
	exitFee := s.calculateFee(s.ActivePosition.Quantity.Mul(exitPrice), false) // Assume taker
	totalFees := s.ActivePosition.EntryFee.Add(exitFee)

	// Calculate PnL
	var pnl decimal.Decimal
	if s.ActivePosition.TradeType == "Long" {
		pnl = exitPrice.Sub(s.ActivePosition.EntryPrice).Mul(s.ActivePosition.Quantity).Sub(totalFees)
	} else {
		pnl = s.ActivePosition.EntryPrice.Sub(exitPrice).Mul(s.ActivePosition.Quantity).Sub(totalFees)
	}

	pnlPct := pnl.Div(s.ActivePosition.SizeUsd).Mul(decimal.NewFromFloat(100.0))

	// Calculate bars held
	barsHeld := 0
	if s.ActivePosition.EntryBarIndex >= 0 && barIndex >= s.ActivePosition.EntryBarIndex {
		barsHeld = barIndex - s.ActivePosition.EntryBarIndex + 1
	}

	// Log exit in verbose mode
	if s.Verbose {
		timestamp := time.UnixMilli(bar.Timestamp).UTC()
		dateTime := timestamp.Format("2006-01-02 15:04:05")
		log.Printf("| %s | %-4s | %-8s | %-8s | %-8s | %-6s | %-12s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | EMA26: %7.2f | EMA100: %7.2f | ATR: %6.2f |",
			dateTime, s.ActivePosition.TradeType, s.ActivePosition.EntryPrice.StringFixed(2),
			exitPrice.StringFixed(2), pnl.StringFixed(2), pnlPct.StringFixed(2), reason,
			s.ActivePosition.TakeProfit.StringFixed(2), s.ActivePosition.StopLoss.StringFixed(2),
			bar.Open.StringFixed(2), bar.High.StringFixed(2), bar.Low.StringFixed(2), bar.Close.StringFixed(2), bar.Volume.StringFixed(2),
			s.EmaFast[barIndex], s.EmaSlow[barIndex], s.Atr[barIndex])
	}

	// Record trade trace
	if s.EnableTraces {
		firstTouch := "none"
		if hitTpSl == "TakeProfit" {
			firstTouch = "TP"
		} else if hitTpSl == "StopLoss" {
			firstTouch = "SL"
		}
		s.TradeTraces = append(s.TradeTraces, TradeTraceEntry{
			TsEntry:        s.ActivePosition.EntryTime,
			TpPrice:        s.ActivePosition.TakeProfit,
			SlPrice:        s.ActivePosition.StopLoss,
			IntrabarPolicy: s.IntrabarPolicy,
			FirstTouch:     firstTouch,
			TsExit:         bar.Timestamp,
			ExitPrice:      exitPrice,
			Fees:           totalFees,
			RealizedPnl:    pnl,
			BarsHeld:       barsHeld,
		})
	}

    // Update TP/SL streaks and max streaks
    if hitTpSl == "TakeProfit" {
        s.TpStreak++
        s.SlStreak = 0
        if s.TpStreak > s.MaxTpStreak { s.MaxTpStreak = s.TpStreak }
    } else if hitTpSl == "StopLoss" {
        s.SlStreak++
        s.TpStreak = 0
        if s.SlStreak > s.MaxSlStreak { s.MaxSlStreak = s.SlStreak }
    } else {
        s.TpStreak = 0
        s.SlStreak = 0
    }

    trade := Trade{
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
		AtrAtEntry: s.ActivePosition.AtrAtEntry,
	}

	s.Trades = append(s.Trades, trade)

	log.Printf("Closed %s at %s, PnL: %s (%s%%)",
		s.ActivePosition.TradeType, exitPrice.String(), pnl.String(), pnlPct.String())

	// Also add a verbose EVENT line to the export so TXT mirrors console logs
    if s.EnableExcel {
        msg := fmt.Sprintf("Closed %s at %s, PnL: %s (%s%%) | TP_Streak=%d SL_Streak=%d", s.ActivePosition.TradeType, exitPrice.String(), pnl.String(), pnlPct.String(), s.TpStreak, s.SlStreak)
        s.ExcelRows = append(s.ExcelRows, ExcelRow{DateTime: time.Now().UTC().Format("2006/01/02 15:04:05"), Type: "EVENT", Reason: msg})
	}

	s.ActivePosition = nil
}

// updateEquity updates equity and drawdown tracking
func (s *EMAATRStrategy) updateEquity() {
	// Calculate current equity from realized PnL
	realizedPnl := decimal.Zero
	for _, trade := range s.Trades {
		realizedPnl = realizedPnl.Add(trade.PnlUsd)
	}

	s.CurrentEquity = decimal.NewFromFloat(10000.0).Add(realizedPnl)

	if s.CurrentEquity.GreaterThan(s.PeakEquity) {
		s.PeakEquity = s.CurrentEquity
	}

	currentDrawdown := s.PeakEquity.Sub(s.CurrentEquity).Div(s.PeakEquity)
	if currentDrawdown.GreaterThan(s.MaxDrawdown) {
		s.MaxDrawdown = currentDrawdown
	}
}

// GenerateSummary calculates trade summary statistics
func (s *EMAATRStrategy) GenerateSummary() TradeSummary {
	if len(s.Trades) == 0 {
		return TradeSummary{}
	}

	var wins, losses int
	var netPnl, grossProfit, grossLoss decimal.Decimal
	var totalHoldingTime int64

	for _, trade := range s.Trades {
		netPnl = netPnl.Add(trade.PnlUsd)

		if trade.PnlUsd.GreaterThan(decimal.Zero) {
			wins++
			grossProfit = grossProfit.Add(trade.PnlUsd)
		} else {
			losses++
			grossLoss = grossLoss.Add(trade.PnlUsd.Abs())
		}

		// Calculate holding time
		entryTime, _ := time.Parse("2006-01-02T15:04:05.000Z", trade.EntryTime)
		exitTime, _ := time.Parse("2006-01-02T15:04:05.000Z", trade.ExitTime)
		holdingTime := exitTime.Sub(entryTime).Milliseconds()
		totalHoldingTime += holdingTime
	}

	totalTrades := len(s.Trades)
	winRate := decimal.NewFromInt(int64(wins)).Div(decimal.NewFromInt(int64(totalTrades))).Mul(decimal.NewFromFloat(100.0))

	var avgWin, avgLoss, expectancy decimal.Decimal
	if wins > 0 {
		avgWin = grossProfit.Div(decimal.NewFromInt(int64(wins)))
	}
	if losses > 0 {
		avgLoss = grossLoss.Div(decimal.NewFromInt(int64(losses)))
	}

	expectancy = winRate.Div(decimal.NewFromFloat(100.0)).Mul(avgWin).Add(
		decimal.NewFromFloat(1.0).Sub(winRate.Div(decimal.NewFromFloat(100.0))).Mul(avgLoss))

	var profitFactor decimal.Decimal
	if grossLoss.GreaterThan(decimal.Zero) {
		profitFactor = grossProfit.Div(grossLoss)
	}

	avgHoldingTimeHours := decimal.NewFromInt(totalHoldingTime).Div(decimal.NewFromInt(int64(totalTrades))).Div(decimal.NewFromFloat(3600000.0))

	return TradeSummary{
		TotalTrades:         totalTrades,
		Wins:                wins,
		Losses:              losses,
		WinRate:             winRate,
		NetPnlUsd:           netPnl,
		AvgWinUsd:           avgWin,
		AvgLossUsd:          avgLoss,
		Expectancy:          expectancy,
		MaxDrawdown:         s.MaxDrawdown.Mul(decimal.NewFromFloat(100.0)),
		ProfitFactor:        profitFactor,
		AvgHoldingTimeHours: avgHoldingTimeHours,
	}
}

// ExportCSV exports trades to CSV file
func (s *EMAATRStrategy) ExportCSV(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"date", "type", "entry_price", "entry_time_utc", "exit_price", "exit_time_utc",
		"exit_reason", "hit_tp_sl", "size_usd", "qty", "fees_usd", "pnl_usd", "pnl_pct", "symbol",
		"tp_price", "sl_price", "bars_held", "atr_at_entry",
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	// Write trades
	for _, trade := range s.Trades {
		record := []string{
			trade.Date,
			trade.Type,
			trade.EntryPrice.String(),
			trade.EntryTime,
			trade.ExitPrice.String(),
			trade.ExitTime,
			trade.ExitReason,
			trade.HitTpSl,
			trade.SizeUsd.String(),
			trade.Qty.String(),
			trade.FeesUsd.String(),
			trade.PnlUsd.String(),
			trade.PnlPct.String(),
			trade.Symbol,
			trade.TpPrice.String(),
			trade.SlPrice.String(),
			fmt.Sprintf("%d", trade.BarsHeld),
			trade.AtrAtEntry.String(),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	// Write summary
	summary := s.GenerateSummary()
	writer.Write([]string{""}) // Empty line
	writer.Write([]string{"# Summary"})
	writer.Write([]string{"total_trades", fmt.Sprintf("%d", summary.TotalTrades)})
	writer.Write([]string{"wins", fmt.Sprintf("%d", summary.Wins)})
	writer.Write([]string{"losses", fmt.Sprintf("%d", summary.Losses)})
	writer.Write([]string{"win_rate", summary.WinRate.String()})
	writer.Write([]string{"net_pnl_usd", summary.NetPnlUsd.String()})
	writer.Write([]string{"avg_win_usd", summary.AvgWinUsd.String()})
	writer.Write([]string{"avg_loss_usd", summary.AvgLossUsd.String()})
	writer.Write([]string{"expectancy", summary.Expectancy.String()})
	writer.Write([]string{"max_drawdown", summary.MaxDrawdown.String()})
	writer.Write([]string{"profit_factor", summary.ProfitFactor.String()})
	writer.Write([]string{"avg_holding_time_hours", summary.AvgHoldingTimeHours.String()})

	return nil
}

// ExportDebugCSV exports debug bars to CSV file
func (s *EMAATRStrategy) ExportDebugCSV(filename string) error {
	if !s.EnableDebug {
		return fmt.Errorf("debug mode not enabled")
	}

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"timestamp_ms", "open", "high", "low", "close",
		"ema26", "ema100", "atr14", "body_pct",
		"long_cond", "short_cond", "reason_blocked",
		"entry_price", "sl_price", "tp_price",
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	// Write debug bars
	for _, db := range s.DebugBars {
		record := []string{
			fmt.Sprintf("%d", db.Timestamp),
			db.Open.String(),
			db.High.String(),
			db.Low.String(),
			db.Close.String(),
			fmt.Sprintf("%.6f", db.Ema26),
			fmt.Sprintf("%.6f", db.Ema100),
			fmt.Sprintf("%.6f", db.Atr14),
			db.BodyPct.String(),
			fmt.Sprintf("%t", db.LongCond),
			fmt.Sprintf("%t", db.ShortCond),
			string(db.ReasonBlocked),
			db.EntryPrice.String(),
			db.SlPrice.String(),
			db.TpPrice.String(),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}

// ExportCandidatesCSV exports signal candidates to CSV file
func (s *EMAATRStrategy) ExportCandidatesCSV(filename string) error {
	if !s.EnableDebug {
		return fmt.Errorf("debug mode not enabled")
	}

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"bar_index", "timestamp_ms", "open", "high", "low", "close",
		"ema26", "ema100", "atr14", "body_pct",
		"trade_type", "reason",
		"entry_price", "sl_price", "tp_price",
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	// Write candidates
	for _, cand := range s.Candidates {
		record := []string{
			fmt.Sprintf("%d", cand.BarIndex),
			fmt.Sprintf("%d", cand.Timestamp),
			cand.Bar.Open.String(),
			cand.Bar.High.String(),
			cand.Bar.Low.String(),
			cand.Bar.Close.String(),
			fmt.Sprintf("%.6f", cand.EmaFast),
			fmt.Sprintf("%.6f", cand.EmaSlow),
			fmt.Sprintf("%.6f", cand.Atr),
			cand.BodyPct.String(),
			cand.TradeType,
			string(cand.Reason),
			cand.EntryPrice.String(),
			cand.SlPrice.String(),
			cand.TpPrice.String(),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}

// PrintSummary prints trade summary to console
func (s *EMAATRStrategy) PrintSummary() {
	summary := s.GenerateSummary()

	fmt.Println("\n=== TRADE SUMMARY ===")
	fmt.Printf("Total Trades: %d\n", summary.TotalTrades)
	fmt.Printf("Wins: %d\n", summary.Wins)
	fmt.Printf("Losses: %d\n", summary.Losses)
	fmt.Printf("Win Rate: %s%%\n", summary.WinRate.String())
	fmt.Printf("Net PnL: $%s\n", summary.NetPnlUsd.String())
	fmt.Printf("Average Win: $%s\n", summary.AvgWinUsd.String())
	fmt.Printf("Average Loss: $%s\n", summary.AvgLossUsd.String())
	fmt.Printf("Expectancy: $%s\n", summary.Expectancy.String())
	fmt.Printf("Max Drawdown: %s%%\n", summary.MaxDrawdown.String())
	fmt.Printf("Profit Factor: %s\n", summary.ProfitFactor.String())
	fmt.Printf("Avg Holding Time: %s hours\n", summary.AvgHoldingTimeHours.String())
    fmt.Printf("Max TP Streak: %d\n", s.MaxTpStreak)
    fmt.Printf("Max SL Streak: %d\n", s.MaxSlStreak)
	fmt.Println("===================")
}

// ExportIndicatorTraceCSV exports indicator trace to CSV file
func (s *EMAATRStrategy) ExportIndicatorTraceCSV(filename string) error {
	if !s.EnableTraces {
		return fmt.Errorf("traces not enabled")
	}

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"timestamp_ms", "open", "high", "low", "close",
		"ema_fast", "ema_slow", "atr", "state",
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	// Write indicator traces
	for _, trace := range s.IndicatorTraces {
		record := []string{
			fmt.Sprintf("%d", trace.Timestamp),
			trace.Open.String(),
			trace.High.String(),
			trace.Low.String(),
			trace.Close.String(),
			fmt.Sprintf("%.6f", trace.EmaFast),
			fmt.Sprintf("%.6f", trace.EmaSlow),
			fmt.Sprintf("%.6f", trace.Atr),
			trace.State,
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}

// ExportSignalTraceCSV exports signal trace to CSV file
func (s *EMAATRStrategy) ExportSignalTraceCSV(filename string) error {
	if !s.EnableTraces {
		return fmt.Errorf("traces not enabled")
	}

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"timestamp_ms", "reason_code", "ema_cross", "body_pct", "atr",
		"entry_side", "entry_price_pre_filter", "entry_price_post_filter",
		"qty_pre_filter", "qty_post_filter", "slippage", "fees",
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	// Write signal traces
	for _, trace := range s.SignalTraces {
		record := []string{
			fmt.Sprintf("%d", trace.Timestamp),
			trace.ReasonCode,
			fmt.Sprintf("%t", trace.EmaCross),
			trace.BodyPct.String(),
			fmt.Sprintf("%.6f", trace.Atr),
			trace.EntrySide,
			trace.EntryPricePreFilter.String(),
			trace.EntryPricePostFilter.String(),
			trace.QtyPreFilter.String(),
			trace.QtyPostFilter.String(),
			trace.Slippage.String(),
			trace.Fees.String(),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}

// ExportExcel exports the detailed table to Excel format (CSV)
func (s *EMAATRStrategy) ExportExcel(filename string) error {
	if len(s.ExcelRows) == 0 {
		return fmt.Errorf("no data to export")
	}

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create Excel file: %v", err)
	}
	defer file.Close()

	// Write CSV header
	_, err = file.WriteString("Date & Time,Type,Entry,Exit,PnL,PnL%,Reason,TP,SL,Open,High,Low,Close,Volume,EMA26,EMA100,ATR,Body%,Conditions\n")
	if err != nil {
		return fmt.Errorf("failed to write header: %v", err)
	}

	// Write data rows
	for _, row := range s.ExcelRows {
		_, err = file.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%.2f,%.2f,%.2f,%s,%s\n",
			row.DateTime, row.Type, row.Entry, row.Exit, row.PnL, row.PnLPct, row.Reason, row.TP, row.SL,
			row.Open, row.High, row.Low, row.Close, row.Volume, row.EMA26, row.EMA100, row.ATR, row.BodyPct, row.Conditions))
		if err != nil {
			return fmt.Errorf("failed to write row: %v", err)
		}
	}

	log.Printf("📊 Excel export completed: %s (%d rows)", filename, len(s.ExcelRows))
	return nil
}

// ExportTableTXT exports the detailed table to a formatted TXT file
func (s *EMAATRStrategy) ExportTableTXT(filename string) error {
	if len(s.ExcelRows) == 0 {
		return fmt.Errorf("no data to export")
	}

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create TXT file: %v", err)
	}
	defer file.Close()

	// Write table header
	_, err = file.WriteString("┌─────────────────────┬──────┬──────────┬──────────┬──────────┬────────┬──────────────┬──────────┬──────────┬────────┬────────┬────────┬────────┬────────┬─────────────┬─────────────┬──────────┬────────┐\n")
	if err != nil {
		return fmt.Errorf("failed to write header line 1: %v", err)
	}
	_, err = file.WriteString("│ Date & Time         │ Type │ Entry    │ Exit     │ PnL      │ PnL%   │ Reason       │ TP       │ SL       │ Open   │ High   │ Low    │ Close  │ Volume │ EMA26       │ EMA100      │ ATR       │ Body%  │\n")
	if err != nil {
		return fmt.Errorf("failed to write header line 2: %v", err)
	}
	_, err = file.WriteString("├─────────────────────┼──────┼──────────┼──────────┼──────────┼────────┼──────────────┼──────────┼──────────┼────────┼────────┼────────┼────────┼────────┼─────────────┼─────────────┼──────────┼────────┤\n")
	if err != nil {
		return fmt.Errorf("failed to write header line 3: %v", err)
	}

	// Write data rows
	for _, row := range s.ExcelRows {
		// If this is an EVENT row, render the console-style message verbatim
		if row.Type == "EVENT" {
			if _, err = file.WriteString(fmt.Sprintf("%s %s\n\n", row.DateTime, row.Reason)); err != nil {
				return fmt.Errorf("failed to write event row: %v", err)
			}
			continue
		}
		// If this is a SCAN row, keep numeric columns visually empty and show conditions in Reason
		if row.Type == "SCAN" {
			_, err = file.WriteString(fmt.Sprintf("| %-19s | %-4s | %-8s | %-8s | %-8s | %-6s | %-12s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | %13s | %13s | %10s | %6s |\n",
				row.DateTime, row.Type, row.Entry, row.Exit, row.PnL, row.PnLPct, row.Reason, row.TP, row.SL,
				row.Open, row.High, row.Low, row.Close, row.Volume, "-", "-", "-", ""))
		} else {
			_, err = file.WriteString(fmt.Sprintf("| %-19s | %-4s | %-8s | %-8s | %-8s | %-6s | %-12s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | %13.2f | %13.2f | %10.2f | %6s |\n",
				row.DateTime, row.Type, row.Entry, row.Exit, row.PnL, row.PnLPct, row.Reason, row.TP, row.SL,
				row.Open, row.High, row.Low, row.Close, row.Volume, row.EMA26, row.EMA100, row.ATR, row.BodyPct))
		}
		if err != nil {
			return fmt.Errorf("failed to write row: %v", err)
		}
	}

	// Write table footer
	_, err = file.WriteString("└─────────────────────┴──────┴──────────┴──────────┴──────────┴────────┴──────────────┴──────────┴──────────┴────────┴────────┴────────┴────────┴────────┴─────────────┴─────────────┴──────────┴────────┘\n")
	if err != nil {
		return fmt.Errorf("failed to write footer: %v", err)
	}

	// Append summary at the end of the TXT for quick reference
	summary := s.GenerateSummary()
	_, err = file.WriteString("\n=== SUMMARY ===\n")
	if err != nil {
		return fmt.Errorf("failed to write summary header: %v", err)
	}
	_, err = file.WriteString(fmt.Sprintf("Total Trades: %d\n", summary.TotalTrades))
	if err != nil {
		return fmt.Errorf("failed to write summary line: %v", err)
	}
	_, err = file.WriteString(fmt.Sprintf("Wins: %d\n", summary.Wins))
	if err != nil {
		return fmt.Errorf("failed to write summary line: %v", err)
	}
	_, err = file.WriteString(fmt.Sprintf("Losses: %d\n", summary.Losses))
	if err != nil {
		return fmt.Errorf("failed to write summary line: %v", err)
	}
	_, err = file.WriteString(fmt.Sprintf("Win Rate: %s%%\n", summary.WinRate.String()))
	if err != nil {
		return fmt.Errorf("failed to write summary line: %v", err)
	}
	_, err = file.WriteString(fmt.Sprintf("Net PnL: $%s\n", summary.NetPnlUsd.String()))
	if err != nil {
		return fmt.Errorf("failed to write summary line: %v", err)
	}
	_, err = file.WriteString(fmt.Sprintf("Average Win: $%s\n", summary.AvgWinUsd.String()))
	if err != nil {
		return fmt.Errorf("failed to write summary line: %v", err)
	}
	_, err = file.WriteString(fmt.Sprintf("Average Loss: $%s\n", summary.AvgLossUsd.String()))
	if err != nil {
		return fmt.Errorf("failed to write summary line: %v", err)
	}
	_, err = file.WriteString(fmt.Sprintf("Expectancy: $%s\n", summary.Expectancy.String()))
	if err != nil {
		return fmt.Errorf("failed to write summary line: %v", err)
	}
	_, err = file.WriteString(fmt.Sprintf("Max Drawdown: %s%%\n", summary.MaxDrawdown.String()))
	if err != nil {
		return fmt.Errorf("failed to write summary line: %v", err)
	}
	_, err = file.WriteString(fmt.Sprintf("Profit Factor: %s\n", summary.ProfitFactor.String()))
	if err != nil {
		return fmt.Errorf("failed to write summary line: %v", err)
	}
	_, err = file.WriteString(fmt.Sprintf("Avg Holding Time: %s hours\n", summary.AvgHoldingTimeHours.String()))
	if err != nil {
		return fmt.Errorf("failed to write summary line: %v", err)
	}

	// Append max streaks (TP/SL)
	_, err = file.WriteString(fmt.Sprintf("Max TP Streak: %d\n", s.MaxTpStreak))
	if err != nil {
		return fmt.Errorf("failed to write summary line: %v", err)
	}
	_, err = file.WriteString(fmt.Sprintf("Max SL Streak: %d\n", s.MaxSlStreak))
	if err != nil {
		return fmt.Errorf("failed to write summary line: %v", err)
	}

	log.Printf("📄 Table TXT export completed: %s (%d rows)", filename, len(s.ExcelRows))
	return nil
}

// ExportTradeTraceCSV exports trade trace to CSV file
func (s *EMAATRStrategy) ExportTradeTraceCSV(filename string) error {
	if !s.EnableTraces {
		return fmt.Errorf("traces not enabled")
	}

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"ts_entry", "tp_price", "sl_price", "intrabar_policy", "first_touch",
		"ts_exit", "exit_price", "fees", "realized_pnl", "bars_held",
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	// Write trade traces
	for _, trace := range s.TradeTraces {
		record := []string{
			fmt.Sprintf("%d", trace.TsEntry),
			trace.TpPrice.String(),
			trace.SlPrice.String(),
			trace.IntrabarPolicy,
			trace.FirstTouch,
			fmt.Sprintf("%d", trace.TsExit),
			trace.ExitPrice.String(),
			trace.Fees.String(),
			trace.RealizedPnl.String(),
			fmt.Sprintf("%d", trace.BarsHeld),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}
