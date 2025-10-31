//! Strategy Runner - Executable for running EMA/ATR strategy
//!
//! This is a standalone executable that can run the EMA/ATR strategy
//! with CSV data input and generate trade results.

package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"backtest-root-installer/go-services/strategies"

	"github.com/shopspring/decimal"
)

func main() {
	// Command line flags
	var (
		csvFile        = flag.String("csv", "", "Path to CSV file with OHLCV data")
		symbol         = flag.String("symbol", "BTCUSDT", "Symbol to trade")
		risk           = flag.Float64("risk", 1000.0, "Risk amount in USD")
		output         = flag.String("output", "trades.csv", "Output CSV file for trades")
		logFile        = flag.String("log-file", "/app/data/strategy.log", "Log file to write strategy-screen output")
		emaFast        = flag.Int("ema-fast", 26, "EMA fast period")
		emaSlow        = flag.Int("ema-slow", 100, "EMA slow period")
		atrPeriod      = flag.Int("atr-period", 14, "ATR period")
		lastDays       = flag.Int("last-days", 0, "If >0, run only the last N days of data (keeps warmup from earlier bars)")
		slMult         = flag.Float64("sl-mult", 2.5, "Stop loss multiplier (ATR * slMult)")
		tpMult         = flag.Float64("tp-mult", 1.8, "Take profit multiplier (TP = SL * tpMult)")
		warmup         = flag.Int("warmup", 300, "Warmup bars for indicators (recommend ≥3× slowest lookback)")
		enableDebug    = flag.Bool("debug", false, "Enable debug mode (exports debug.csv and candidates.csv)")
		enableTraces   = flag.Bool("enable-traces", false, "Enable trace exports (indicator_trace.csv, signal_trace.csv, trade_trace.csv)")
		tracePrefix    = flag.String("trace-prefix", "", "Prefix for trace files (e.g., 'indicator_trace.csv' -> 'prefix_indicator_trace.csv')")
		verbose        = flag.Bool("verbose", false, "Enable verbose logging (shows every candle's calculations)")
		exportExcel    = flag.Bool("export-excel", false, "Export detailed table to Excel file (CSV format)")
		exportTXT      = flag.Bool("export-txt", false, "Export detailed table to formatted TXT file")
		entryMode      = flag.String("entry-mode", "next-open", "Entry mode: 'next-open' or 'signal-close'")
		maxHolding     = flag.Int("max-holding-bars", 72, "Max bars to hold position before timeout")
		firstTouch     = flag.String("first-touch", "chart", "First-touch policy: 'sl-first', 'tp-first', or 'chart'")
		sizingMode     = flag.String("sizing", "notional", "Sizing mode: 'notional' or 'risk'")
		atrTiming      = flag.String("atr-timing", "signal", "ATR timing: 'signal' or 'entry'")
		intraExits     = flag.Bool("intra-exits", false, "Allow exits on same bar as entry (for next-open mode)")
		slippageMode   = flag.String("slippage-mode", "TRADE_SWEEP", "Slippage mode: 'NONE', 'TRADE_SWEEP', or 'SYNTHETIC_BOOK'")
		intrabarPolicy = flag.String("intrabar-policy", "LINEAR_INTERPOLATION", "Intrabar policy: 'EXACT_TRADES', 'ONE_SECOND_BARS', or 'LINEAR_INTERPOLATION'")
		makerFee       = flag.Float64("maker-fee", 0.0001, "Maker fee rate (e.g., 0.0001 = 0.01%)")
		takerFee       = flag.Float64("taker-fee", 0.001, "Taker fee rate (e.g., 0.001 = 0.1%)")
		tickSize       = flag.Float64("tick-size", 0.01, "Tick size (minimum price increment)")
		lotSize        = flag.Float64("lot-size", 0.00001, "Lot size (minimum quantity increment)")
		minNotional    = flag.Float64("min-notional", 10.0, "Minimum order value")
	)
	flag.Parse()

	// Setup dual logging (stdout + file) for strategy-screen
	if err := os.MkdirAll(filepath.Dir(*logFile), 0o755); err == nil {
		f, err := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err == nil {
			mw := io.MultiWriter(os.Stdout, f)
			log.SetOutput(mw)
			// Keep default log flags
		}
	}

	// Validate required parameters
	if *csvFile == "" {
		fmt.Println("Error: -csv flag is required")
		flag.Usage()
		os.Exit(1)
	}

	// Check if CSV file exists
	if _, err := os.Stat(*csvFile); os.IsNotExist(err) {
		log.Fatalf("CSV file does not exist: %s", *csvFile)
	}

	// Create strategy instance
	strategy := strategies.NewEMAATRStrategy()

	// Configure strategy parameters
	strategy.EmaFastPeriod = *emaFast
	strategy.EmaSlowPeriod = *emaSlow
	strategy.AtrPeriod = *atrPeriod
	strategy.SlMultiplier = decimal.NewFromFloat(*slMult)
	strategy.AtpMultiplier = decimal.NewFromFloat(*tpMult)
	strategy.RiskAmount = decimal.NewFromFloat(*risk)
	strategy.WarmupBars = *warmup
	strategy.MaxHoldingBars = *maxHolding
	strategy.EnableDebug = *enableDebug

	// Set entry mode
	if *entryMode == "signal-close" {
		strategy.EntryMode = strategies.EntryModeSignalClose
	} else {
		strategy.EntryMode = strategies.EntryModeNextBarOpen
	}

	// Set first-touch policy
	switch *firstTouch {
	case "sl-first":
		strategy.FirstTouchPolicy = strategies.FirstTouchPolicySLFirst
	case "tp-first":
		strategy.FirstTouchPolicy = strategies.FirstTouchPolicyTPFirst
	default:
		strategy.FirstTouchPolicy = strategies.FirstTouchPolicyChart
	}

	// Set sizing mode
	if *sizingMode == "risk" {
		strategy.SizingMode = strategies.SizingModeRisk
	} else {
		strategy.SizingMode = strategies.SizingModeNotional
	}

	// Set ATR timing and other options
	strategy.ATRTiming = *atrTiming
	strategy.IntraExitsOnEntry = *intraExits
	strategy.EnableTraces = *enableTraces
	strategy.Verbose = *verbose
	strategy.EnableExcel = *exportExcel || *exportTXT // Enable data collection if either export is requested
	strategy.IntrabarPolicy = *intrabarPolicy
	strategy.SlippageMode = *slippageMode

	// Configure exchange rules
	strategy.ExchangeRules.TickSize = decimal.NewFromFloat(*tickSize)
	strategy.ExchangeRules.LotSize = decimal.NewFromFloat(*lotSize)
	strategy.ExchangeRules.MinNotional = decimal.NewFromFloat(*minNotional)
	strategy.ExchangeRules.MakerFee = decimal.NewFromFloat(*makerFee)
	strategy.ExchangeRules.TakerFee = decimal.NewFromFloat(*takerFee)

	log.Printf("Starting EMA/ATR Strategy for %s", *symbol)
	log.Printf("Risk Amount: $%.2f", *risk)
	log.Printf("EMA Fast: %d, EMA Slow: %d", *emaFast, *emaSlow)
	if *verbose {
		log.Printf("🔍 VERBOSE MODE ENABLED: Will show every candle's calculations")
	} else {
		log.Printf("📊 SUMMARY MODE: Will show progress every 1000th bar (use --verbose for detailed logging)")
	}
	log.Printf("ATR Period: %d", *atrPeriod)
	log.Printf("Body pct (open-relative, INCLUSIVE): LONG %.3f%%-%.3f%%, SHORT %.3f%%-%.3f%%",
		strategy.BodyPctMinLong.Mul(decimal.NewFromFloat(100)).InexactFloat64(),
		strategy.BodyPctMaxLong.Mul(decimal.NewFromFloat(100)).InexactFloat64(),
		strategy.BodyPctMinShort.Mul(decimal.NewFromFloat(100)).InexactFloat64(),
		strategy.BodyPctMaxShort.Mul(decimal.NewFromFloat(100)).InexactFloat64())
	log.Printf("SL Multiplier: %.1f, TP Multiplier: %.1f (TP = SL * %.1f)", *slMult, *tpMult, *tpMult)
	log.Printf("Entry Mode: %s", *entryMode)
	log.Printf("First Touch Policy: %s", *firstTouch)
	log.Printf("Sizing Mode: %s", *sizingMode)
	log.Printf("ATR Timing: %s", *atrTiming)
	log.Printf("Intra Exits: %t", *intraExits)
	log.Printf("Warmup Bars: %d", *warmup)
	log.Printf("Max Holding Bars: %d", *maxHolding)
	log.Printf("Slippage Mode: %s", *slippageMode)
	log.Printf("Intrabar Policy: %s", *intrabarPolicy)
	log.Printf("Exchange Rules: tick=%.6f, lot=%.8f, min_notional=%.2f, maker_fee=%.4f, taker_fee=%.4f",
		*tickSize, *lotSize, *minNotional, *makerFee, *takerFee)
	log.Printf("Traces Enabled: %t", *enableTraces)
	log.Printf("")

	// Load CSV data
	log.Printf("Loading data from %s...", *csvFile)
	if err := strategy.LoadCSV(*csvFile); err != nil {
		log.Fatalf("Failed to load CSV: %v", err)
	}
	log.Printf("Loaded %d bars", len(strategy.Bars))

	// If last-days requested, slice to the last N days before indicator calculation
	if *lastDays > 0 && len(strategy.Bars) > 0 {
		cutoff := strategy.Bars[len(strategy.Bars)-1].Timestamp - int64(*lastDays)*24*60*60*1000
		// Find the index where selected period starts
		selIdx := 0
		for i := range strategy.Bars {
			if strategy.Bars[i].Timestamp >= cutoff {
				selIdx = i
				break
			}
		}
		if selIdx > 0 {
			// Include warmup bars before the selected period to preserve rolling indicators
			preIdx := selIdx - strategy.WarmupBars
			if preIdx < 0 {
				preIdx = 0
			}
			log.Printf("Selecting last %d days starting at %s (index %d of %d)", *lastDays,
				time.UnixMilli(strategy.Bars[selIdx].Timestamp).UTC().Format("2006-01-02 15:04:05"), selIdx, len(strategy.Bars))
			// Slice bars to selected period + warmup history
			strategy.Bars = strategy.Bars[preIdx:]
			log.Printf("Sliced to %d bars for processing (warmup: %d bars, selected: %d bars)",
				len(strategy.Bars), strategy.WarmupBars, len(strategy.Bars)-strategy.WarmupBars)
		}
	}

	// Run strategy
	log.Printf("Running strategy...")
	if err := strategy.Run(); err != nil {
		log.Fatalf("Strategy execution failed: %v", err)
	}

	// Print results
	fmt.Printf("Strategy completed. Generated %d trades\n", len(strategy.Trades))
	strategy.PrintSummary()

	// Export to CSV
	if err := strategy.ExportCSV(*output); err != nil {
		log.Fatalf("Failed to export CSV: %v", err)
	}
	fmt.Printf("Trades exported to %s\n", *output)

	// Export Excel file if enabled
	if *exportExcel {
		excelFile := strings.Replace(*output, ".csv", "_detailed_table.csv", 1)
		if err := strategy.ExportExcel(excelFile); err != nil {
			log.Fatalf("Failed to export Excel: %v", err)
		}
		fmt.Printf("Detailed table exported to %s\n", excelFile)
	}

	// Export TXT file if enabled
	if *exportTXT {
		txtFile := strings.Replace(*output, ".csv", "_detailed_table.txt", 1)
		if err := strategy.ExportTableTXT(txtFile); err != nil {
			log.Fatalf("Failed to export TXT: %v", err)
		}
		// Print file path with download instructions
		fmt.Printf("\n📄 Detailed table exported to: %s\n", txtFile)
		fmt.Printf("📂 File path: %s\n", txtFile)
		containerName := "backtest-go-services-1" // Adjust if your container name differs
		fmt.Printf("💡 To download from Docker:\n")
		fmt.Printf("   docker cp %s:%s ./detailed_table.txt\n\n", containerName, txtFile)
	}

	// Export debug files if enabled
	if *enableDebug {
		debugFile := strings.TrimSuffix(*output, ".csv") + "_debug.csv"
		candidatesFile := strings.TrimSuffix(*output, ".csv") + "_candidates.csv"
		if err := strategy.ExportDebugCSV(debugFile); err != nil {
			log.Printf("Warning: Failed to export debug CSV: %v", err)
		} else {
			fmt.Printf("Debug bars exported to %s\n", debugFile)
		}
		if err := strategy.ExportCandidatesCSV(candidatesFile); err != nil {
			log.Printf("Warning: Failed to export candidates CSV: %v", err)
		} else {
			fmt.Printf("Candidates exported to %s\n", candidatesFile)
		}
	}

	// Export trace files if enabled (manual backtest validation)
	if *enableTraces {
		prefix := *tracePrefix
		if prefix != "" && !strings.HasSuffix(prefix, "_") {
			prefix = prefix + "_"
		}

		indicatorFile := prefix + "indicator_trace.csv"
		signalFile := prefix + "signal_trace.csv"
		tradeFile := prefix + "trade_trace.csv"

		if err := strategy.ExportIndicatorTraceCSV(indicatorFile); err != nil {
			log.Printf("Warning: Failed to export indicator trace: %v", err)
		} else {
			fmt.Printf("Indicator trace exported to %s\n", indicatorFile)
		}

		if err := strategy.ExportSignalTraceCSV(signalFile); err != nil {
			log.Printf("Warning: Failed to export signal trace: %v", err)
		} else {
			fmt.Printf("Signal trace exported to %s\n", signalFile)
		}

		if err := strategy.ExportTradeTraceCSV(tradeFile); err != nil {
			log.Printf("Warning: Failed to export trade trace: %v", err)
		} else {
			fmt.Printf("Trade trace exported to %s\n", tradeFile)
		}

		fmt.Printf("\n📊 Manual Backtest Validation Files:\n")
		fmt.Printf("  • %s - Every candle: OHLC, EMA26/100, ATR, state\n", indicatorFile)
		fmt.Printf("  • %s - Decision points: reason, body%%, ATR, pre/post filter prices\n", signalFile)
		fmt.Printf("  • %s - Entry→Exit: TP/SL, first_touch, exit price, PnL\n", tradeFile)
		fmt.Printf("  • %s - Final results: trades with UTC times, fees, PnL%%\n", *output)
		fmt.Printf("\n🔍 Validation checklist:\n")
		fmt.Printf("  1. Open indicator_trace.csv; verify EMA26/100 and ATR against TradingView\n")
		fmt.Printf("  2. Open signal_trace.csv; confirm ema_cross=true and body%% within range\n")
		fmt.Printf("  3. Open trade_trace.csv; verify first_touch resolution matches chart order\n")
		fmt.Printf("  4. Open trades.csv; check exit times match TP/SL hit bars\n")
	}

	// Print sample trades
	if len(strategy.Trades) > 0 {
		fmt.Println("\nSample trades:")
		fmt.Println("Date       | Type  | Entry    | Exit     | PnL      | PnL%   | Reason")
		fmt.Println("-----------|-------|----------|----------|----------|--------|--------")

		// Show first 5 trades
		limit := 5
		if len(strategy.Trades) < limit {
			limit = len(strategy.Trades)
		}

		for i := 0; i < limit; i++ {
			trade := strategy.Trades[i]
			fmt.Printf("%-10s | %-5s | %-8s | %-8s | %-8s | %-6s | %s\n",
				trade.Date,
				trade.Type,
				trade.EntryPrice.StringFixed(2),
				trade.ExitPrice.StringFixed(2),
				trade.PnlUsd.StringFixed(2),
				trade.PnlPct.StringFixed(2),
				trade.ExitReason,
			)
		}

		if len(strategy.Trades) > 5 {
			fmt.Printf("... and %d more trades\n", len(strategy.Trades)-5)
		}
	}
}
