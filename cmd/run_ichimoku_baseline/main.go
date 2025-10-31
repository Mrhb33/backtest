package main

import (
    "bufio"
    "flag"
    "fmt"
    "io"
    "os"
    "strings"

    "backtest-root-installer/go-services/strategies"
    "github.com/shopspring/decimal"
)

func main() {
    csvPath := flag.String("csv", "", "Path to local CSV (timestamp,open,high,low,close,volume)")
    from := flag.String("from", "2020-10-30 00:00:00", "Start UTC (YYYY-MM-DD HH:MM:SS)")
    to := flag.String("to", "2025-10-30 00:00:00", "End UTC (YYYY-MM-DD HH:MM:SS)")
    verbose := flag.Bool("verbose", false, "Enable verbose table output")
    nextOpen := flag.Bool("next_open", true, "Enter next bar open (true) or signal close (false)")
    initEq := flag.Float64("init_equity", 1000.0, "Initial equity amount in USD")
    riskFrac := flag.Float64("risk_fraction", 1.0, "Fraction of equity to allocate per trade (0..1)")
    tpPct := flag.Float64("tp_pct", 0.019, "Take profit percent (e.g., 0.019 = +1.9%)")
    slPct := flag.Float64("sl_pct", 0.009, "Stop loss percent (e.g., 0.009 = -0.9%)")
    flag.Parse()

    if csvPath == nil || *csvPath == "" { panic("-csv is required") }

    strat := strategies.NewIchimokuBaselineStrategy()
    strat.EnableExcel = true
    if verbose != nil && *verbose { strat.Verbose = true }
    if nextOpen != nil && *nextOpen { strat.EntryMode = strategies.EntryModeNextBarOpen } else { strat.EntryMode = strategies.EntryModeSignalClose }
    // Apply runtime parameters
    if initEq != nil { strat.InitialEquity = decimal.NewFromFloat(*initEq); strat.CurrentEquity = strat.InitialEquity; strat.PeakEquity = strat.InitialEquity }
    if riskFrac != nil { strat.RiskFraction = decimal.NewFromFloat(*riskFrac) }
    if tpPct != nil { strat.TpPct = decimal.NewFromFloat(*tpPct) }
    if slPct != nil { strat.SlPct = decimal.NewFromFloat(*slPct) }

    // Pre-clean quotes/BOM into .clean.csv like other runners
    clean := *csvPath + ".clean.csv"
    inF, err := os.Open(*csvPath)
    if err != nil { panic(err) }
    defer inF.Close()
    outF, err := os.Create(clean)
    if err != nil { panic(err) }
    w := bufio.NewWriter(outF)
    r := bufio.NewReader(inF)
    for {
        line, err := r.ReadString('\n')
        if line != "" {
            line = strings.TrimPrefix(line, "\ufeff")
            line = strings.ReplaceAll(line, "\"", "")
            w.WriteString(line)
        }
        if err == io.EOF { break }
        if err != nil { break }
    }
    _ = w.Flush()
    _ = outF.Close()

    if err := strat.LoadCSV(clean); err != nil { panic(err) }
    if err := strat.CalculateIndicators(); err != nil { panic(err) }
    if err := strat.Run(); err != nil { panic(err) }

    // Export TXT table
    _ = exportIchimokuTable(strat, "./detailed_table.txt")

    // Print small summary
    total, wins, losses, _ := computeSummary(strat)
    winRate := 0.0
    if total > 0 { winRate = float64(wins)/float64(total)*100.0 }
    finalEq := strat.CurrentEquity
    retPct := decimal.Zero
    if strat.InitialEquity.GreaterThan(decimal.Zero) {
        retPct = finalEq.Div(strat.InitialEquity).Sub(decimal.NewFromFloat(1)).Mul(decimal.NewFromFloat(100))
    }
    fmt.Printf("Ichimoku %s→%s | Trades:%d Wins:%d Losses:%d WinRate:%.2f%% | FinalEq:$%s | TotalReturn:%s%%\n", *from, *to, total, wins, losses, winRate, finalEq.StringFixed(2), retPct.StringFixed(2))
}

func exportIchimokuTable(s *strategies.IchimokuBaselineStrategy, name string) error {
    if len(s.ExcelRows) == 0 { return nil }
    f, err := os.Create(name)
    if err != nil { return err }
    defer f.Close()
    _, err = f.WriteString("┌─────────────────────┬──────────┬──────────┬──────────────┬──────────┬──────────┬────────┬────────────────────────────────────────────┐\n")
    if err != nil { return err }
    _, err = f.WriteString("│ Date & Time         │ Open     │ Close    │ Kijun        │ TP       │ SL       │ Type   │ Reason | Conditions                     │\n")
    if err != nil { return err }
    _, err = f.WriteString("├─────────────────────┼──────────┼──────────┼──────────────┼──────────┼──────────┼────────┼────────────────────────────────────────────┤\n")
    if err != nil { return err }
    for _, row := range s.ExcelRows {
        if row.Type == "EVENT" {
            _, err = f.WriteString(fmt.Sprintf("%s %s\n\n", row.DateTime, row.Reason))
            if err != nil { return err }
            continue
        }
        kijun := fmt.Sprintf("%8.2f", row.EMA26)
        tp := row.TP; sl := row.SL
        if tp == "" { tp = "-" }
        if sl == "" { sl = "-" }
        cond := row.Conditions
        line := fmt.Sprintf("| %-19s | %8s | %8s | %12s | %8s | %8s | %-6s | %-20s | %-28s |\n",
            row.DateTime, row.Open, row.Close, kijun, tp, sl, row.Type, row.Reason, cond)
        if _, err = f.WriteString(line); err != nil { return err }
    }
    _, err = f.WriteString("└─────────────────────┴──────────┴──────────┴──────────────┴──────────┴──────────┴────────┴────────────────────────────────────────────┘\n")
    if err != nil { return err }

    // Summary
    total, wins, losses, net := computeSummary(s)
    winRate := 0.0
    if total > 0 { winRate = float64(wins)/float64(total)*100.0 }
    // Additional lines: per-trade amount and final amount (equity)
    tradeAmount := "1000.00"
    finalAmount := s.CurrentEquity.StringFixed(2)
    _, err = f.WriteString(fmt.Sprintf("\n=== SUMMARY ===\nTrades: %d\nWins: %d\nLosses: %d\nWinRate: %.2f%%\nNetPnL: $%s\nPer-Trade Amount: $%s\nFinal Amount: $%s\nMax TP Streak: %d\nMax SL Streak: %d\n", total, wins, losses, winRate, net, tradeAmount, finalAmount, s.MaxTpStreak, s.MaxSlStreak))
    return err
}

func computeSummary(s *strategies.IchimokuBaselineStrategy) (total, wins, losses int, net string) {
    total = len(s.Trades)
    sum := decimal.Zero
    for _, t := range s.Trades {
        if t.PnlUsd.GreaterThan(decimal.Zero) { wins++ } else if t.PnlUsd.LessThan(decimal.Zero) { losses++ }
        sum = sum.Add(t.PnlUsd)
    }
    net = sum.String()
    return
}


