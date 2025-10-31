package main

import (
    "bufio"
    "encoding/csv"
    "flag"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "path/filepath"
    "strings"

    "backtest-root-installer/go-services/strategies"

    "golang.org/x/text/encoding/unicode"
    "golang.org/x/text/transform"
    "github.com/shopspring/decimal"
)

func main() {
    chURL := flag.String("ch-url", "http://localhost:18123", "ClickHouse HTTP URL")
    db := flag.String("db", "backtest", "ClickHouse database")
    table := flag.String("table", "data", "ClickHouse table")
    symbol := flag.String("symbol", "BTCUSDT", "Trading symbol")
    from := flag.String("from", "2020-09-01 00:00:00", "Start UTC (YYYY-MM-DD HH:MM:SS)")
    to := flag.String("to", "2024-10-01 00:00:00", "End UTC (YYYY-MM-DD HH:MM:SS)")
    user := flag.String("ch-user", "backtest", "ClickHouse user")
    pass := flag.String("ch-pass", "backtest123", "ClickHouse password")
    outCSV := flag.String("out", "./donchian_basis_5m.csv", "Temp CSV output path")
    csvPath := flag.String("csv", "", "Path to local CSV; if set, skip ClickHouse download")
    verbose := flag.Bool("verbose", false, "Enable verbose table output to console")
    nextOpen := flag.Bool("next_open", false, "Enter on next bar open (default false: signal close)")
    donLen := flag.Int("don_len", 20, "Donchian length")
    tpPct := flag.Float64("tp_pct", 0.026, "TP percent (e.g., 0.026 = 2.6%)")
    slPct := flag.Float64("sl_pct", 0.01, "SL percent (e.g., 0.01 = 1%)")
    flag.Parse()

    if csvPath != nil && *csvPath != "" {
        *outCSV = *csvPath
    } else {
        q := fmt.Sprintf(`
SELECT
    open_time_ms,
    toString(open),
    toString(high),
    toString(low),
    toString(close),
    toString(volume)
FROM %s.%s
WHERE symbol = '%s'
  AND interval = '5m'
  AND open_time_ms >= toUnixTimestamp64Milli(toDateTime64('%s',3,'UTC'))
  AND open_time_ms <  toUnixTimestamp64Milli(toDateTime64('%s',3,'UTC'))
ORDER BY open_time_ms
FORMAT CSV
`, *db, *table, *symbol, *from, *to)

        endpoint := fmt.Sprintf("%s/?%s", strings.TrimRight(*chURL, "/"), url.Values{
            "query":    {q},
            "user":     {*user},
            "password": {*pass},
        }.Encode())

        if err := os.MkdirAll(filepath.Dir(*outCSV), 0o755); err != nil { panic(err) }
        resp, err := http.Get(endpoint)
        if err != nil { panic(err) }
        defer resp.Body.Close()
        if resp.StatusCode != 200 {
            b, _ := io.ReadAll(resp.Body)
            panic(fmt.Errorf("clickhouse export error %d: %s", resp.StatusCode, string(b)))
        }
        outFile, err := os.Create(*outCSV)
        if err != nil { panic(err) }
        defer outFile.Close()
        w := bufio.NewWriter(outFile)
        w.WriteString("timestamp,open,high,low,close,volume\n")
        if _, err := io.Copy(w, resp.Body); err != nil { panic(err) }
        w.Flush()
    }

    strat := strategies.NewDonchianBasisStrategy()
    strat.CadenceMs = 300000
    strat.DonchianLen = *donLen
    strat.TpPct = decimal.NewFromFloat(*tpPct)
    strat.SlPct = decimal.NewFromFloat(*slPct)
    if nextOpen != nil && *nextOpen {
        strat.EntryMode = strategies.EntryModeNextBarOpen
    } else {
        strat.EntryMode = strategies.EntryModeSignalClose
    }
    // Always enable table building; toggle console verbosity via -verbose
    strat.EnableExcel = true
    if verbose != nil && *verbose { strat.Verbose = true }

    if csvPath != nil && *csvPath != "" {
        clean := *csvPath + ".clean.csv"
        inF, err := os.Open(*csvPath)
        if err != nil { panic(err) }
        defer inF.Close()
        outF, err := os.Create(clean)
        if err != nil { panic(err) }
        w := bufio.NewWriter(outF)
        var reader io.Reader = inF
        br := bufio.NewReader(inF)
        b1, _ := br.Peek(2)
        if len(b1) >= 2 && ((b1[0] == 0xFF && b1[1] == 0xFE) || (b1[0] == 0xFE && b1[1] == 0xFF)) {
            inF.Seek(0, 0)
            reader = transform.NewReader(inF, unicode.UTF16(unicode.LittleEndian, unicode.ExpectBOM).NewDecoder())
        } else {
            inF.Seek(0, 0)
            reader = br
        }
        scanner := bufio.NewScanner(reader)
        buf := make([]byte, 0, 1024*1024)
        scanner.Buffer(buf, 1024*1024)
        for scanner.Scan() {
            line := scanner.Text()
            if line == "" { continue }
            line = strings.TrimPrefix(line, "\ufeff")
            line = strings.ReplaceAll(line, "\"", "")
            w.WriteString(line)
            w.WriteByte('\n')
        }
        if err := scanner.Err(); err != nil { panic(err) }
        if err := w.Flush(); err != nil { panic(err) }
        outF.Close()
        if err := strat.LoadCSV(clean); err != nil { panic(err) }
    } else {
        if err := strat.LoadCSV(*outCSV); err != nil { panic(err) }
    }

    if err := strat.CalculateIndicators(); err != nil { panic(err) }
    if err := strat.Run(); err != nil { panic(err) }

    // Brief summary
    fmt.Println("=== Donchian-Basis Backtest Summary ===")
    fmt.Printf("Period: %s to %s UTC\n", *from, *to)
    fmt.Printf("Bars: %d\n", len(strat.Bars))
    fmt.Printf("Trades: %d\n", len(strat.Trades))

    // Exports
    _ = exportCSVLikeEMA(strat, "./trades.csv")
    _ = exportDonchianTable(strat, "./detailed_table.txt")

    // Print summary to console as well
    total, wins, losses, net := computeSummary(strat)
    winRate := 0.0
    if total > 0 { winRate = float64(wins) / float64(total) * 100.0 }
    fmt.Printf("Summary → Trades: %d, Wins: %d, Losses: %d, WinRate: %.2f%%, NetPnL: $%s\n", total, wins, losses, winRate, net)
    fmt.Printf("Max TP Streak: %d, Max SL Streak: %d\n", strat.MaxTpStreak, strat.MaxSlStreak)
    fmt.Println("Wrote: ./trades.csv and ./detailed_table.txt")
}

// thin wrappers to reuse existing types without importing extras here
func exportCSVLikeEMA(s *strategies.DonchianBasisStrategy, name string) error {
    // Build a minimal CSV identical to EMA/ATR ExportCSV columns (sans ATR)
    f, err := os.Create(name)
    if err != nil { return err }
    defer f.Close()
    w := csv.NewWriter(f)
    defer w.Flush()
    header := []string{"date","type","entry_price","entry_time_utc","exit_price","exit_time_utc","exit_reason","hit_tp_sl","size_usd","qty","fees_usd","pnl_usd","pnl_pct","symbol","tp_price","sl_price","bars_held","atr_at_entry"}
    if err := w.Write(header); err != nil { return err }
    for _, t := range s.Trades {
        rec := []string{t.Date, t.Type, t.EntryPrice.String(), t.EntryTime, t.ExitPrice.String(), t.ExitTime, t.ExitReason, t.HitTpSl, t.SizeUsd.String(), t.Qty.String(), t.FeesUsd.String(), t.PnlUsd.String(), t.PnlPct.String(), t.Symbol, t.TpPrice.String(), t.SlPrice.String(), fmt.Sprintf("%d", t.BarsHeld), "0"}
        if err := w.Write(rec); err != nil { return err }
    }
    return nil
}

func exportDonchianTable(s *strategies.DonchianBasisStrategy, name string) error {
    if len(s.ExcelRows) == 0 { return nil }
    f, err := os.Create(name)
    if err != nil { return err }
    defer f.Close()
    // Header with requested fields
    _, err = f.WriteString("┌─────────────────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────────┬──────────┬──────────┬──────────┬────────┬──────────────┐\n")
    if err != nil { return err }
    _, err = f.WriteString("│ Date & Time         │ Open     │ High     │ Low      │ Close    │ Volume   │ Donchian B   │ EMA200   │ TP       │ SL       │ Type   │ Reason       │\n")
    if err != nil { return err }
    _, err = f.WriteString("├─────────────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┼──────────┼──────────┼──────────┼────────┼──────────────┤\n")
    if err != nil { return err }
    for _, row := range s.ExcelRows {
        // Map EMA26 to Donchian B and EMA100 to EMA200
        b := fmt.Sprintf("%8.2f", row.EMA26)
        ema := fmt.Sprintf("%8.2f", row.EMA100)
        tp := row.TP
        sl := row.SL
        if tp == "" { tp = "-" }
        if sl == "" { sl = "-" }
        line := fmt.Sprintf("| %-19s | %8s | %8s | %8s | %8s | %8s | %12s | %8s | %8s | %8s | %-6s | %-12s |\n",
            row.DateTime, row.Open, row.High, row.Low, row.Close, row.Volume, b, ema, tp, sl, row.Type, row.Reason)
        if _, err = f.WriteString(line); err != nil { return err }
    }
    _, err = f.WriteString("└─────────────────────┴──────────┴──────────┴──────────┴──────────┴──────────┴──────────────┴──────────┴──────────┴──────────┴────────┴──────────────┘\n")
    if err != nil { return err }

    // Append summary
    total, wins, losses, net := computeSummary(s)
    winRate := 0.0
    if total > 0 { winRate = float64(wins) / float64(total) * 100.0 }
    _, err = f.WriteString(fmt.Sprintf("\n=== SUMMARY ===\nTrades: %d\nWins: %d\nLosses: %d\nWinRate: %.2f%%\nNetPnL: $%s\nMax TP Streak: %d\nMax SL Streak: %d\n", total, wins, losses, winRate, net, s.MaxTpStreak, s.MaxSlStreak))
    return err
}

func computeSummary(s *strategies.DonchianBasisStrategy) (total int, wins int, losses int, net string) {
    total = len(s.Trades)
    sum := decimal.NewFromInt(0)
    for _, t := range s.Trades {
        if t.PnlUsd.GreaterThan(decimal.Zero) { wins++ } else if t.PnlUsd.LessThan(decimal.Zero) { losses++ }
        sum = sum.Add(t.PnlUsd)
    }
    net = sum.String()
    return
}


