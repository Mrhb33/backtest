package main

import (
	"bufio"
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
)

func main() {
	// Flags
	chURL := flag.String("ch-url", "http://localhost:18123", "ClickHouse HTTP URL")
	db := flag.String("db", "backtest", "ClickHouse database")
	table := flag.String("table", "data", "ClickHouse table")
	symbol := flag.String("symbol", "BTCUSDT", "Trading symbol")
	from := flag.String("from", "2020-09-01 00:00:00", "Start UTC (YYYY-MM-DD HH:MM:SS)")
	to := flag.String("to", "2024-10-01 00:00:00", "End UTC (YYYY-MM-DD HH:MM:SS)")
	user := flag.String("ch-user", "backtest", "ClickHouse user")
	pass := flag.String("ch-pass", "backtest123", "ClickHouse password")
	outCSV := flag.String("out", "./ema_atr_5m.csv", "Temp CSV output path")
	csvPath := flag.String("csv", "", "Path to local CSV; if set, skip ClickHouse download")
	verbose := flag.Bool("verbose", false, "Enable verbose table output to console")
	flag.Parse()

	// If a local CSV is provided, use it and skip ClickHouse export
	if csvPath != nil && *csvPath != "" {
		*outCSV = *csvPath
	} else {
		// Build query to export CSV matching strategies.LoadCSV format
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

		// Download CSV
		if err := os.MkdirAll(filepath.Dir(*outCSV), 0o755); err != nil {
			panic(err)
		}
		resp, err := http.Get(endpoint)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			b, _ := io.ReadAll(resp.Body)
			panic(fmt.Errorf("clickhouse export error %d: %s", resp.StatusCode, string(b)))
		}
		outFile, err := os.Create(*outCSV)
		if err != nil {
			panic(err)
		}
		defer outFile.Close()
		writer := bufio.NewWriter(outFile)
		// Optional header for LoadCSV to skip
		writer.WriteString("timestamp,open,high,low,close,volume\n")
		if _, err := io.Copy(writer, resp.Body); err != nil {
			panic(err)
		}
		writer.Flush()
	}

	// Run strategy
	strat := strategies.NewEMAATRStrategy()
	// Set cadence to 5m
	strat.CadenceMs = 300000
	if verbose != nil && *verbose {
		strat.Verbose = true
		strat.EnableExcel = true
	}
	if csvPath != nil && *csvPath != "" {
		// Preprocess to a clean CSV file (strip quotes), then use strategy loader
		cleanPath := *csvPath + ".clean.csv"
		inF, err := os.Open(*csvPath)
		if err != nil {
			panic(err)
		}
		defer inF.Close()
		outF, err := os.Create(cleanPath)
		if err != nil {
			panic(err)
		}
		w := bufio.NewWriter(outF)
		// Wrap reader with UTF-16 decoder if BOM present; otherwise use raw
		var reader io.Reader = inF
		// Peek first 2 bytes for BOM
		br := bufio.NewReader(inF)
		b1, _ := br.Peek(2)
		if len(b1) >= 2 && ((b1[0] == 0xFF && b1[1] == 0xFE) || (b1[0] == 0xFE && b1[1] == 0xFF)) {
			// Reset to start
			inF.Seek(0, 0)
			reader = transform.NewReader(inF, unicode.UTF16(unicode.LittleEndian, unicode.ExpectBOM).NewDecoder())
		} else {
			// Use buffered reader for performance
			inF.Seek(0, 0)
			reader = br
		}
		scanner := bufio.NewScanner(reader)
		buf := make([]byte, 0, 1024*1024)
		scanner.Buffer(buf, 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}
			line = strings.TrimPrefix(line, "\ufeff")
			line = strings.ReplaceAll(line, "\"", "")
			w.WriteString(line)
			w.WriteByte('\n')
		}
		if err := scanner.Err(); err != nil {
			panic(err)
		}
		if err := w.Flush(); err != nil {
			panic(err)
		}
		outF.Close()

		if err := strat.LoadCSV(cleanPath); err != nil {
			panic(err)
		}
		fmt.Printf("Loaded bars from preprocessed CSV: %d\n", len(strat.Bars))
	} else {
		if err := strat.LoadCSV(*outCSV); err != nil {
			panic(err)
		}
		fmt.Printf("Loaded bars via LoadCSV: %d\n", len(strat.Bars))
	}
	if err := strat.CalculateIndicators(); err != nil {
		panic(err)
	}
	if err := strat.Run(); err != nil {
		panic(err)
	}

	// Print brief summary
	summary := strat.GenerateSummary()
	fmt.Println("=== EMA/ATR Backtest Summary ===")
	fmt.Printf("Period: %s to %s UTC\n", *from, *to)
	fmt.Printf("Bars: %d\n", len(strat.Bars))
	fmt.Printf("Trades: %d, WinRate: %s%%, ProfitFactor: %s, NetPnL: $%s\n",
		summary.TotalTrades, summary.WinRate.String(), summary.ProfitFactor.String(), summary.NetPnlUsd.String())

	// Optionally export table or traces
	_ = strat.ExportCSV("./trades.csv")
	_ = strat.ExportTableTXT("./detailed_table.txt")
	_ = strat.ExportTableTXT("./5_year.txt")
}
