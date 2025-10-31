// install_candles.go
// One-shot installer for Binance spot monthly klines → ClickHouse (1m + derived 5m/15m) with dedup guarantees.
package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// Config via env
type cfg struct {
	DSN        string
	Symbols    []string
	StartYM    string
	EndYM      string
	BaseURL    string
	Database   string
	Table      string
	User       string
	Password   string
	OnlyDerive bool
}

func mustEnv(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func loadCfg() cfg {
	syms := strings.Split(mustEnv("SYMBOLS", "BTCUSDT,ETHUSDT"), ",")
	for i := range syms {
		syms[i] = strings.TrimSpace(syms[i])
	}
	return cfg{
		DSN:        mustEnv("CLICKHOUSE_DSN", "clickhouse://default:@localhost:9000?secure=false&compress=lz4"),
		Symbols:    syms,
		StartYM:    mustEnv("START_YM", "2020-10"),
		EndYM:      mustEnv("END_YM", "2025-10"),
		BaseURL:    mustEnv("BASE_URL", "https://data.binance.vision"),
		Database:   mustEnv("CH_DATABASE", "backtest"),
		Table:      mustEnv("CH_TABLE", "data"),
		User:       mustEnv("CH_USER", "backtest"),
		Password:   mustEnv("CH_PASSWORD", "backtest123"),
		OnlyDerive: strings.EqualFold(mustEnv("ONLY_DERIVE", "false"), "true") || mustEnv("ONLY_DERIVE", "false") == "1",
	}
}

func ymRange(startYM, endYM string) ([]time.Time, error) {
	start, err := time.Parse("2006-01", startYM)
	if err != nil {
		return nil, fmt.Errorf("parse START_YM: %w", err)
	}
	end, err := time.Parse("2006-01", endYM)
	if err != nil {
		return nil, fmt.Errorf("parse END_YM: %w", err)
	}
	if end.Before(start) {
		return nil, errors.New("END_YM < START_YM")
	}
	var out []time.Time
	cur := time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, time.UTC)
	lim := time.Date(end.Year(), end.Month(), 1, 0, 0, 0, 0, time.UTC)
	for !cur.After(lim) {
		out = append(out, cur)
		cur = cur.AddDate(0, 1, 0)
	}
	return out, nil
}

func main() {
	cfg := loadCfg()
	ctx := context.Background()

	// Connect CH
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{dsnHost(cfg.DSN)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": uint64(0),
		},
	})
	if err != nil {
		panic(err)
	}
	if err := conn.Ping(ctx); err != nil {
		panic(fmt.Errorf("clickhouse ping: %w", err))
	}

	// Ensure DB + table
	if err := ensureSchema(ctx, conn, cfg); err != nil {
		panic(err)
	}

	months, err := ymRange(cfg.StartYM, cfg.EndYM)
	if err != nil {
		panic(err)
	}

	// Check if 1m data already exists
	var count uint64
	err = conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s.%s WHERE interval = '1m' LIMIT 1", cfg.Database, cfg.Table)).Scan(&count)
	if !cfg.OnlyDerive && (err != nil || count == 0) {
		// Ingest 1m for each symbol/month
		for _, sym := range cfg.Symbols {
			fmt.Printf("==> %s | 1m monthly ingestion %s to %s\n", sym, cfg.StartYM, cfg.EndYM)
			for _, m := range months {
				if err := ingestMonth1m(ctx, conn, cfg, sym, m); err != nil {
					// Non-fatal: continue other months/symbols
					fmt.Printf("WARN: %s %s 1m ingest failed: %v\n", sym, m.Format("2006-01"), err)
				}
			}
		}
	} else {
		fmt.Printf("==> skipping 1m ingestion (%s). Existing 1m rows: %d\n", map[bool]string{true: "ONLY_DERIVE", false: "already present"}[cfg.OnlyDerive], count)
	}

	// Derive 5m/15m from 1m, idempotent + dedup
	if err := deriveAggregations(ctx, conn, cfg, "5m", 5); err != nil {
		panic(err)
	}
	if err := deriveAggregations(ctx, conn, cfg, "15m", 15); err != nil {
		panic(err)
	}

	if cfg.OnlyDerive {
		fmt.Println("✅ Done. 5m/15m derived (1m ingestion skipped).")
	} else {
		fmt.Println("✅ Done. 1m/5m/15m installed with dedup safeguards.")
	}
}

// dsnHost extracts host:port from a DSN-like URL for driver bootstrap; the DSN itself is parsed internally.
func dsnHost(dsn string) string {
	// very light parsing: find after '@' until '?' or end, else default
	host := "localhost:9000"
	if i := strings.Index(dsn, "@"); i != -1 {
		rest := dsn[i+1:]
		if j := strings.Index(rest, "?"); j != -1 {
			host = rest[:j]
		} else {
			host = rest
		}
		// trim scheme leftovers if present
		host = strings.TrimPrefix(host, "/")
		host = strings.TrimPrefix(host, "//")
	}
	return host
}

func ensureSchema(ctx context.Context, conn clickhouse.Conn, c cfg) error {
	// Create database first
	dbDDL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", c.Database)
	if err := conn.Exec(ctx, dbDDL); err != nil {
		return fmt.Errorf("create database: %w", err)
	}

	// Create table
	tableDDL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			symbol String,
			interval LowCardinality(String),
			open_time_ms UInt64,
			open Float64,
			high Float64,
			low Float64,
			close Float64,
			volume Float64,
			quote_volume Float64,
			trades UInt64,
			taker_base Float64,
			taker_quote Float64,
			close_time_ms UInt64,
			ingested_at DateTime64(3),
			version UInt64
		)
		ENGINE = ReplacingMergeTree(version)
		ORDER BY (symbol, interval, open_time_ms)
		SETTINGS index_granularity = 8192
	`, c.Database, c.Table)
	return conn.Exec(ctx, tableDDL)
}

func ingestMonth1m(ctx context.Context, conn clickhouse.Conn, c cfg, symbol string, month time.Time) error {
	y := month.Year()
	mm := int(month.Month())
	zipURL := fmt.Sprintf("%s/data/spot/monthly/klines/%s/1m/%s-1m-%04d-%02d.zip", c.BaseURL, symbol, symbol, y, mm)

	fmt.Printf("  -> %s\n", zipURL)
	data, err := httpGet(zipURL)
	if err != nil {
		return err
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("zip open: %w", err)
	}
	// find first CSV entry
	var csvFile *zip.File
	for _, f := range zr.File {
		if strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
			csvFile = f
			break
		}
	}
	if csvFile == nil {
		return errors.New("no csv in zip")
	}
	rc, err := csvFile.Open()
	if err != nil {
		return fmt.Errorf("zip entry open: %w", err)
	}
	defer rc.Close()

	reader := csv.NewReader(io.NopCloser(rc))
	reader.FieldsPerRecord = -1
	// Binance columns:
	// 0 Open time(ms), 1 Open, 2 High, 3 Low, 4 Close, 5 Volume, 6 Close time(ms),
	// 7 Quote asset volume, 8 Number of trades, 9 Taker buy base, 10 Taker buy quote, 11 Ignore

	// Prepare batch
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf(`INSERT INTO %s.%s SETTINGS insert_deduplicate=1`, c.Database, c.Table))
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	now := time.Now().UTC()
	ver := uint64(now.UnixNano()) // same for this file; ReplacingMergeTree keeps last

	rows := 0
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("csv read: %w", err)
		}
		if len(rec) < 11 {
			continue
		}
		openMs, _ := parseU64(rec[0])
		open, _ := parseF(rec[1])
		high, _ := parseF(rec[2])
		low, _ := parseF(rec[3])
		closep, _ := parseF(rec[4])
		vol, _ := parseF(rec[5])
		closeMs, _ := parseU64(rec[6])
		qVol, _ := parseF(rec[7])
		trades, _ := parseU64(rec[8])
		tbb, _ := parseF(rec[9])
		tbq, _ := parseF(rec[10])

		if err := batch.Append(
			symbol, "1m",
			openMs,
			open, high, low, closep,
			vol,
			qVol,
			trades,
			tbb,
			tbq,
			closeMs,
			now,
			ver,
		); err != nil {
			return fmt.Errorf("batch append: %w", err)
		}
		rows++
		// Large files? clickhouse-go handles streaming; no need to flush mid-file unless memory spikes.
	}
	if rows == 0 {
		fmt.Println("    (empty)")
		return nil
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("batch send: %w", err)
	}
	fmt.Printf("    inserted %d rows (1m) for %s-%02d\n", rows, month.Format("2006"), int(month.Month()))
	return nil
}

func httpGet(url string) ([]byte, error) {
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("User-Agent", "BridgeHB-CandlesInstaller/1.0")
	client := &http.Client{Timeout: 180 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func deriveAggregations(ctx context.Context, conn clickhouse.Conn, c cfg, tf string, minutes int) error {
	fmt.Printf("==> deriving %s from 1m (dedup on)\n", tf)
	// Aggregate from 1m using a subquery to avoid alias conflicts
	q := fmt.Sprintf(`
        INSERT INTO %s.%s SETTINGS insert_deduplicate=1
        SELECT
            symbol,
            '%s' AS interval,
            toUInt64(toUnixTimestamp(start_ts) * 1000) AS open_time_ms,
            argMin(open, open_time_ms)  AS open,
            max(high)                   AS high,
            min(low)                    AS low,
            argMax(close, open_time_ms) AS close,
            sum(volume)                 AS volume,
            sum(quote_volume)           AS quote_volume,
            sum(trades)                 AS trades,
            sum(taker_base)             AS taker_base,
            sum(taker_quote)            AS taker_quote,
            toUInt64(toUnixTimestamp(start_ts) * 1000 + %d*60*1000 - 1) AS close_time_ms,
            now64(3)                    AS ingested_at,
            toUInt64(toUnixTimestamp64Nano(now64(9))) AS version
        FROM (
            SELECT
                symbol,
                open_time_ms,
                open, high, low, close, volume, quote_volume, trades, taker_base, taker_quote,
                toStartOfInterval(toDateTime(open_time_ms / 1000), INTERVAL %d MINUTE) AS start_ts
            FROM %s.%s
            WHERE interval = '1m'
        )
        GROUP BY symbol, start_ts
    `, c.Database, c.Table, tf, minutes, minutes, c.Database, c.Table)

	return conn.Exec(ctx, q)
}

func parseU64(s string) (uint64, error) { return strconv.ParseUint(strings.TrimSpace(s), 10, 64) }
func parseF(s string) (float64, error)  { return strconv.ParseFloat(strings.TrimSpace(s), 64) }

// --- Optional: pretty printing ClickHouse errors (kept minimal)
func explainCHError(err error) string {
	var ex *chproto.Exception
	if errors.As(err, &ex) {
		return fmt.Sprintf("ClickHouse [%d] %s (%s)", ex.Code, ex.Message, ex.Name)
	}
	return err.Error()
}
