// install_derived_clean.go
// One-shot derivation: 5m/15m from existing 1m in ClickHouse with dedup/idempotency.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

type deriveCfg struct {
	DSN      string
	Database string
	Table    string
}

func getEnv(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func loadDeriveCfg() deriveCfg {
	return deriveCfg{
		DSN:      getEnv("CLICKHOUSE_DSN", "clickhouse://default:@localhost:9000?secure=false&compress=lz4"),
		Database: getEnv("CH_DATABASE", "backtest"),
		Table:    getEnv("CH_TABLE", "data"),
	}
}

func main() {
	cfg := loadDeriveCfg()
	ctx := context.Background()

	// Open ClickHouse using DSN
	opts, err := clickhouse.ParseDSN(cfg.DSN)
	if err != nil {
		panic(fmt.Errorf("parse DSN: %w", err))
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		panic(err)
	}
	if err := conn.Ping(ctx); err != nil {
		panic(fmt.Errorf("clickhouse ping: %w", err))
	}

	// Check if 1m data exists
	var count uint64
	err = conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s.%s WHERE interval = '1m' LIMIT 1", cfg.Database, cfg.Table)).Scan(&count)
	if err != nil {
		panic(fmt.Errorf("check 1m data: %w", err))
	}
	if count == 0 {
		panic("No 1m data found. Run install_candles.go first to ingest 1m data.")
	}
	fmt.Printf("==> Found %d 1m rows, deriving 5m/15m...\n", count)

	// Derive 5m/15m (idempotent; insert_deduplicate=1)
	if err := deriveTimeframes(ctx, conn, cfg, "5m", 5); err != nil {
		panic(fmt.Errorf("derive 5m: %w", err))
	}
	if err := deriveTimeframes(ctx, conn, cfg, "15m", 15); err != nil {
		panic(fmt.Errorf("derive 15m: %w", err))
	}

	fmt.Println("✅ Done. 5m/15m derived from existing 1m with dedup guarantees.")
}

func deriveTimeframes(ctx context.Context, conn clickhouse.Conn, c deriveCfg, tf string, minutes int) error {
	fmt.Printf("==> deriving %s from 1m (dedup on)\n", tf)
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
			max(close_time_ms)          AS close_time_ms,
			sum(quote_volume)           AS quote_volume,
			sum(trades)                 AS trades,
			sum(taker_base)             AS taker_base,
			sum(taker_quote)            AS taker_quote,
			now64(3)                    AS ingested_at,
			toUInt64(toUnixTimestamp64Nano(now64(9))) AS version
		FROM (
			SELECT
				symbol,
				open_time_ms,
				open, high, low, close, volume, quote_volume, trades,
				taker_base, taker_quote, close_time_ms,
				toStartOfInterval(toDateTime(open_time_ms / 1000), INTERVAL %d MINUTE) AS start_ts
			FROM %s.%s
			WHERE interval = '1m'
		)
		GROUP BY symbol, start_ts
	`, c.Database, c.Table, tf, minutes, c.Database, c.Table)

	return conn.Exec(ctx, q)
}
