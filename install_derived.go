//go:build derive_only
// +build derive_only

// install_derived.go
// One-shot derivation of 5m/15m from existing 1m in ClickHouse, idempotent with dedup.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

type cfg struct {
	DSN      string
	Database string
	Table    string
	User     string
	Password string
}

func mustEnv(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func loadCfg() cfg {
	return cfg{
		DSN:      mustEnv("CLICKHOUSE_DSN", "clickhouse://default:@localhost:9000?secure=false&compress=lz4"),
		Database: mustEnv("CH_DATABASE", "backtest"),
		Table:    mustEnv("CH_TABLE", "data"),
		User:     mustEnv("CH_USER", "backtest"),
		Password: mustEnv("CH_PASSWORD", "backtest123"),
	}
}

func dsnHost(dsn string) string {
	host := "localhost:9000"
	if i := strings.Index(dsn, "@"); i != -1 {
		rest := dsn[i+1:]
		if j := strings.Index(rest, "?"); j != -1 {
			host = rest[:j]
		} else {
			host = rest
		}
		host = strings.TrimPrefix(host, "/")
		host = strings.TrimPrefix(host, "//")
	}
	return host
}

func main() {
	cfg := loadCfg()
	ctx := context.Background()

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

	fmt.Println("==> deriving 5m from 1m (dedup on)")
	if err := deriveAggregations(ctx, conn, cfg, "5m", 5); err != nil {
		panic(err)
	}
	fmt.Println("==> deriving 15m from 1m (dedup on)")
	if err := deriveAggregations(ctx, conn, cfg, "15m", 15); err != nil {
		panic(err)
	}

	fmt.Println("✅ Done. 5m/15m derived from existing 1m.")
}

// deriveAggregations aggregates 1m rows into target timeframe
func deriveAggregations(ctx context.Context, conn clickhouse.Conn, c cfg, tf string, minutes int) error {
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
