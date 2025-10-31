package clickhouse

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// IngestPipeline orchestrates the complete BTCUSDT ingestion
type IngestPipeline struct {
	baseURL    string
	client     *BatchClient
	httpClient *http.Client
}

func NewIngestPipeline(baseURL string) *IngestPipeline {
	return &IngestPipeline{
		baseURL: baseURL,
		client:  NewBatchClient(baseURL, 1000), // 1k batch size for testing
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// StageMonth loads CSV data into raw_klines
func (p *IngestPipeline) StageMonth(month string, csvData [][]string) error {
	// Check if already ingested (idempotency)
	ledger, err := p.checkIngestLedger(month)
	if err != nil {
		return fmt.Errorf("ledger check error: %w", err)
	}
	if ledger != nil {
		fmt.Printf("Month %s already ingested, skipping\n", month)
		return nil
	}

	// Process CSV rows
	rowCount := 0
	for _, row := range csvData {
		if len(row) < 12 {
			continue // Skip malformed rows
		}

		kline := RawKline{
			OpenTimeMs:               row[0],
			Open:                     row[1],
			High:                     row[2],
			Low:                      row[3],
			Close:                    row[4],
			Volume:                   row[5],
			CloseTimeMs:              row[6],
			QuoteAssetVolume:         row[7],
			NumberOfTrades:           row[8],
			TakerBuyBaseAssetVolume:  row[9],
			TakerBuyQuoteAssetVolume: row[10],
			Ignore:                   row[11],
			FileMonth:                month,
			IngestedAt:               time.Now().Format("2006-01-02 15:04:05"),
			Source:                   "binance_csv",
		}

		if err := p.client.AddKline(kline); err != nil {
			return fmt.Errorf("add kline error: %w", err)
		}
		rowCount++
	}

	// Flush remaining buffer
	if err := p.client.Flush(); err != nil {
		return fmt.Errorf("flush error: %w", err)
	}

	// Record in ledger
	return p.recordIngestLedger(month, "binance_csv", rowCount)
}

// Canonicalize1m runs the canonicalization INSERT...SELECT
func (p *IngestPipeline) Canonicalize1m() error {
	query := `
        INSERT INTO backtest.data
		SELECT
			symbol,
			interval,
			open_time_ms,
			argMax(open_d, ingested_at) AS open,
			argMax(high_d, ingested_at) AS high,
			argMax(low_d, ingested_at) AS low,
			argMax(close_d, ingested_at) AS close,
			argMax(vol_d, ingested_at) AS volume,
			argMax(quote_vol_d, ingested_at) AS quote_volume,
			argMax(trades, ingested_at) AS trades,
			argMax(taker_base_d, ingested_at) AS taker_base,
			argMax(taker_quote_d, ingested_at) AS taker_quote,
			argMax(close_time_ms, ingested_at) AS close_time_ms,
			toUInt64(now64()) AS version,
			now() AS ingested_at
		FROM (
			SELECT
				'BTCUSDT' AS symbol,
				'1m' AS interval,
				toUInt64(open_time_ms) AS open_time_ms,
				toUInt64(close_time_ms) AS close_time_ms,
				toUInt32OrNull(number_of_trades) AS trades,
				-- Cast to Decimal once using OrNull variants
				toDecimal128OrNull(open, 18) AS open_d,
				toDecimal128OrNull(high, 18) AS high_d,
				toDecimal128OrNull(low, 18) AS low_d,
				toDecimal128OrNull(close, 18) AS close_d,
				toDecimal128OrNull(volume, 18) AS vol_d,
				toDecimal128OrNull(quote_asset_volume, 18) AS quote_vol_d,
				toDecimal128OrNull(taker_buy_base_asset_volume, 18) AS taker_base_d,
				toDecimal128OrNull(taker_buy_quote_asset_volume, 18) AS taker_quote_d,
				ingested_at
            FROM backtest.raw_klines
			-- Filter first to reduce data before grouping
			WHERE
				toUInt64(open_time_ms) > 0
				AND toUInt64(close_time_ms) > 0
				AND toDecimal128OrNull(open, 18) IS NOT NULL
				AND toDecimal128OrNull(high, 18) IS NOT NULL
				AND toDecimal128OrNull(low, 18) IS NOT NULL
				AND toDecimal128OrNull(close, 18) IS NOT NULL
				AND toDecimal128OrNull(volume, 18) IS NOT NULL
				AND toDecimal128OrNull(quote_asset_volume, 18) IS NOT NULL
				-- Time alignment checks
				AND (toUInt64(open_time_ms) % 60000) = 0
				AND toUInt64(close_time_ms) = toUInt64(open_time_ms) + 60000 - 1
		)
		-- Additional sanity checks on the already-cast columns
		WHERE
			open_d IS NOT NULL
			AND high_d IS NOT NULL
			AND low_d IS NOT NULL
			AND close_d IS NOT NULL
			AND vol_d IS NOT NULL
			AND quote_vol_d IS NOT NULL
			-- Sanity checks
			AND high_d >= greatest(open_d, close_d, low_d)
			AND low_d <= least(open_d, close_d, high_d)
		GROUP BY symbol, interval, open_time_ms
	`

	return p.executeQuery(query)
}

// CheckCompleteness returns missing minutes for 1m
func (p *IngestPipeline) CheckCompleteness() ([]uint64, error) {
	// This would execute the query and return missing timestamps
	// For now, return empty slice as placeholder
	return []uint64{}, nil
}

// Derive5m15m creates 5m and 15m bars from 1m
func (p *IngestPipeline) Derive5m15m() error {
	// Derive 5m
	query5m := `
        INSERT INTO backtest.data
		SELECT
			'BTCUSDT' AS symbol,
			'5m' AS interval,
			(intDiv(open_time_ms, 300000) * 300000) AS open_time_ms,
			any(open) AS open,
			max(high) AS high,
			min(low) AS low,
			anyLast(close) AS close,
			sum(volume) AS volume,
			sum(quote_volume) AS quote_volume,
			sum(trades) AS trades,
			sum(taker_base) AS taker_base,
			sum(taker_quote) AS taker_quote,
			open_time_ms + 300000 - 1 AS close_time_ms,
			toUInt64(now64()) AS version,
			now() AS ingested_at
        FROM backtest.data
		WHERE symbol='BTCUSDT' AND interval='1m'
		GROUP BY open_time_ms
	`

	if err := p.executeQuery(query5m); err != nil {
		return fmt.Errorf("derive 5m error: %w", err)
	}

	// Derive 15m
	query15m := `
        INSERT INTO backtest.data
		SELECT
			'BTCUSDT' AS symbol,
			'15m' AS interval,
			(intDiv(open_time_ms, 900000) * 900000) AS open_time_ms,
			any(open) AS open,
			max(high) AS high,
			min(low) AS low,
			anyLast(close) AS close,
			sum(volume) AS volume,
			sum(quote_volume) AS quote_volume,
			sum(trades) AS trades,
			sum(taker_base) AS taker_base,
			sum(taker_quote) AS taker_quote,
			open_time_ms + 900000 - 1 AS close_time_ms,
			toUInt64(now64()) AS version,
			now() AS ingested_at
        FROM backtest.data
		WHERE symbol='BTCUSDT' AND interval='1m'
		GROUP BY open_time_ms
	`

	return p.executeQuery(query15m)
}

// Helper methods
func (p *IngestPipeline) checkIngestLedger(month string) (*IngestLedger, error) {
	query := fmt.Sprintf(`SELECT month, file_sha256, row_count, source, inserted_at FROM backtest.ingest_ledger WHERE month = '%s' LIMIT 1`, month)
	
	url := fmt.Sprintf("%s/?query=%s", p.baseURL, url.QueryEscape(query))
	req, err := http.NewRequestWithContext(context.Background(), "GET", url, strings.NewReader(""))
	if err != nil {
		return nil, fmt.Errorf("request error: %w", err)
	}

	req.SetBasicAuth("backtest", "backtest123")
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("clickhouse error: %d %s", resp.StatusCode, string(body))
	}

	// Parse response - for now return nil (not found)
	// In a real implementation, you'd parse the JSON response
	return nil, nil
}

func (p *IngestPipeline) recordIngestLedger(month, source string, rowCount int) error {
	// Generate a simple hash for now
	fileSHA := fmt.Sprintf("sha256_%s_%d", month, time.Now().Unix())
	
	query := fmt.Sprintf(`INSERT INTO backtest.ingest_ledger (month, file_sha256, row_count, source) VALUES ('%s', '%s', %d, '%s')`, 
		month, fileSHA, rowCount, source)
	
	url := fmt.Sprintf("%s/?query=%s", p.baseURL, url.QueryEscape(query))
	
	req, err := http.NewRequestWithContext(context.Background(), "POST", url, strings.NewReader(""))
	if err != nil {
		return fmt.Errorf("request error: %w", err)
	}

	req.SetBasicAuth("backtest", "backtest123")
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("clickhouse error: %d %s", resp.StatusCode, string(body))
	}

	return nil
}

func (p *IngestPipeline) executeQuery(query string) error {
	// Execute query via HTTP client
	url := fmt.Sprintf("%s/?query=%s", p.baseURL, url.QueryEscape(query))
	req, err := http.NewRequestWithContext(context.Background(), "POST", url, strings.NewReader(""))
	if err != nil {
		return fmt.Errorf("request error: %w", err)
	}

	req.SetBasicAuth("backtest", "backtest123")
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("clickhouse error: %d %s", resp.StatusCode, string(body))
	}

	return nil
}

type IngestLedger struct {
	Month     string
	FileSHA   string
	RowCount  int
	Source    string
	CreatedAt time.Time
}
