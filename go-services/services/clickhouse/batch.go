package clickhouse

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// BatchClient handles ClickHouse HTTP batch inserts with compression
type BatchClient struct {
	baseURL    string
	username   string
	password   string
	httpClient *http.Client
	buffer     []RawKline
	batchSize  int
}

type RawKline struct {
	OpenTimeMs               string `json:"open_time_ms"`
	Open                     string `json:"open"`
	High                     string `json:"high"`
	Low                      string `json:"low"`
	Close                    string `json:"close"`
	Volume                   string `json:"volume"`
	CloseTimeMs              string `json:"close_time_ms"`
	QuoteAssetVolume         string `json:"quote_asset_volume"`
	NumberOfTrades           string `json:"number_of_trades"`
	TakerBuyBaseAssetVolume  string `json:"taker_buy_base_asset_volume"`
	TakerBuyQuoteAssetVolume string `json:"taker_buy_quote_asset_volume"`
	Ignore                   string `json:"ignore"`
	FileMonth                string `json:"file_month"`
	IngestedAt               string `json:"ingested_at"`
	Source                   string `json:"source"`
}

func NewBatchClient(baseURL string, batchSize int) *BatchClient {
	return &BatchClient{
		baseURL:   baseURL,
		username:  "backtest",
		password:  "backtest123",
		batchSize: batchSize,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		buffer: make([]RawKline, 0, batchSize),
	}
}

func (c *BatchClient) AddKline(kline RawKline) error {
	c.buffer = append(c.buffer, kline)
	if len(c.buffer) >= c.batchSize {
		return c.Flush()
	}
	return nil
}

func (c *BatchClient) Flush() error {
	if len(c.buffer) == 0 {
		return nil
	}

	// Convert to JSONEachRow format (one JSON object per line)
	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)

	for _, kline := range c.buffer {
		jsonData, err := json.Marshal(kline)
		if err != nil {
			return fmt.Errorf("marshal error: %w", err)
		}
		if _, err := gzWriter.Write(jsonData); err != nil {
			return fmt.Errorf("gzip error: %w", err)
		}
		if _, err := gzWriter.Write([]byte("\n")); err != nil {
			return fmt.Errorf("gzip error: %w", err)
		}
	}
	gzWriter.Close()

	// HTTP POST to ClickHouse with CH-friendly settings
	query := "INSERT INTO backtest.raw_klines FORMAT JSONEachRow"
	settings := "input_format_null_as_default=1&date_time_input_format=best_effort"
	url := fmt.Sprintf("%s/?query=%s&%s", c.baseURL, url.QueryEscape(query), settings)
	req, err := http.NewRequestWithContext(context.Background(), "POST", url, &buf)
	if err != nil {
		return fmt.Errorf("request error: %w", err)
	}

	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Content-Encoding", "gzip")
	req.SetBasicAuth(c.username, c.password)
	req.Header.Set("X-ClickHouse-Settings", "max_insert_block_size=1000000,input_format_allow_errors_num=0,insert_deduplicate=1")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("clickhouse error %d: %s", resp.StatusCode, string(body))
	}

	// Clear buffer
	c.buffer = c.buffer[:0]
	return nil
}

func (c *BatchClient) Close() error {
	return c.Flush()
}
