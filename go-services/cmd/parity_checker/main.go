package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// BinanceAPIResponse represents the response from Binance klines API
type BinanceAPIResponse struct {
	OpenTime                 int64  `json:"0"`
	Open                     string `json:"1"`
	High                     string `json:"2"`
	Low                      string `json:"3"`
	Close                    string `json:"4"`
	Volume                   string `json:"5"`
	CloseTime                int64  `json:"6"`
	QuoteAssetVolume         string `json:"7"`
	NumberOfTrades           int    `json:"8"`
	TakerBuyBaseAssetVolume  string `json:"9"`
	TakerBuyQuoteAssetVolume string `json:"10"`
	Ignore                   string `json:"11"`
}

// ParityChecker validates data against Binance API
type ParityChecker struct {
	conn         driver.Conn
	httpClient   *http.Client
	binanceURL   string
	symbol       string
	interval     string
}

func NewParityChecker(clickhouseURL string) (*ParityChecker, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{clickhouseURL},
		Auth: clickhouse.Auth{
			Database: "backtest",
			Username: "backtest",
			Password: "backtest123",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	return &ParityChecker{
		conn:       conn,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		binanceURL: "https://api.binance.com/api/v3/klines",
		symbol:     "BTCUSDT",
		interval:   "1m",
	}, nil
}

func (pc *ParityChecker) Close() error {
	return pc.conn.Close()
}

// fetchBinanceData fetches klines data from Binance API
func (pc *ParityChecker) fetchBinanceData(startTime, endTime int64) ([]BinanceAPIResponse, error) {
	url := fmt.Sprintf("%s?symbol=%s&interval=%s&startTime=%d&endTime=%d&limit=1000",
		pc.binanceURL, pc.symbol, pc.interval, startTime, endTime)

	log.Printf("Fetching from Binance API: %s", url)

	resp, err := pc.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch from Binance API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Binance API error: %d %s", resp.StatusCode, resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var klines [][]interface{}
	if err := json.Unmarshal(body, &klines); err != nil {
		return nil, fmt.Errorf("failed to parse JSON response: %w", err)
	}

	var results []BinanceAPIResponse
	for _, kline := range klines {
		if len(kline) < 12 {
			continue
		}

		apiResp := BinanceAPIResponse{
			OpenTime:                 int64(kline[0].(float64)),
			Open:                     kline[1].(string),
			High:                     kline[2].(string),
			Low:                      kline[3].(string),
			Close:                    kline[4].(string),
			Volume:                   kline[5].(string),
			CloseTime:                int64(kline[6].(float64)),
			QuoteAssetVolume:         kline[7].(string),
			NumberOfTrades:           int(kline[8].(float64)),
			TakerBuyBaseAssetVolume:  kline[9].(string),
			TakerBuyQuoteAssetVolume: kline[10].(string),
			Ignore:                   kline[11].(string),
		}
		results = append(results, apiResp)
	}

	log.Printf("Fetched %d klines from Binance API", len(results))
	return results, nil
}

// fetchOurData fetches klines data from our database
func (pc *ParityChecker) fetchOurData(startTime, endTime int64) ([]map[string]interface{}, error) {
	query := `
		SELECT 
			open_time_ms, open, high, low, close, volume_base, quote_volume, trades,
			taker_base_vol, taker_quote_vol, close_time_ms
		FROM backtest.ohlcv_raw
		WHERE symbol = ? AND interval = '1m' 
		AND open_time_ms >= ? AND open_time_ms <= ?
		ORDER BY open_time_ms`

	rows, err := pc.conn.Query(pc.conn.Context(), query, pc.symbol, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to query our data: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var (
			openTimeMs     uint64
			open           string
			high           string
			low            string
			close          string
			volumeBase     string
			quoteVolume    string
			trades         uint32
			takerBaseVol   string
			takerQuoteVol  string
			closeTimeMs    uint64
		)

		if err := rows.Scan(&openTimeMs, &open, &high, &low, &close, &volumeBase, &quoteVolume, &trades, &takerBaseVol, &takerQuoteVol, &closeTimeMs); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		result := map[string]interface{}{
			"open_time_ms":     openTimeMs,
			"open":             open,
			"high":             high,
			"low":              low,
			"close":            close,
			"volume_base":      volumeBase,
			"quote_volume":     quoteVolume,
			"trades":           trades,
			"taker_base_vol":   takerBaseVol,
			"taker_quote_vol":  takerQuoteVol,
			"close_time_ms":    closeTimeMs,
		}
		results = append(results, result)
	}

	log.Printf("Fetched %d klines from our database", len(results))
	return results, nil
}

// compareValues compares two decimal values with tolerance
func (pc *ParityChecker) compareValues(ours, binance string, fieldName string) (float64, bool) {
	ourVal, err1 := strconv.ParseFloat(ours, 64)
	binanceVal, err2 := strconv.ParseFloat(binance, 64)

	if err1 != nil || err2 != nil {
		log.Printf("Warning: failed to parse %s values: ours=%s, binance=%s", fieldName, ours, binance)
		return 0, false
	}

	diff := math.Abs(ourVal - binanceVal)
	tolerance := 1e-8 // Very small tolerance for exact match

	return diff, diff <= tolerance
}

// compareKlines compares a single kline from our DB with Binance API
func (pc *ParityChecker) compareKlines(ours map[string]interface{}, binance BinanceAPIResponse) map[string]interface{} {
	openTimeMs := ours["open_time_ms"].(uint64)
	
	result := map[string]interface{}{
		"symbol":           pc.symbol,
		"interval":         pc.interval,
		"open_time_ms":     openTimeMs,
		"binance_open":     binance.Open,
		"binance_high":     binance.High,
		"binance_low":      binance.Low,
		"binance_close":    binance.Close,
		"binance_volume":   binance.Volume,
		"binance_trades":   binance.NumberOfTrades,
		"our_open":         ours["open"],
		"our_high":         ours["high"],
		"our_low":          ours["low"],
		"our_close":        ours["close"],
		"our_volume":       ours["volume_base"],
		"our_trades":       ours["trades"],
		"checked_at":       time.Now(),
	}

	// Compare each field
	openDiff, openMatch := pc.compareValues(ours["open"].(string), binance.Open, "open")
	highDiff, highMatch := pc.compareValues(ours["high"].(string), binance.High, "high")
	lowDiff, lowMatch := pc.compareValues(ours["low"].(string), binance.Low, "low")
	closeDiff, closeMatch := pc.compareValues(ours["close"].(string), binance.Close, "close")
	volumeDiff, volumeMatch := pc.compareValues(ours["volume_base"].(string), binance.Volume, "volume")

	// Compare trades (integer comparison)
	ourTrades := int(ours["trades"].(uint32))
	tradesDiff := int(math.Abs(float64(ourTrades - binance.NumberOfTrades)))
	tradesMatch := tradesDiff == 0

	result["open_diff"] = openDiff
	result["high_diff"] = highDiff
	result["low_diff"] = lowDiff
	result["close_diff"] = closeDiff
	result["volume_diff"] = volumeDiff
	result["trades_diff"] = tradesDiff
	result["is_exact_match"] = openMatch && highMatch && lowMatch && closeMatch && volumeMatch && tradesMatch

	return result
}

// runParityCheck performs parity validation for a time range
func (pc *ParityChecker) runParityCheck(startTime, endTime int64) error {
	log.Printf("Running parity check for %s to %s", 
		time.Unix(startTime/1000, 0).Format("2006-01-02 15:04:05"),
		time.Unix(endTime/1000, 0).Format("2006-01-02 15:04:05"))

	// Fetch data from both sources
	binanceData, err := pc.fetchBinanceData(startTime, endTime)
	if err != nil {
		return fmt.Errorf("failed to fetch Binance data: %w", err)
	}

	ourData, err := pc.fetchOurData(startTime, endTime)
	if err != nil {
		return fmt.Errorf("failed to fetch our data: %w", err)
	}

	// Create maps for efficient lookup
	binanceMap := make(map[uint64]BinanceAPIResponse)
	for _, kline := range binanceData {
		binanceMap[uint64(kline.OpenTime)] = kline
	}

	ourMap := make(map[uint64]map[string]interface{})
	for _, kline := range ourData {
		openTimeMs := kline["open_time_ms"].(uint64)
		ourMap[openTimeMs] = kline
	}

	// Compare data
	var comparisons []map[string]interface{}
	exactMatches := 0
	totalComparisons := 0

	for openTimeMs, ourKline := range ourMap {
		if binanceKline, exists := binanceMap[openTimeMs]; exists {
			comparison := pc.compareKlines(ourKline, binanceKline)
			comparisons = append(comparisons, comparison)
			totalComparisons++

			if comparison["is_exact_match"].(bool) {
				exactMatches++
			}
		}
	}

	// Store results in database
	if err := pc.storeParityResults(comparisons); err != nil {
		log.Printf("Warning: failed to store parity results: %v", err)
	}

	// Log summary
	log.Printf("Parity check summary:")
	log.Printf("  Total comparisons: %d", totalComparisons)
	log.Printf("  Exact matches: %d", exactMatches)
	log.Printf("  Match rate: %.2f%%", float64(exactMatches)/float64(totalComparisons)*100)

	if exactMatches < totalComparisons {
		log.Printf("  Mismatches found: %d", totalComparisons-exactMatches)
	}

	return nil
}

// storeParityResults stores parity check results in the database
func (pc *ParityChecker) storeParityResults(comparisons []map[string]interface{}) error {
	if len(comparisons) == 0 {
		return nil
	}

	query := `
		INSERT INTO backtest.parity_checks (
			symbol, interval, open_time_ms, binance_open, binance_high, binance_low, binance_close,
			binance_volume, binance_trades, our_open, our_high, our_low, our_close,
			our_volume, our_trades, open_diff, high_diff, low_diff, close_diff,
			volume_diff, trades_diff, is_exact_match, checked_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	stmt, err := pc.conn.PrepareBatch(pc.conn.Context(), query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, comp := range comparisons {
		err := stmt.Append(
			comp["symbol"],
			comp["interval"],
			comp["open_time_ms"],
			comp["binance_open"],
			comp["binance_high"],
			comp["binance_low"],
			comp["binance_close"],
			comp["binance_volume"],
			comp["binance_trades"],
			comp["our_open"],
			comp["our_high"],
			comp["our_low"],
			comp["our_close"],
			comp["our_volume"],
			comp["our_trades"],
			comp["open_diff"],
			comp["high_diff"],
			comp["low_diff"],
			comp["close_diff"],
			comp["volume_diff"],
			comp["trades_diff"],
			comp["is_exact_match"],
			comp["checked_at"],
		)
		if err != nil {
			return fmt.Errorf("failed to append to batch: %w", err)
		}
	}

	return stmt.Send()
}

// generateRandomSamples generates random time samples for parity checking
func (pc *ParityChecker) generateRandomSamples(count int, startTime, endTime int64) []int64 {
	var samples []int64
	timeRange := endTime - startTime
	
	for i := 0; i < count; i++ {
		// Generate random offset within the time range
		offset := int64(float64(timeRange) * (float64(i) + 0.5) / float64(count))
		sampleTime := startTime + offset
		
		// Round to nearest minute
		sampleTime = (sampleTime / 60000) * 60000
		samples = append(samples, sampleTime)
	}
	
	return samples
}

// runRandomParityCheck performs parity validation on random samples
func (pc *ParityChecker) runRandomParityCheck(sampleCount int, startTime, endTime int64) error {
	log.Printf("Running random parity check with %d samples", sampleCount)
	
	samples := pc.generateRandomSamples(sampleCount, startTime, endTime)
	
	for i, sampleTime := range samples {
		// Check 1-minute window around the sample
		windowStart := sampleTime
		windowEnd := sampleTime + 60000 // 1 minute
		
		log.Printf("Sample %d/%d: checking %s", i+1, len(samples), 
			time.Unix(sampleTime/1000, 0).Format("2006-01-02 15:04:05"))
		
		if err := pc.runParityCheck(windowStart, windowEnd); err != nil {
			log.Printf("Warning: parity check failed for sample %d: %v", i+1, err)
		}
	}
	
	return nil
}

func main() {
	if len(os.Args) < 4 {
		log.Fatal("Usage: parity_checker <clickhouse_url> <start_time> <end_time> [random_samples]")
	}

	clickhouseURL := os.Args[1]
	
	startTime, err := strconv.ParseInt(os.Args[2], 10, 64)
	if err != nil {
		log.Fatalf("Invalid start time: %v", err)
	}

	endTime, err := strconv.ParseInt(os.Args[3], 10, 64)
	if err != nil {
		log.Fatalf("Invalid end time: %v", err)
	}

	randomSamples := 100
	if len(os.Args) > 4 {
		randomSamples, err = strconv.Atoi(os.Args[4])
		if err != nil {
			log.Fatalf("Invalid random samples count: %v", err)
		}
	}

	// Create parity checker
	checker, err := NewParityChecker(clickhouseURL)
	if err != nil {
		log.Fatalf("Failed to create parity checker: %v", err)
	}
	defer checker.Close()

	// Run random parity check
	if err := checker.runRandomParityCheck(randomSamples, startTime, endTime); err != nil {
		log.Fatalf("Parity check failed: %v", err)
	}

	log.Println("Parity check completed successfully!")
}
