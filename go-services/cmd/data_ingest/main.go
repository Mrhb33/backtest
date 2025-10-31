package main

import (
	"bufio"
	"compress/gzip"
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// BinanceKline represents a single kline from Binance CSV
type BinanceKline struct {
	OpenTimeMs           uint64
	Open                 string
	High                 string
	Low                  string
	Close                string
	VolumeBase           string
	CloseTimeMs          uint64
	QuoteAssetVolume     string
	NumberOfTrades       uint32
	TakerBuyBaseVolume   string
	TakerBuyQuoteVolume  string
	Ignore               string
}

// NormalizedKline represents our canonical OHLCV format
type NormalizedKline struct {
	Symbol          string
	Interval        string
	OpenTimeMs      uint64
	Open            string
	High            string
	Low             string
	Close           string
	VolumeBase      string
	QuoteVolume     string
	Trades          uint32
	TakerBaseVol    string
	TakerQuoteVol   string
	CloseTimeMs     uint64
	Source          string
	FileMonth       string
	IngestedAt      time.Time
	RowHash         string
}

// DataIngester handles the complete data ingestion pipeline
type DataIngester struct {
	conn     driver.Conn
	symbol   string
	interval string
	source   string
}

func NewDataIngester(clickhouseURL string) (*DataIngester, error) {
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

	return &DataIngester{
		conn:     conn,
		symbol:   "BTCUSDT",
		interval: "1m",
		source:   "binance-bulk",
	}, nil
}

func (di *DataIngester) Close() error {
	return di.conn.Close()
}

// calculateRowHash computes MD5 hash for data validation
func (di *DataIngester) calculateRowHash(kline *NormalizedKline) string {
	data := fmt.Sprintf("%s|%s|%d|%s|%s|%s|%s|%s|%s|%d|%s|%s|%d",
		kline.Symbol,
		kline.Interval,
		kline.OpenTimeMs,
		kline.Open,
		kline.High,
		kline.Low,
		kline.Close,
		kline.VolumeBase,
		kline.QuoteVolume,
		kline.Trades,
		kline.TakerBaseVol,
		kline.TakerQuoteVol,
		kline.CloseTimeMs,
	)
	hash := md5.Sum([]byte(data))
	return hex.EncodeToString(hash[:])
}

// parseBinanceKline parses a single line from Binance CSV
func (di *DataIngester) parseBinanceKline(line []string) (*BinanceKline, error) {
	if len(line) < 12 {
		return nil, fmt.Errorf("invalid CSV line: expected 12 fields, got %d", len(line))
	}

	openTimeMs, err := strconv.ParseUint(line[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid open_time_ms: %w", err)
	}

	closeTimeMs, err := strconv.ParseUint(line[6], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid close_time_ms: %w", err)
	}

	numberOfTrades, err := strconv.ParseUint(line[8], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid number_of_trades: %w", err)
	}

	return &BinanceKline{
		OpenTimeMs:          openTimeMs,
		Open:                line[1],
		High:                line[2],
		Low:                 line[3],
		Close:               line[4],
		VolumeBase:          line[5],
		CloseTimeMs:         closeTimeMs,
		QuoteAssetVolume:    line[7],
		NumberOfTrades:      uint32(numberOfTrades),
		TakerBuyBaseVolume:  line[9],
		TakerBuyQuoteVolume: line[10],
		Ignore:              line[11],
	}, nil
}

// normalizeKline converts Binance format to our canonical format
func (di *DataIngester) normalizeKline(bk *BinanceKline, fileMonth string) *NormalizedKline {
	// Standardize close_time_ms: open_time_ms + interval_ms - 1
	intervalMs := uint64(60000) // 1 minute in milliseconds
	standardizedCloseTimeMs := bk.OpenTimeMs + intervalMs - 1

	// Parse file month from filename (e.g., "2025-09-01")
	parsedFileMonth, _ := time.Parse("2006-01-02", fileMonth)

	nk := &NormalizedKline{
		Symbol:         di.symbol,
		Interval:       di.interval,
		OpenTimeMs:     bk.OpenTimeMs,
		Open:           bk.Open,
		High:           bk.High,
		Low:            bk.Low,
		Close:          bk.Close,
		VolumeBase:     bk.VolumeBase,
		QuoteVolume:    bk.QuoteAssetVolume,
		Trades:         bk.NumberOfTrades,
		TakerBaseVol:   bk.TakerBuyBaseVolume,
		TakerQuoteVol:  bk.TakerBuyQuoteVolume,
		CloseTimeMs:    standardizedCloseTimeMs,
		Source:         di.source,
		FileMonth:      fileMonth,
		IngestedAt:     time.Now().UTC(),
	}

	// Calculate row hash for validation
	nk.RowHash = di.calculateRowHash(nk)

	return nk
}

// validateKline performs data integrity checks
func (di *DataIngester) validateKline(nk *NormalizedKline) error {
	// Parse numeric values for validation
	open, err := strconv.ParseFloat(nk.Open, 64)
	if err != nil {
		return fmt.Errorf("invalid open price: %w", err)
	}

	high, err := strconv.ParseFloat(nk.High, 64)
	if err != nil {
		return fmt.Errorf("invalid high price: %w", err)
	}

	low, err := strconv.ParseFloat(nk.Low, 64)
	if err != nil {
		return fmt.Errorf("invalid low price: %w", err)
	}

	close, err := strconv.ParseFloat(nk.Close, 64)
	if err != nil {
		return fmt.Errorf("invalid close price: %w", err)
	}

	volume, err := strconv.ParseFloat(nk.VolumeBase, 64)
	if err != nil {
		return fmt.Errorf("invalid volume: %w", err)
	}

	// Validate OHLC relationships
	if high < low {
		return fmt.Errorf("high < low: high=%.8f, low=%.8f", high, low)
	}
	if high < open {
		return fmt.Errorf("high < open: high=%.8f, open=%.8f", high, open)
	}
	if high < close {
		return fmt.Errorf("high < close: high=%.8f, close=%.8f", high, close)
	}
	if low > open {
		return fmt.Errorf("low > open: low=%.8f, open=%.8f", low, open)
	}
	if low > close {
		return fmt.Errorf("low > close: low=%.8f, close=%.8f", low, close)
	}

	// Validate volume
	if volume < 0 {
		return fmt.Errorf("negative volume: %.12f", volume)
	}

	// Validate time alignment (1-minute intervals)
	if nk.OpenTimeMs%60000 != 0 {
		return fmt.Errorf("time not aligned to minute: %d", nk.OpenTimeMs)
	}

	return nil
}

// processFile processes a single CSV file
func (di *DataIngester) processFile(filePath string) error {
	log.Printf("Processing file: %s", filePath)

	// Extract file month from filename
	fileName := filepath.Base(filePath)
	fileMonth := strings.TrimSuffix(fileName, ".csv")
	if len(fileMonth) > 10 {
		fileMonth = fileMonth[:10] // Take first 10 characters (YYYY-MM-DD)
	}

	// Open file (handle both .csv and .csv.gz)
	var reader io.Reader
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	if strings.HasSuffix(filePath, ".gz") {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	} else {
		reader = file
	}

	// Parse CSV
	csvReader := csv.NewReader(reader)
	csvReader.FieldsPerRecord = 12

	var batch []*NormalizedKline
	batchSize := 50000
	processedCount := 0
	errorCount := 0

	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading CSV line: %v", err)
			errorCount++
			continue
		}

		// Parse Binance kline
		binanceKline, err := di.parseBinanceKline(line)
		if err != nil {
			log.Printf("Error parsing kline: %v", err)
			errorCount++
			continue
		}

		// Normalize to our format
		normalizedKline := di.normalizeKline(binanceKline, fileMonth)

		// Validate data integrity
		if err := di.validateKline(normalizedKline); err != nil {
			log.Printf("Validation error: %v", err)
			errorCount++
			continue
		}

		batch = append(batch, normalizedKline)
		processedCount++

		// Insert batch when it reaches batch size
		if len(batch) >= batchSize {
			if err := di.insertBatch(batch); err != nil {
				log.Printf("Error inserting batch: %v", err)
				errorCount += len(batch)
			} else {
				log.Printf("Inserted batch of %d records", len(batch))
			}
			batch = batch[:0] // Reset batch
		}
	}

	// Insert remaining records
	if len(batch) > 0 {
		if err := di.insertBatch(batch); err != nil {
			log.Printf("Error inserting final batch: %v", err)
			errorCount += len(batch)
		} else {
			log.Printf("Inserted final batch of %d records", len(batch))
		}
	}

	log.Printf("File processing complete: %d processed, %d errors", processedCount, errorCount)
	return nil
}

// insertBatch inserts a batch of normalized klines into staging table
func (di *DataIngester) insertBatch(batch []*NormalizedKline) error {
	if len(batch) == 0 {
		return nil
	}

	query := `
		INSERT INTO backtest.staging_klines (
			symbol, interval, open_time_ms, open, high, low, close,
			volume_base, quote_volume, trades, taker_base_vol, taker_quote_vol,
			close_time_ms, source, file_month, ingested_at, row_hash
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	stmt, err := di.conn.PrepareBatch(di.conn.Context(), query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, nk := range batch {
		err := stmt.Append(
			nk.Symbol,
			nk.Interval,
			nk.OpenTimeMs,
			nk.Open,
			nk.High,
			nk.Low,
			nk.Close,
			nk.VolumeBase,
			nk.QuoteVolume,
			nk.Trades,
			nk.TakerBaseVol,
			nk.TakerQuoteVol,
			nk.CloseTimeMs,
			nk.Source,
			nk.FileMonth,
			nk.IngestedAt,
			nk.RowHash,
		)
		if err != nil {
			return fmt.Errorf("failed to append to batch: %w", err)
		}
	}

	return stmt.Send()
}

// deduplicateAndMove moves data from staging to canonical table with deduplication
func (di *DataIngester) deduplicateAndMove() error {
	log.Println("Starting deduplication and move to canonical table...")

	query := `
		INSERT INTO backtest.ohlcv_raw
		SELECT 
			symbol, interval, open_time_ms, open, high, low, close,
			volume_base, quote_volume, trades, taker_base_vol, taker_quote_vol,
			close_time_ms, source, file_month, ingested_at
		FROM (
			SELECT *,
				row_number() OVER (
					PARTITION BY symbol, interval, open_time_ms 
					ORDER BY ingested_at DESC
				) AS rn
			FROM backtest.staging_klines
		)
		WHERE rn = 1`

	_, err := di.conn.Exec(di.conn.Context(), query)
	if err != nil {
		return fmt.Errorf("failed to deduplicate and move data: %w", err)
	}

	log.Println("Deduplication and move completed")
	return nil
}

// deriveHigherTimeframes creates 5m and 15m data from 1m
func (di *DataIngester) deriveHigherTimeframes() error {
	log.Println("Deriving 5m and 15m timeframes from 1m data...")

	// Derive 5m data
	query5m := `
		INSERT INTO backtest.ohlcv_5m
		SELECT
			symbol,
			'5m' AS interval,
			toUnixTimestamp64Milli(toStartOfFiveMinute(toDateTime(open_time_ms/1000))) AS open_time_ms,
			argMin(open, open_time_ms) AS open,
			max(high) AS high,
			min(low) AS low,
			argMax(close, open_time_ms) AS close,
			sum(volume_base) AS volume_base,
			sum(quote_volume) AS quote_volume,
			sum(trades) AS trades,
			sum(taker_base_vol) AS taker_base_vol,
			sum(taker_quote_vol) AS taker_quote_vol,
			toUnixTimestamp64Milli(toStartOfFiveMinute(toDateTime(open_time_ms/1000))) + 300000 - 1 AS close_time_ms,
			'derived-5m' AS source,
			toDate(toDateTime(open_time_ms/1000)) AS file_month,
			now64(3) AS ingested_at
		FROM backtest.ohlcv_raw
		WHERE symbol = ? AND interval = '1m'
		GROUP BY symbol, toStartOfFiveMinute(toDateTime(open_time_ms/1000))`

	_, err := di.conn.Exec(di.conn.Context(), query5m, di.symbol)
	if err != nil {
		return fmt.Errorf("failed to derive 5m data: %w", err)
	}

	// Derive 15m data
	query15m := `
		INSERT INTO backtest.ohlcv_15m
		SELECT
			symbol,
			'15m' AS interval,
			toUnixTimestamp64Milli(toStartOfFifteenMinutes(toDateTime(open_time_ms/1000))) AS open_time_ms,
			argMin(open, open_time_ms) AS open,
			max(high) AS high,
			min(low) AS low,
			argMax(close, open_time_ms) AS close,
			sum(volume_base) AS volume_base,
			sum(quote_volume) AS quote_volume,
			sum(trades) AS trades,
			sum(taker_base_vol) AS taker_base_vol,
			sum(taker_quote_vol) AS taker_quote_vol,
			toUnixTimestamp64Milli(toStartOfFifteenMinutes(toDateTime(open_time_ms/1000))) + 900000 - 1 AS close_time_ms,
			'derived-15m' AS source,
			toDate(toDateTime(open_time_ms/1000)) AS file_month,
			now64(3) AS ingested_at
		FROM backtest.ohlcv_raw
		WHERE symbol = ? AND interval = '1m'
		GROUP BY symbol, toStartOfFifteenMinutes(toDateTime(open_time_ms/1000))`

	_, err = di.conn.Exec(di.conn.Context(), query15m, di.symbol)
	if err != nil {
		return fmt.Errorf("failed to derive 15m data: %w", err)
	}

	log.Println("Higher timeframe derivation completed")
	return nil
}

// runDataQualityChecks performs comprehensive data quality validation
func (di *DataIngester) runDataQualityChecks() error {
	log.Println("Running data quality checks...")

	// Check for missing minutes
	missingQuery := `
		INSERT INTO backtest.missing_minutes
		SELECT symbol, interval, expected_time_ms, now64(3)
		FROM backtest.find_missing_1m`

	_, err := di.conn.Exec(di.conn.Context(), missingQuery)
	if err != nil {
		log.Printf("Warning: failed to check missing minutes: %v", err)
	}

	// Check for anomalies
	anomalyQuery := `
		INSERT INTO backtest.data_anomalies
		SELECT 
			symbol, interval, open_time_ms, anomaly_type, 
			CASE 
				WHEN anomaly_type = 'negative_wick' THEN 4
				WHEN anomaly_type = 'zero_volume_with_trades' THEN 3
				WHEN anomaly_type = 'price_spike' THEN 3
				ELSE 1
			END as severity,
			details, now64(3)
		FROM backtest.detect_anomalies`

	_, err = di.conn.Exec(di.conn.Context(), anomalyQuery)
	if err != nil {
		log.Printf("Warning: failed to check anomalies: %v", err)
	}

	// Generate daily checksums
	checksumQuery := `
		INSERT INTO backtest.daily_checksums
		SELECT 
			symbol, interval, day, 
			md5Hex(sum(CAST(md5Hex(concat(toString(open_time_ms), toString(open), toString(high), toString(low), toString(close), toString(volume_base))) AS UInt128))) as checksum,
			count() as row_count,
			min(open_time_ms) as first_time_ms,
			max(open_time_ms) as last_time_ms,
			now64(3) as computed_at
		FROM backtest.ohlcv_raw
		WHERE symbol = ? AND interval = '1m'
		GROUP BY symbol, interval, day`

	_, err = di.conn.Exec(di.conn.Context(), checksumQuery, di.symbol)
	if err != nil {
		log.Printf("Warning: failed to generate checksums: %v", err)
	}

	log.Println("Data quality checks completed")
	return nil
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: data_ingest <csv_file_path> [clickhouse_url]")
	}

	filePath := os.Args[1]
	clickhouseURL := "localhost:9000"
	if len(os.Args) > 2 {
		clickhouseURL = os.Args[2]
	}

	// Create data ingester
	ingester, err := NewDataIngester(clickhouseURL)
	if err != nil {
		log.Fatalf("Failed to create data ingester: %v", err)
	}
	defer ingester.Close()

	// Process the file
	if err := ingester.processFile(filePath); err != nil {
		log.Fatalf("Failed to process file: %v", err)
	}

	// Deduplicate and move to canonical table
	if err := ingester.deduplicateAndMove(); err != nil {
		log.Fatalf("Failed to deduplicate and move data: %v", err)
	}

	// Derive higher timeframes
	if err := ingester.deriveHigherTimeframes(); err != nil {
		log.Fatalf("Failed to derive higher timeframes: %v", err)
	}

	// Run data quality checks
	if err := ingester.runDataQualityChecks(); err != nil {
		log.Fatalf("Failed to run data quality checks: %v", err)
	}

	log.Println("Data ingestion pipeline completed successfully!")
}
