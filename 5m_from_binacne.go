// binance_5m_to_clickhouse.go
// One-shot installer: downloads Binance 5m candles (Sep 2024 → Sep 2025)
// and inserts them directly into ClickHouse `backtest.data` (deduplicated).
//
// Usage examples:
//   go run binance_5m_to_clickhouse.go \
//     -symbol BTCUSDT \
//     -from 2024-09 \
//     -to 2025-09 \
//     -ch-url http://localhost:18123 \
//     -db backtest \
//     -table data
//
// Defaults match the user's container mapping (18123→8123) and requested period.

package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// === Config flags ===
var (
	flagSymbol = flag.String("symbol", "BTCUSDT", "Trading pair symbol (e.g., BTCUSDT)")
	flagFrom   = flag.String("from", "2024-09", "Start month inclusive (YYYY-MM)")
	flagTo     = flag.String("to", "2025-09", "End month inclusive (YYYY-MM)")
	flagChURL  = flag.String("ch-url", "http://localhost:18123", "ClickHouse HTTP base URL (mapped 18123→8123)")
	flagDB     = flag.String("db", "backtest", "ClickHouse database name")
	flagTable  = flag.String("table", "data", "ClickHouse table name for canonical candles")
	flagBatch  = flag.Int("batch", 5000, "Rows per insert batch")
	flagTmpDir = flag.String("tmp", "./tmp_binance", "Temp directory for downloads")
	flagUser   = flag.String("ch-user", "default", "ClickHouse user")
	flagPass   = flag.String("ch-pass", "", "ClickHouse password")
)

// Binance monthly 5m URL pattern
func monthlyZipURL(symbol string, yyyy int, mm time.Month) string {
	return fmt.Sprintf(
		"https://data.binance.vision/data/spot/monthly/klines/%s/5m/%s-5m-%04d-%02d.zip",
		symbol, symbol, yyyy, int(mm),
	)
}

// Row mapped to ClickHouse `backtest.data` columns
// Adjust names/types to match your table definition.
// We intentionally provide only core columns and rely on defaults for the rest.
type chRow struct {
	Symbol      string `json:"symbol"`
	Interval    string `json:"interval"`
	OpenTimeMs  uint64 `json:"open_time_ms"`
	Open        string `json:"open"`
	High        string `json:"high"`
	Low         string `json:"low"`
	Close       string `json:"close"`
	Volume      string `json:"volume"`
	QuoteVolume string `json:"quote_volume"`
	Trades      uint32 `json:"trades"`
	TakerBase   string `json:"taker_base"`
	TakerQuote  string `json:"taker_quote"`
	CloseTimeMs uint64 `json:"close_time_ms"`
}

// Minimal JSONEachRow batcher with gzip compression
type chBatch struct {
	baseURL   string
	db        string
	table     string
	user      string
	pass      string
	batchSize int
	buf       *bytes.Buffer
	gz        *gzip.Writer
	rowsInBuf int
	client    *http.Client
}

func newBatch(baseURL, db, table, user, pass string, batchSize int) *chBatch {
	b := &chBatch{
		baseURL:   strings.TrimRight(baseURL, "/"),
		db:        db,
		table:     table,
		user:      user,
		pass:      pass,
		batchSize: batchSize,
		buf:       &bytes.Buffer{},
		client:    &http.Client{Timeout: 60 * time.Second},
	}
	b.gz = gzip.NewWriter(b.buf)
	return b
}

func (b *chBatch) add(row chRow) error {
	data, err := json.Marshal(row)
	if err != nil {
		return err
	}
	if _, err := b.gz.Write(data); err != nil {
		return err
	}
	if _, err := b.gz.Write([]byte("\n")); err != nil {
		return err
	}
	b.rowsInBuf++
	if b.rowsInBuf >= b.batchSize {
		return b.flush()
	}
	return nil
}

func (b *chBatch) flush() error {
	if b.rowsInBuf == 0 {
		return nil
	}
	if err := b.gz.Close(); err != nil {
		return err
	}

	// INSERT with explicit column list; rely on table schema for types
	query := fmt.Sprintf(
		"INSERT INTO %s.%s (symbol, interval, open_time_ms, open, high, low, close, volume, quote_volume, trades, taker_base, taker_quote, close_time_ms) FORMAT JSONEachRow",
		b.db, b.table,
	)
	chURL := fmt.Sprintf("%s/?query=%s", b.baseURL, url.QueryEscape(query))

	req, err := http.NewRequest("POST", chURL, bytes.NewReader(b.buf.Bytes()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Content-Encoding", "gzip")
	// Make ClickHouse deduplicate identical rows on insert boundary
	req.Header.Set("X-ClickHouse-Settings", "insert_deduplicate=1,input_format_null_as_default=1,date_time_input_format=best_effort")
	if b.user != "" {
		req.SetBasicAuth(b.user, b.pass)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("clickhouse insert error %d: %s", resp.StatusCode, string(body))
	}

	// reset buffer
	b.buf.Reset()
	b.gz = gzip.NewWriter(b.buf)
	b.rowsInBuf = 0
	return nil
}

func (b *chBatch) close() error { return b.flush() }

// Utility: month iterator inclusive
func iterMonths(from, to time.Time) []time.Time {
	if from.After(to) {
		return nil
	}
	start := time.Date(from.Year(), from.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(to.Year(), to.Month(), 1, 0, 0, 0, 0, time.UTC)
	var months []time.Time
	cur := start
	for !cur.After(end) {
		months = append(months, cur)
		cur = cur.AddDate(0, 1, 0)
	}
	return months
}

func parseYYYYMM(s string) (time.Time, error) {
	parts := strings.Split(s, "-")
	if len(parts) != 2 {
		return time.Time{}, errors.New("month must be YYYY-MM")
	}
	y, err := strconv.Atoi(parts[0])
	if err != nil {
		return time.Time{}, err
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil {
		return time.Time{}, err
	}
	if m < 1 || m > 12 {
		return time.Time{}, errors.New("month out of range")
	}
	return time.Date(y, time.Month(m), 1, 0, 0, 0, 0, time.UTC), nil
}

// download to temp file with simple retry
func downloadToFile(urlStr, dstPath string) error {
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	client := &http.Client{Timeout: 5 * time.Minute}
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		resp, err := client.Get(urlStr)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}
		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
			resp.Body.Close()
			lastErr = fmt.Errorf("http %d: %s", resp.StatusCode, string(body))
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}
		f, err := os.Create(dstPath)
		if err != nil {
			resp.Body.Close()
			return err
		}
		if _, err := io.Copy(f, resp.Body); err != nil {
			resp.Body.Close()
			f.Close()
			return err
		}
		resp.Body.Close()
		f.Close()
		return nil
	}
	return fmt.Errorf("download failed after retries: %w", lastErr)
}

func openFirstCSV(zf string) (io.ReadCloser, string, error) {
	file, err := os.Open(zf)
	if err != nil {
		return nil, "", err
	}
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, "", err
	}
	zr, err := zip.NewReader(file, stat.Size())
	if err != nil {
		file.Close()
		return nil, "", err
	}
	for _, f := range zr.File {
		if strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
			rc, err := f.Open()
			if err != nil {
				file.Close()
				return nil, "", err
			}
			// Wrap rc so closing it also closes the underlying .zip file descriptor
			return &compositeReadCloser{
				ReadCloser: rc,
				closers:    []io.Closer{rc, file},
			}, f.Name, nil
		}
	}
	file.Close()
	return nil, "", errors.New("no CSV found in zip")
}

// compositeReadCloser wraps a ReadCloser and closes additional resources in order
type compositeReadCloser struct {
	io.ReadCloser
	closers []io.Closer
}

// Close closes all nested resources (inner first, then outer file)
func (s *compositeReadCloser) Close() error {
	for i := len(s.closers) - 1; i >= 0; i-- {
		_ = s.closers[i].Close()
	}
	return nil
}

func main() {
	flag.Parse()

	from, err := parseYYYYMM(*flagFrom)
	if err != nil {
		panic(err)
	}
	to, err := parseYYYYMM(*flagTo)
	if err != nil {
		panic(err)
	}
	months := iterMonths(from, to)
	if len(months) == 0 {
		panic("no months in range")
	}

	batch := newBatch(*flagChURL, *flagDB, *flagTable, *flagUser, *flagPass, *flagBatch)
	defer batch.close()

	fmt.Printf("▶ Installing %s 5m candles from %s to %s into %s.%s via %s\n",
		*flagSymbol, months[0].Format("2006-01"), months[len(months)-1].Format("2006-01"), *flagDB, *flagTable, *flagChURL)

	for _, m := range months {
		zipURL := monthlyZipURL(*flagSymbol, m.Year(), m.Month())
		zipPath := filepath.Join(*flagTmpDir, fmt.Sprintf("%s-5m-%04d-%02d.zip", *flagSymbol, m.Year(), int(m.Month())))

		fmt.Printf("↓ %s\n", zipURL)
		if err := downloadToFile(zipURL, zipPath); err != nil {
			panic(fmt.Errorf("download %s failed: %w", zipURL, err))
		}

		rc, name, err := openFirstCSV(zipPath)
		if err != nil {
			panic(err)
		}
		fmt.Printf("• Parsing %s ...\n", name)

		csvr := csv.NewReader(bufio.NewReader(rc))
		csvr.FieldsPerRecord = -1 // tolerate
		rowsRead := 0
		rowsInserted := 0

		for {
			rec, err := csvr.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				rc.Close()
				panic(err)
			}
			if len(rec) < 12 {
				continue
			}

			// Binance kline CSV layout
			// 0 openTime(ms), 1 open, 2 high, 3 low, 4 close, 5 volume,
			// 6 closeTime(ms), 7 quoteAssetVolume, 8 numberOfTrades,
			// 9 takerBuyBaseAssetVolume, 10 takerBuyQuoteAssetVolume, 11 ignore
			openMs, err1 := strconv.ParseUint(rec[0], 10, 64)
			closeMs, err2 := strconv.ParseUint(rec[6], 10, 64)
			tradesU64, err3 := strconv.ParseUint(rec[8], 10, 32)
			if err1 != nil || err2 != nil || err3 != nil {
				continue
			}

			// Normalize timestamp units to milliseconds
			// < 1e12  → seconds → multiply by 1000
			// > 1e14  → microseconds → divide by 1000
			// else    → already milliseconds
			if openMs < 1_000_000_000_000 {
				openMs *= 1000
			} else if openMs > 100_000_000_000_000 {
				openMs /= 1000
			}
			if closeMs < 1_000_000_000_000 {
				closeMs *= 1000
			} else if closeMs > 100_000_000_000_000 {
				closeMs /= 1000
			}

			row := chRow{
				Symbol:      *flagSymbol,
				Interval:    "5m",
				OpenTimeMs:  openMs,
				Open:        rec[1],
				High:        rec[2],
				Low:         rec[3],
				Close:       rec[4],
				Volume:      rec[5],
				QuoteVolume: rec[7],
				Trades:      uint32(tradesU64),
				TakerBase:   rec[9],
				TakerQuote:  rec[10],
				CloseTimeMs: closeMs,
			}

			if err := batch.add(row); err != nil {
				rc.Close()
				panic(err)
			}
			rowsRead++
			rowsInserted++
		}

		rc.Close()
		if err := batch.flush(); err != nil {
			panic(err)
		}
		fmt.Printf("✓ %s %s inserted %d rows\n", *flagSymbol, m.Format("2006-01"), rowsInserted)
	}

	fmt.Println("✅ Completed without errors.")
}
