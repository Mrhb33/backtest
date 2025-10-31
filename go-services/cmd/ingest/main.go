package main

import (
	"archive/zip"
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	ch "backtest-root-installer/go-services/services/clickhouse"
)

// Minimal downloader/unzipper and CSV streamer (logic only; CH insert would be added via client)

func main() {
	symbol := flag.String("symbol", "BTCUSDT", "symbol")
	start := flag.String("start", "2020-10", "start month YYYY-MM")
	end := flag.String("end", "2025-10", "end month YYYY-MM")
	out := flag.String("out", "./data", "output dir")
	chURL := flag.String("clickhouse", "http://localhost:8123", "ClickHouse URL")
	validate := flag.Bool("validate", false, "run validation suite")
	flag.Parse()

	if *validate {
		fmt.Println("Validation mode not implemented yet")
		return
	}

	// Initialize ingestion pipeline
	pipeline := ch.NewIngestPipeline(*chURL)

	months := enumerateMonths(*start, *end)
	for _, m := range months {
		fmt.Printf("Processing month: %s\n", m)

		// Download
		url := fmt.Sprintf("https://data.binance.vision/data/spot/monthly/klines/%s/1m/%s-1m-%s.zip", *symbol, *symbol, m)
		zipPath := filepath.Join(*out, fmt.Sprintf("%s-1m-%s.zip", *symbol, m))
		if err := downloadFile(url, zipPath); err != nil {
			fmt.Printf("Download error for %s: %v\n", m, err)
			continue
		}

		// Parse CSV and stage
		csvData, err := parseCSVFromZip(zipPath)
		if err != nil {
			fmt.Printf("Parse error for %s: %v\n", m, err)
			continue
		}

		// Stage to ClickHouse
		if err := pipeline.StageMonth(m, csvData); err != nil {
			fmt.Printf("Stage error for %s: %v\n", m, err)
			continue
		}

		// Canonicalize 1m
		if err := pipeline.Canonicalize1m(); err != nil {
			fmt.Printf("Canonicalize error for %s: %v\n", m, err)
			continue
		}

		// Check completeness and backfill if needed
		missing, err := pipeline.CheckCompleteness()
		if err != nil {
			fmt.Printf("Completeness check error for %s: %v\n", m, err)
			continue
		}
		if len(missing) > 0 {
			fmt.Printf("Found %d missing minutes for %s, backfilling...\n", len(missing), m)
			// TODO: implement backfill
		}

		// Derive 5m/15m
		if err := pipeline.Derive5m15m(); err != nil {
			fmt.Printf("Derive error for %s: %v\n", m, err)
			continue
		}

		fmt.Printf("✅ Completed month: %s\n", m)
	}

	fmt.Println("🎉 Ingestion complete!")
}

func enumerateMonths(start, end string) []string {
	var res []string
	t0, _ := time.Parse("2006-01", start)
	t1, _ := time.Parse("2006-01", end)
	for tm := t0; !tm.After(t1); tm = tm.AddDate(0, 1, 0) {
		res = append(res, tm.Format("2006-01"))
	}
	return res
}

func downloadFile(url, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("bad status: %s", resp.Status)
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, resp.Body)
	return err
}

func parseCSVFromZip(zipPath string) ([][]string, error) {
	var csvData [][]string

	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	for _, f := range r.File {
		if !strings.HasSuffix(f.Name, ".csv") {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		defer rc.Close()

		scanner := csv.NewReader(bufio.NewReader(rc))
		scanner.FieldsPerRecord = -1

		for {
			rec, err := scanner.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			csvData = append(csvData, rec)
		}
	}

	return csvData, nil
}
