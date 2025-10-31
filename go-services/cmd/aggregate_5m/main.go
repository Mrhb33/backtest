package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
)

type Bar struct {
	Timestamp int64
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run main.go <input.csv>")
	}

	inputFile := os.Args[1]
	outputFile := "btc_5min_aggregated.csv"

	// Read 1-minute data
	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("Failed to read CSV: %v", err)
	}

	// Parse 1-minute bars
	var bars []Bar
	for i, record := range records {
		if len(record) < 6 {
			continue
		}

		timestamp, err := strconv.ParseInt(record[0], 10, 64)
		if err != nil {
			continue
		}

		open, err := strconv.ParseFloat(record[1], 64)
		if err != nil {
			continue
		}

		high, err := strconv.ParseFloat(record[2], 64)
		if err != nil {
			continue
		}

		low, err := strconv.ParseFloat(record[3], 64)
		if err != nil {
			continue
		}

		close, err := strconv.ParseFloat(record[4], 64)
		if err != nil {
			continue
		}

		volume, err := strconv.ParseFloat(record[5], 64)
		if err != nil {
			volume = 0
		}

		bars = append(bars, Bar{
			Timestamp: timestamp,
			Open:      open,
			High:      high,
			Low:       low,
			Close:     close,
			Volume:    volume,
		})

		if i%10000 == 0 {
			fmt.Printf("Processed %d bars...\n", i)
		}
	}

	fmt.Printf("Loaded %d 1-minute bars\n", len(bars))

	// Aggregate to 5-minute bars
	aggregated := make(map[int64]*Bar)

	for _, bar := range bars {
		// Round timestamp to 5-minute boundary
		fiveMinTimestamp := (bar.Timestamp / 300000) * 300000

		if existing, exists := aggregated[fiveMinTimestamp]; exists {
			// Update existing 5-minute bar
			existing.High = max(existing.High, bar.High)
			existing.Low = min(existing.Low, bar.Low)
			existing.Close = bar.Close // Last close in the 5-minute period
			existing.Volume += bar.Volume
		} else {
			// Create new 5-minute bar
			aggregated[fiveMinTimestamp] = &Bar{
				Timestamp: fiveMinTimestamp,
				Open:      bar.Open,
				High:      bar.High,
				Low:       bar.Low,
				Close:     bar.Close,
				Volume:    bar.Volume,
			}
		}
	}

	// Write 5-minute bars to CSV
	outFile, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer outFile.Close()

	writer := csv.NewWriter(outFile)
	defer writer.Flush()

	// Write header
	writer.Write([]string{"timestamp_ms", "open", "high", "low", "close", "volume"})

	// Write bars in chronological order
	count := 0
	for timestamp := range aggregated {
		bar := aggregated[timestamp]
		writer.Write([]string{
			strconv.FormatInt(bar.Timestamp, 10),
			strconv.FormatFloat(bar.Open, 'f', 2, 64),
			strconv.FormatFloat(bar.High, 'f', 2, 64),
			strconv.FormatFloat(bar.Low, 'f', 2, 64),
			strconv.FormatFloat(bar.Close, 'f', 2, 64),
			strconv.FormatFloat(bar.Volume, 'f', 6, 64),
		})
		count++
	}

	fmt.Printf("Created %d 5-minute bars in %s\n", count, outputFile)
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
