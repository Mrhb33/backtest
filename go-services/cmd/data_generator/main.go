//! Data Generator - Creates sample OHLCV data for testing
//!
//! Generates realistic BTC/USDT price data with some trending periods
//! for testing the EMA/ATR strategy.

package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/shopspring/decimal"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <output_file.csv> [bars]")
		fmt.Println("Example: go run main.go btc_data.csv 1000")
		os.Exit(1)
	}

	outputFile := os.Args[1]
	bars := 1000
	if len(os.Args) > 2 {
		fmt.Sscanf(os.Args[2], "%d", &bars)
	}

	fmt.Printf("Generating %d bars of BTC/USDT data to %s\n", bars, outputFile)

	// Create file
	file, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{"timestamp_ms", "open", "high", "low", "close", "volume"}
	if err := writer.Write(header); err != nil {
		log.Fatalf("Failed to write header: %v", err)
	}

	// Generate data
	rand.Seed(42) // Fixed seed for reproducibility

	// Starting price around $50,000
	price := 50000.0
	// 6 months ago from now
	baseTime := time.Now().AddDate(0, -6, 0)

	for i := 0; i < bars; i++ {
		// Generate some trending periods
		trend := 0.0
		if i > 100 && i < 300 {
			trend = 0.001 // Uptrend
		} else if i > 400 && i < 600 {
			trend = -0.001 // Downtrend
		} else if i > 700 && i < 900 {
			trend = 0.0005 // Gentle uptrend
		}

		// Random walk with trend
		change := (rand.Float64()-0.5)*0.02 + trend // ±1% random + trend
		price *= (1 + change)

		// Ensure price stays reasonable
		if price < 10000 {
			price = 10000
		}
		if price > 100000 {
			price = 100000
		}

		// Generate OHLC
		open := price

		// Intraday volatility
		volatility := 0.005 + rand.Float64()*0.01 // 0.5% to 1.5%
		high := open * (1 + volatility*rand.Float64())
		low := open * (1 - volatility*rand.Float64())
		close := open + (high-low)*(rand.Float64()-0.5)*0.8 // Close somewhere in the range

		// Ensure OHLC relationships are valid
		if high < open {
			high = open
		}
		if high < close {
			high = close
		}
		if low > open {
			low = open
		}
		if low > close {
			low = close
		}

		// Generate volume (random but correlated with price movement)
		volume := 1000 + rand.Float64()*5000 + math.Abs(change)*100000

		// Convert to decimal for precision
		openDec := decimal.NewFromFloat(open)
		highDec := decimal.NewFromFloat(high)
		lowDec := decimal.NewFromFloat(low)
		closeDec := decimal.NewFromFloat(close)
		volumeDec := decimal.NewFromFloat(volume)

		// Timestamp (5-minute bars)
		timestamp := baseTime.Add(time.Duration(i) * 5 * time.Minute).UnixMilli()

		// Write record
		record := []string{
			fmt.Sprintf("%d", timestamp),
			openDec.String(),
			highDec.String(),
			lowDec.String(),
			closeDec.String(),
			volumeDec.String(),
		}

		if err := writer.Write(record); err != nil {
			log.Fatalf("Failed to write record: %v", err)
		}

		// Update price for next iteration
		price = close
	}

	fmt.Printf("Generated %d bars successfully\n", bars)
	fmt.Printf("Price range: $%.2f - $%.2f\n", 10000.0, 100000.0)
}
