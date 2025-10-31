package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// BinanceDownloader handles downloading and verifying Binance bulk data
type BinanceDownloader struct {
	baseURL    string
	dataDir    string
	symbol     string
	httpClient *http.Client
}

func NewBinanceDownloader(dataDir string) *BinanceDownloader {
	return &BinanceDownloader{
		baseURL: "https://data.binance.vision/data/spot/monthly/klines",
		dataDir: dataDir,
		symbol:  "BTCUSDT",
		httpClient: &http.Client{
			Timeout: 30 * time.Minute,
		},
	}
}

// DownloadInfo represents information about a downloaded file
type DownloadInfo struct {
	Symbol     string
	Interval   string
	Year       int
	Month      int
	FilePath   string
	Size       int64
	Checksum   string
	Downloaded bool
	Verified   bool
}

// generateFileName creates the expected filename for Binance data
func (bd *BinanceDownloader) generateFileName(year, month int) string {
	return fmt.Sprintf("%s-1m-%d-%02d.zip", bd.symbol, year, month)
}

// generateChecksumFileName creates the checksum filename
func (bd *BinanceDownloader) generateChecksumFileName(year, month int) string {
	return fmt.Sprintf("%s-1m-%d-%02d.zip.checksum", bd.symbol, year, month)
}

// downloadFile downloads a file from URL to local path
func (bd *BinanceDownloader) downloadFile(url, filePath string) error {
	log.Printf("Downloading: %s", url)

	// Create directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Create or truncate file
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filePath, err)
	}
	defer file.Close()

	// Download file
	resp, err := bd.httpClient.Get(url)
	if err != nil {
		return fmt.Errorf("failed to download %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP error: %d %s", resp.StatusCode, resp.Status)
	}

	// Copy to file
	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to write file %s: %w", filePath, err)
	}

	log.Printf("Downloaded: %s", filePath)
	return nil
}

// verifyChecksum verifies the MD5 checksum of a file
func (bd *BinanceDownloader) verifyChecksum(filePath, expectedChecksum string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file for checksum verification: %w", err)
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return fmt.Errorf("failed to calculate checksum: %w", err)
	}

	actualChecksum := hex.EncodeToString(hash.Sum(nil))
	if actualChecksum != expectedChecksum {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", expectedChecksum, actualChecksum)
	}

	log.Printf("Checksum verified: %s", filePath)
	return nil
}

// downloadChecksum downloads and parses the checksum file
func (bd *BinanceDownloader) downloadChecksum(year, month int) (string, error) {
	checksumFileName := bd.generateChecksumFileName(year, month)
	url := fmt.Sprintf("%s/%s/%s", bd.baseURL, bd.symbol, checksumFileName)
	
	checksumFilePath := filepath.Join(bd.dataDir, "checksums", checksumFileName)
	
	// Download checksum file
	if err := bd.downloadFile(url, checksumFilePath); err != nil {
		return "", fmt.Errorf("failed to download checksum file: %w", err)
	}

	// Read checksum
	content, err := os.ReadFile(checksumFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to read checksum file: %w", err)
	}

	// Parse checksum (format: "checksum filename")
	parts := strings.Fields(string(content))
	if len(parts) < 1 {
		return "", fmt.Errorf("invalid checksum file format")
	}

	return parts[0], nil
}

// downloadMonth downloads data for a specific month
func (bd *BinanceDownloader) downloadMonth(year, month int) (*DownloadInfo, error) {
	fileName := bd.generateFileName(year, month)
	url := fmt.Sprintf("%s/%s/%s", bd.baseURL, bd.symbol, fileName)
	filePath := filepath.Join(bd.dataDir, "raw", fileName)

	info := &DownloadInfo{
		Symbol:   bd.symbol,
		Interval: "1m",
		Year:     year,
		Month:    month,
		FilePath: filePath,
	}

	// Check if file already exists
	if _, err := os.Stat(filePath); err == nil {
		log.Printf("File already exists: %s", filePath)
		info.Downloaded = true
		
		// Get file size
		if stat, err := os.Stat(filePath); err == nil {
			info.Size = stat.Size()
		}
	} else {
		// Download the file
		if err := bd.downloadFile(url, filePath); err != nil {
			return info, fmt.Errorf("failed to download %s: %w", fileName, err)
		}
		info.Downloaded = true

		// Get file size
		if stat, err := os.Stat(filePath); err == nil {
			info.Size = stat.Size()
		}
	}

	// Download and verify checksum
	expectedChecksum, err := bd.downloadChecksum(year, month)
	if err != nil {
		log.Printf("Warning: failed to download checksum for %s: %v", fileName, err)
		return info, nil // Continue without checksum verification
	}

	info.Checksum = expectedChecksum

	// Verify checksum
	if err := bd.verifyChecksum(filePath, expectedChecksum); err != nil {
		log.Printf("Warning: checksum verification failed for %s: %v", fileName, err)
		return info, nil // Continue even if checksum fails
	}

	info.Verified = true
	return info, nil
}

// downloadRange downloads data for a range of months
func (bd *BinanceDownloader) downloadRange(startYear, startMonth, endYear, endMonth int) ([]*DownloadInfo, error) {
	var downloads []*DownloadInfo
	var errors []string

	currentYear := startYear
	currentMonth := startMonth

	for {
		// Check if we've reached the end
		if currentYear > endYear || (currentYear == endYear && currentMonth > endMonth) {
			break
		}

		log.Printf("Downloading %d-%02d...", currentYear, currentMonth)
		
		info, err := bd.downloadMonth(currentYear, currentMonth)
		if err != nil {
			errorMsg := fmt.Sprintf("Failed to download %d-%02d: %v", currentYear, currentMonth, err)
			log.Printf("Error: %s", errorMsg)
			errors = append(errors, errorMsg)
		} else {
			downloads = append(downloads, info)
		}

		// Move to next month
		currentMonth++
		if currentMonth > 12 {
			currentMonth = 1
			currentYear++
		}
	}

	// Log summary
	log.Printf("Download summary:")
	log.Printf("  Total files: %d", len(downloads))
	log.Printf("  Downloaded: %d", countDownloaded(downloads))
	log.Printf("  Verified: %d", countVerified(downloads))
	log.Printf("  Errors: %d", len(errors))

	if len(errors) > 0 {
		log.Printf("Errors encountered:")
		for _, err := range errors {
			log.Printf("  - %s", err)
		}
	}

	return downloads, nil
}

// countDownloaded counts successfully downloaded files
func countDownloaded(downloads []*DownloadInfo) int {
	count := 0
	for _, d := range downloads {
		if d.Downloaded {
			count++
		}
	}
	return count
}

// countVerified counts successfully verified files
func countVerified(downloads []*DownloadInfo) int {
	count := 0
	for _, d := range downloads {
		if d.Verified {
			count++
		}
	}
	return count
}

// generateDownloadReport creates a summary report of downloads
func (bd *BinanceDownloader) generateDownloadReport(downloads []*DownloadInfo) error {
	reportPath := filepath.Join(bd.dataDir, "download_report.txt")
	
	file, err := os.Create(reportPath)
	if err != nil {
		return fmt.Errorf("failed to create report file: %w", err)
	}
	defer file.Close()

	fmt.Fprintf(file, "Binance Data Download Report\n")
	fmt.Fprintf(file, "Generated: %s\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(file, "Symbol: %s\n", bd.symbol)
	fmt.Fprintf(file, "Interval: 1m\n")
	fmt.Fprintf(file, "Data Directory: %s\n\n", bd.dataDir)

	fmt.Fprintf(file, "Download Summary:\n")
	fmt.Fprintf(file, "  Total files: %d\n", len(downloads))
	fmt.Fprintf(file, "  Downloaded: %d\n", countDownloaded(downloads))
	fmt.Fprintf(file, "  Verified: %d\n", countVerified(downloads))
	fmt.Fprintf(file, "  Failed: %d\n\n", len(downloads)-countDownloaded(downloads))

	fmt.Fprintf(file, "File Details:\n")
	fmt.Fprintf(file, "%-12s %-8s %-12s %-8s %-8s %s\n", "Year-Month", "Size(MB)", "Checksum", "Download", "Verify", "File Path")
	fmt.Fprintf(file, "%s\n", strings.Repeat("-", 80))

	for _, d := range downloads {
		sizeMB := float64(d.Size) / (1024 * 1024)
		downloaded := "✓"
		if !d.Downloaded {
			downloaded = "✗"
		}
		verified := "✓"
		if !d.Verified {
			verified = "✗"
		}
		
		fmt.Fprintf(file, "%-12s %-8.1f %-12s %-8s %-8s %s\n", 
			fmt.Sprintf("%d-%02d", d.Year, d.Month),
			sizeMB,
			d.Checksum[:8]+"...",
			downloaded,
			verified,
			d.FilePath)
	}

	log.Printf("Download report saved to: %s", reportPath)
	return nil
}

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: binance_downloader <data_dir> <start_year> <start_month> [end_year] [end_month]")
	}

	dataDir := os.Args[1]
	startYear, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("Invalid start year: %v", err)
	}

	startMonth, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatalf("Invalid start month: %v", err)
	}

	endYear := startYear
	endMonth := startMonth

	if len(os.Args) >= 5 {
		endYear, err = strconv.Atoi(os.Args[4])
		if err != nil {
			log.Fatalf("Invalid end year: %v", err)
		}
	}

	if len(os.Args) >= 6 {
		endMonth, err = strconv.Atoi(os.Args[5])
		if err != nil {
			log.Fatalf("Invalid end month: %v", err)
		}
	}

	// Validate date range
	if startYear > endYear || (startYear == endYear && startMonth > endMonth) {
		log.Fatal("Invalid date range: start date must be before or equal to end date")
	}

	if startMonth < 1 || startMonth > 12 || endMonth < 1 || endMonth > 12 {
		log.Fatal("Invalid month: must be between 1 and 12")
	}

	// Create downloader
	downloader := NewBinanceDownloader(dataDir)

	// Download data
	downloads, err := downloader.downloadRange(startYear, startMonth, endYear, endMonth)
	if err != nil {
		log.Fatalf("Download failed: %v", err)
	}

	// Generate report
	if err := downloader.generateDownloadReport(downloads); err != nil {
		log.Printf("Warning: failed to generate report: %v", err)
	}

	log.Println("Download process completed!")
}
