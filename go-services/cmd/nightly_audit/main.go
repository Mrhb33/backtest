package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// NightlyAudit performs comprehensive data quality checks
type NightlyAudit struct {
	conn driver.Conn
}

func NewNightlyAudit(clickhouseURL string) (*NightlyAudit, error) {
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

	return &NightlyAudit{conn: conn}, nil
}

func (na *NightlyAudit) Close() error {
	return na.conn.Close()
}

// AuditResult represents the result of an audit check
type AuditResult struct {
	CheckName    string
	Status       string // "PASS", "WARN", "FAIL"
	Message      string
	Details      map[string]interface{}
	CheckedAt    time.Time
}

// runMissingMinuteCheck checks for missing 1-minute bars
func (na *NightlyAudit) runMissingMinuteCheck() (*AuditResult, error) {
	log.Println("Running missing minute check...")

	query := `
		SELECT count() as missing_count
		FROM backtest.find_missing_1m
		WHERE detected_at >= today() - INTERVAL 1 DAY`

	var missingCount uint64
	if err := na.conn.QueryRow(na.conn.Context(), query).Scan(&missingCount); err != nil {
		return nil, fmt.Errorf("failed to check missing minutes: %w", err)
	}

	status := "PASS"
	message := fmt.Sprintf("No missing minutes found")
	if missingCount > 0 {
		status = "FAIL"
		message = fmt.Sprintf("Found %d missing minutes", missingCount)
	}

	return &AuditResult{
		CheckName: "missing_minutes",
		Status:    status,
		Message:   message,
		Details: map[string]interface{}{
			"missing_count": missingCount,
		},
		CheckedAt: time.Now(),
	}, nil
}

// runDuplicateCheck checks for duplicate records
func (na *NightlyAudit) runDuplicateCheck() (*AuditResult, error) {
	log.Println("Running duplicate check...")

	query := `
		SELECT count() as duplicate_count
		FROM backtest.find_duplicates
		WHERE duplicate_count > 1`

	var duplicateCount uint64
	if err := na.conn.QueryRow(na.conn.Context(), query).Scan(&duplicateCount); err != nil {
		return nil, fmt.Errorf("failed to check duplicates: %w", err)
	}

	status := "PASS"
	message := "No duplicates found"
	if duplicateCount > 0 {
		status = "WARN"
		message = fmt.Sprintf("Found %d duplicate groups", duplicateCount)
	}

	return &AuditResult{
		CheckName: "duplicates",
		Status:    status,
		Message:   message,
		Details: map[string]interface{}{
			"duplicate_count": duplicateCount,
		},
		CheckedAt: time.Now(),
	}, nil
}

// runAnomalyCheck checks for data anomalies
func (na *NightlyAudit) runAnomalyCheck() (*AuditResult, error) {
	log.Println("Running anomaly check...")

	query := `
		SELECT 
			anomaly_type,
			severity,
			count() as count
		FROM backtest.data_anomalies
		WHERE detected_at >= today() - INTERVAL 1 DAY
		GROUP BY anomaly_type, severity
		ORDER BY severity DESC, count DESC`

	rows, err := na.conn.Query(na.conn.Context(), query)
	if err != nil {
		return nil, fmt.Errorf("failed to check anomalies: %w", err)
	}
	defer rows.Close()

	anomalies := make(map[string]map[string]uint64)
	totalAnomalies := uint64(0)
	criticalAnomalies := uint64(0)

	for rows.Next() {
		var (
			anomalyType string
			severity    uint8
			count       uint64
		)

		if err := rows.Scan(&anomalyType, &severity, &count); err != nil {
			return nil, fmt.Errorf("failed to scan anomaly row: %w", err)
		}

		if anomalies[anomalyType] == nil {
			anomalies[anomalyType] = make(map[string]uint64)
		}

		severityStr := "low"
		switch severity {
		case 2:
			severityStr = "medium"
		case 3:
			severityStr = "high"
		case 4:
			severityStr = "critical"
			criticalAnomalies += count
		}

		anomalies[anomalyType][severityStr] = count
		totalAnomalies += count
	}

	status := "PASS"
	message := "No anomalies found"
	if totalAnomalies > 0 {
		if criticalAnomalies > 0 {
			status = "FAIL"
			message = fmt.Sprintf("Found %d critical anomalies", criticalAnomalies)
		} else {
			status = "WARN"
			message = fmt.Sprintf("Found %d anomalies", totalAnomalies)
		}
	}

	return &AuditResult{
		CheckName: "anomalies",
		Status:    status,
		Message:   message,
		Details: map[string]interface{}{
			"total_anomalies":    totalAnomalies,
			"critical_anomalies": criticalAnomalies,
			"anomaly_breakdown":  anomalies,
		},
		CheckedAt: time.Now(),
	}, nil
}

// runCompletenessCheck checks daily data completeness
func (na *NightlyAudit) runCompletenessCheck() (*AuditResult, error) {
	log.Println("Running completeness check...")

	query := `
		SELECT 
			day,
			completeness_pct,
			missing_bars
		FROM backtest.daily_completeness
		WHERE day >= today() - INTERVAL 7 DAY
		AND interval = '1m'
		ORDER BY day DESC`

	rows, err := na.conn.Query(na.conn.Context(), query)
	if err != nil {
		return nil, fmt.Errorf("failed to check completeness: %w", err)
	}
	defer rows.Close()

	var incompleteDays []map[string]interface{}
	lowestCompleteness := 100.0

	for rows.Next() {
		var (
			day              time.Time
			completenessPct  float64
			missingBars      uint64
		)

		if err := rows.Scan(&day, &completenessPct, &missingBars); err != nil {
			return nil, fmt.Errorf("failed to scan completeness row: %w", err)
		}

		if completenessPct < lowestCompleteness {
			lowestCompleteness = completenessPct
		}

		if completenessPct < 99.0 { // Less than 99% complete
			incompleteDays = append(incompleteDays, map[string]interface{}{
				"day":              day.Format("2006-01-02"),
				"completeness_pct": completenessPct,
				"missing_bars":     missingBars,
			})
		}
	}

	status := "PASS"
	message := "All days have 99%+ completeness"
	if len(incompleteDays) > 0 {
		status = "WARN"
		message = fmt.Sprintf("Found %d days with <99%% completeness", len(incompleteDays))
	}

	return &AuditResult{
		CheckName: "completeness",
		Status:    status,
		Message:   message,
		Details: map[string]interface{}{
			"lowest_completeness": lowestCompleteness,
			"incomplete_days":     incompleteDays,
		},
		CheckedAt: time.Now(),
	}, nil
}

// runParityCheck runs random parity validation against Binance API
func (na *NightlyAudit) runParityCheck() (*AuditResult, error) {
	log.Println("Running parity check...")

	// Get recent data range
	query := `
		SELECT 
			min(open_time_ms) as min_time,
			max(open_time_ms) as max_time,
			count() as total_bars
		FROM backtest.ohlcv_raw
		WHERE symbol = 'BTCUSDT' AND interval = '1m'
		AND open_time_ms >= today() - INTERVAL 1 DAY`

	var (
		minTime   uint64
		maxTime   uint64
		totalBars uint64
	)

	if err := na.conn.QueryRow(na.conn.Context(), query).Scan(&minTime, &maxTime, &totalBars); err != nil {
		return nil, fmt.Errorf("failed to get data range: %w", err)
	}

	if totalBars == 0 {
		return &AuditResult{
			CheckName: "parity",
			Status:    "WARN",
			Message:   "No recent data found for parity check",
			Details:   map[string]interface{}{},
			CheckedAt: time.Now(),
		}, nil
	}

	// Check recent parity results
	parityQuery := `
		SELECT 
			count() as total_checks,
			sum(is_exact_match) as exact_matches,
			avg(open_diff) as avg_open_diff,
			avg(high_diff) as avg_high_diff,
			avg(low_diff) as avg_low_diff,
			avg(close_diff) as avg_close_diff
		FROM backtest.parity_checks
		WHERE checked_at >= today() - INTERVAL 1 DAY`

	var (
		totalChecks   uint64
		exactMatches  uint64
		avgOpenDiff   float64
		avgHighDiff   float64
		avgLowDiff    float64
		avgCloseDiff  float64
	)

	if err := na.conn.QueryRow(na.conn.Context(), parityQuery).Scan(&totalChecks, &exactMatches, &avgOpenDiff, &avgHighDiff, &avgLowDiff, &avgCloseDiff); err != nil {
		// No parity data available
		return &AuditResult{
			CheckName: "parity",
			Status:    "WARN",
			Message:   "No recent parity check data available",
			Details:   map[string]interface{}{},
			CheckedAt: time.Now(),
		}, nil
	}

	matchRate := float64(exactMatches) / float64(totalChecks) * 100

	status := "PASS"
	message := fmt.Sprintf("Parity check passed: %.2f%% exact matches", matchRate)
	if matchRate < 95.0 {
		status = "FAIL"
		message = fmt.Sprintf("Parity check failed: only %.2f%% exact matches", matchRate)
	} else if matchRate < 99.0 {
		status = "WARN"
		message = fmt.Sprintf("Parity check warning: %.2f%% exact matches", matchRate)
	}

	return &AuditResult{
		CheckName: "parity",
		Status:    status,
		Message:   message,
		Details: map[string]interface{}{
			"total_checks":    totalChecks,
			"exact_matches":   exactMatches,
			"match_rate":      matchRate,
			"avg_open_diff":   avgOpenDiff,
			"avg_high_diff":   avgHighDiff,
			"avg_low_diff":    avgLowDiff,
			"avg_close_diff":  avgCloseDiff,
		},
		CheckedAt: time.Now(),
	}, nil
}

// runDataFreshnessCheck checks if data is being updated regularly
func (na *NightlyAudit) runDataFreshnessCheck() (*AuditResult, error) {
	log.Println("Running data freshness check...")

	query := `
		SELECT 
			max(ingested_at) as last_ingestion,
			count() as recent_bars
		FROM backtest.ohlcv_raw
		WHERE symbol = 'BTCUSDT' AND interval = '1m'
		AND ingested_at >= now() - INTERVAL 1 HOUR`

	var (
		lastIngestion time.Time
		recentBars    uint64
	)

	if err := na.conn.QueryRow(na.conn.Context(), query).Scan(&lastIngestion, &recentBars); err != nil {
		return nil, fmt.Errorf("failed to check data freshness: %w", err)
	}

	timeSinceLastIngestion := time.Since(lastIngestion)

	status := "PASS"
	message := "Data is fresh"
	if timeSinceLastIngestion > 2*time.Hour {
		status = "FAIL"
		message = fmt.Sprintf("Data is stale: last ingestion %v ago", timeSinceLastIngestion)
	} else if timeSinceLastIngestion > 1*time.Hour {
		status = "WARN"
		message = fmt.Sprintf("Data may be stale: last ingestion %v ago", timeSinceLastIngestion)
	}

	return &AuditResult{
		CheckName: "data_freshness",
		Status:    status,
		Message:   message,
		Details: map[string]interface{}{
			"last_ingestion":        lastIngestion,
			"time_since_ingestion":  timeSinceLastIngestion.String(),
			"recent_bars":           recentBars,
		},
		CheckedAt: time.Now(),
	}, nil
}

// runAllChecks runs all audit checks
func (na *NightlyAudit) runAllChecks() ([]*AuditResult, error) {
	var results []*AuditResult

	checks := []func() (*AuditResult, error){
		na.runMissingMinuteCheck,
		na.runDuplicateCheck,
		na.runAnomalyCheck,
		na.runCompletenessCheck,
		na.runParityCheck,
		na.runDataFreshnessCheck,
	}

	for _, check := range checks {
		result, err := check()
		if err != nil {
			log.Printf("Warning: check failed: %v", err)
			result = &AuditResult{
				CheckName: "unknown",
				Status:    "FAIL",
				Message:   fmt.Sprintf("Check failed: %v", err),
				Details:   map[string]interface{}{},
				CheckedAt: time.Now(),
			}
		}
		results = append(results, result)
	}

	return results, nil
}

// generateAuditReport creates a comprehensive audit report
func (na *NightlyAudit) generateAuditReport(results []*AuditResult) error {
	reportPath := "/app/data/nightly_audit_report.txt"
	
	file, err := os.Create(reportPath)
	if err != nil {
		return fmt.Errorf("failed to create audit report: %w", err)
	}
	defer file.Close()

	fmt.Fprintf(file, "Nightly Data Quality Audit Report\n")
	fmt.Fprintf(file, "Generated: %s\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(file, "Database: backtest\n\n")

	// Summary
	passCount := 0
	warnCount := 0
	failCount := 0

	for _, result := range results {
		switch result.Status {
		case "PASS":
			passCount++
		case "WARN":
			warnCount++
		case "FAIL":
			failCount++
		}
	}

	fmt.Fprintf(file, "Summary:\n")
	fmt.Fprintf(file, "  Total checks: %d\n", len(results))
	fmt.Fprintf(file, "  Passed: %d\n", passCount)
	fmt.Fprintf(file, "  Warnings: %d\n", warnCount)
	fmt.Fprintf(file, "  Failed: %d\n\n", failCount)

	// Detailed results
	fmt.Fprintf(file, "Detailed Results:\n")
	fmt.Fprintf(file, "%s\n", "="*80)

	for _, result := range results {
		fmt.Fprintf(file, "\nCheck: %s\n", result.CheckName)
		fmt.Fprintf(file, "Status: %s\n", result.Status)
		fmt.Fprintf(file, "Message: %s\n", result.Message)
		fmt.Fprintf(file, "Checked at: %s\n", result.CheckedAt.Format(time.RFC3339))
		
		if len(result.Details) > 0 {
			fmt.Fprintf(file, "Details:\n")
			for key, value := range result.Details {
				fmt.Fprintf(file, "  %s: %v\n", key, value)
			}
		}
		fmt.Fprintf(file, "%s\n", "-"*40)
	}

	log.Printf("Audit report saved to: %s", reportPath)
	return nil
}

// storeAuditResults stores audit results in the database
func (na *NightlyAudit) storeAuditResults(results []*AuditResult) error {
	query := `
		INSERT INTO backtest.audit_log (
			operation_id, operation_type, table_name, affected_rows,
			operation_details, executed_by, executed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)`

	stmt, err := na.conn.PrepareBatch(na.conn.Context(), query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	operationID := fmt.Sprintf("audit_%d", time.Now().Unix())
	executedBy := "nightly_audit"

	for _, result := range results {
		details := fmt.Sprintf("Check: %s, Status: %s, Message: %s", 
			result.CheckName, result.Status, result.Message)
		
		affectedRows := uint64(0)
		if result.Status == "FAIL" {
			affectedRows = 1 // Flag failed checks
		}

		err := stmt.Append(
			operationID,
			"data_quality_check",
			"ohlcv_raw",
			affectedRows,
			details,
			executedBy,
			result.CheckedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to append audit result: %w", err)
		}
	}

	return stmt.Send()
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: nightly_audit <clickhouse_url>")
	}

	clickhouseURL := os.Args[1]

	// Create audit instance
	audit, err := NewNightlyAudit(clickhouseURL)
	if err != nil {
		log.Fatalf("Failed to create audit instance: %v", err)
	}
	defer audit.Close()

	// Run all checks
	results, err := audit.runAllChecks()
	if err != nil {
		log.Fatalf("Failed to run audit checks: %v", err)
	}

	// Generate report
	if err := audit.generateAuditReport(results); err != nil {
		log.Printf("Warning: failed to generate report: %v", err)
	}

	// Store results
	if err := audit.storeAuditResults(results); err != nil {
		log.Printf("Warning: failed to store audit results: %v", err)
	}

	// Log summary
	passCount := 0
	warnCount := 0
	failCount := 0

	for _, result := range results {
		switch result.Status {
		case "PASS":
			passCount++
		case "WARN":
			warnCount++
		case "FAIL":
			failCount++
		}
	}

	log.Printf("Nightly audit completed: %d passed, %d warnings, %d failed", 
		passCount, warnCount, failCount)

	// Exit with error code if any checks failed
	if failCount > 0 {
		os.Exit(1)
	}
}
