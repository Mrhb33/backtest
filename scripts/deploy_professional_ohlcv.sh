#!/bin/bash

# Professional OHLCV Pipeline Deployment Script
# Implements Binance-parity data pipeline with comprehensive validation

set -e

# Configuration
CLICKHOUSE_URL="${CLICKHOUSE_URL:-localhost:9000}"
DATA_DIR="${DATA_DIR:-/app/data}"
SYMBOL="${SYMBOL:-BTCUSDT}"
INTERVAL="${INTERVAL:-1m}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if ClickHouse is running
    if ! curl -s "http://${CLICKHOUSE_URL}/ping" > /dev/null; then
        log_error "ClickHouse is not running at ${CLICKHOUSE_URL}"
        exit 1
    fi
    
    # Check if Docker is running
    if ! docker info > /dev/null 2>&1; then
        log_error "Docker is not running"
        exit 1
    fi
    
    # Check if required directories exist
    mkdir -p "${DATA_DIR}/raw"
    mkdir -p "${DATA_DIR}/checksums"
    mkdir -p "${DATA_DIR}/reports"
    
    log_success "Prerequisites check passed"
}

# Deploy schema
deploy_schema() {
    log_info "Deploying professional OHLCV schema..."
    
    # Apply the new schema
    if docker compose exec clickhouse clickhouse-client --query "$(cat schemas/professional_ohlcv_schema.sql)"; then
        log_success "Schema deployed successfully"
    else
        log_error "Failed to deploy schema"
        exit 1
    fi
}

# Build Go services
build_services() {
    log_info "Building Go services..."
    
    # Build data ingest service
    if docker compose exec go-services go build -o /app/data_ingest ./cmd/data_ingest; then
        log_success "Data ingest service built"
    else
        log_error "Failed to build data ingest service"
        exit 1
    fi
    
    # Build Binance downloader
    if docker compose exec go-services go build -o /app/binance_downloader ./cmd/binance_downloader; then
        log_success "Binance downloader built"
    else
        log_error "Failed to build Binance downloader"
        exit 1
    fi
    
    # Build parity checker
    if docker compose exec go-services go build -o /app/parity_checker ./cmd/parity_checker; then
        log_success "Parity checker built"
    else
        log_error "Failed to build parity checker"
        exit 1
    fi
    
    # Build nightly audit
    if docker compose exec go-services go build -o /app/nightly_audit ./cmd/nightly_audit; then
        log_success "Nightly audit built"
    else
        log_error "Failed to build nightly audit"
        exit 1
    fi
}

# Download Binance data
download_binance_data() {
    local start_year=${1:-2024}
    local start_month=${2:-1}
    local end_year=${3:-2025}
    local end_month=${4:-12}
    
    log_info "Downloading Binance data for ${SYMBOL} from ${start_year}-${start_month} to ${end_year}-${end_month}..."
    
    if docker compose exec go-services /app/binance_downloader \
        "${DATA_DIR}" \
        "${start_year}" \
        "${start_month}" \
        "${end_year}" \
        "${end_month}"; then
        log_success "Binance data download completed"
    else
        log_error "Failed to download Binance data"
        exit 1
    fi
}

# Ingest data
ingest_data() {
    log_info "Starting data ingestion pipeline..."
    
    # Find all CSV files in the raw directory
    local csv_files
    csv_files=$(docker compose exec go-services find "${DATA_DIR}/raw" -name "*.csv" -o -name "*.csv.gz" | head -10)
    
    if [ -z "$csv_files" ]; then
        log_warning "No CSV files found in ${DATA_DIR}/raw"
        return 0
    fi
    
    # Process each file
    echo "$csv_files" | while read -r file; do
        if [ -n "$file" ]; then
            log_info "Processing file: $file"
            if docker compose exec go-services /app/data_ingest "$file" "${CLICKHOUSE_URL}"; then
                log_success "Processed: $file"
            else
                log_error "Failed to process: $file"
            fi
        fi
    done
    
    log_success "Data ingestion pipeline completed"
}

# Run data quality checks
run_quality_checks() {
    log_info "Running comprehensive data quality checks..."
    
    # Check for missing minutes
    log_info "Checking for missing minutes..."
    if docker compose exec clickhouse clickhouse-client --query "
        SELECT count() as missing_count 
        FROM backtest.find_missing_1m" | grep -q "0"; then
        log_success "No missing minutes found"
    else
        log_warning "Missing minutes detected"
    fi
    
    # Check for duplicates
    log_info "Checking for duplicates..."
    if docker compose exec clickhouse clickhouse-client --query "
        SELECT count() as duplicate_count 
        FROM backtest.find_duplicates" | grep -q "0"; then
        log_success "No duplicates found"
    else
        log_warning "Duplicates detected"
    fi
    
    # Check for anomalies
    log_info "Checking for data anomalies..."
    local anomaly_count
    anomaly_count=$(docker compose exec clickhouse clickhouse-client --query "
        SELECT count() 
        FROM backtest.detect_anomalies" | tr -d '\n')
    
    if [ "$anomaly_count" -eq 0 ]; then
        log_success "No anomalies found"
    else
        log_warning "Found $anomaly_count anomalies"
    fi
    
    # Check daily completeness
    log_info "Checking daily completeness..."
    docker compose exec clickhouse clickhouse-client --query "
        SELECT 
            day,
            completeness_pct,
            missing_bars
        FROM backtest.daily_completeness
        WHERE interval = '1m' AND day >= today() - INTERVAL 7 DAY
        ORDER BY day DESC
        LIMIT 10"
    
    log_success "Data quality checks completed"
}

# Run parity validation
run_parity_validation() {
    local sample_count=${1:-100}
    
    log_info "Running parity validation with $sample_count random samples..."
    
    # Get recent data range
    local time_range
    time_range=$(docker compose exec clickhouse clickhouse-client --query "
        SELECT 
            min(open_time_ms) as min_time,
            max(open_time_ms) as max_time
        FROM backtest.ohlcv_raw
        WHERE symbol = '${SYMBOL}' AND interval = '1m'
        AND open_time_ms >= today() - INTERVAL 1 DAY")
    
    if [ -z "$time_range" ] || [ "$time_range" = "0\t0" ]; then
        log_warning "No recent data found for parity validation"
        return 0
    fi
    
    # Extract time range
    local min_time max_time
    min_time=$(echo "$time_range" | cut -f1)
    max_time=$(echo "$time_range" | cut -f2)
    
    if [ "$min_time" -eq 0 ] || [ "$max_time" -eq 0 ]; then
        log_warning "Invalid time range for parity validation"
        return 0
    fi
    
    # Run parity checker
    if docker compose exec go-services /app/parity_checker \
        "${CLICKHOUSE_URL}" \
        "${min_time}" \
        "${max_time}" \
        "${sample_count}"; then
        log_success "Parity validation completed"
    else
        log_warning "Parity validation failed"
    fi
}

# Generate comprehensive report
generate_report() {
    log_info "Generating comprehensive data quality report..."
    
    local report_file="${DATA_DIR}/reports/data_quality_report_$(date +%Y%m%d_%H%M%S).txt"
    
    {
        echo "Professional OHLCV Data Quality Report"
        echo "Generated: $(date)"
        echo "Symbol: ${SYMBOL}"
        echo "Interval: ${INTERVAL}"
        echo "ClickHouse URL: ${CLICKHOUSE_URL}"
        echo ""
        
        echo "=== DATA SUMMARY ==="
        docker compose exec clickhouse clickhouse-client --query "
            SELECT 
                symbol,
                interval,
                count() as total_bars,
                min(toDateTime(open_time_ms/1000)) as first_bar,
                max(toDateTime(open_time_ms/1000)) as last_bar
            FROM backtest.ohlcv_raw
            GROUP BY symbol, interval
            ORDER BY symbol, interval"
        
        echo ""
        echo "=== MISSING MINUTES ==="
        docker compose exec clickhouse clickhouse-client --query "
            SELECT count() as missing_count
            FROM backtest.find_missing_1m"
        
        echo ""
        echo "=== DUPLICATES ==="
        docker compose exec clickhouse clickhouse-client --query "
            SELECT count() as duplicate_count
            FROM backtest.find_duplicates"
        
        echo ""
        echo "=== ANOMALIES ==="
        docker compose exec clickhouse clickhouse-client --query "
            SELECT 
                anomaly_type,
                count() as count
            FROM backtest.detect_anomalies
            GROUP BY anomaly_type
            ORDER BY count DESC"
        
        echo ""
        echo "=== DAILY COMPLETENESS (Last 7 Days) ==="
        docker compose exec clickhouse clickhouse-client --query "
            SELECT 
                day,
                completeness_pct,
                missing_bars
            FROM backtest.daily_completeness
            WHERE interval = '1m' AND day >= today() - INTERVAL 7 DAY
            ORDER BY day DESC"
        
        echo ""
        echo "=== PARITY CHECK RESULTS ==="
        docker compose exec clickhouse clickhouse-client --query "
            SELECT 
                count() as total_checks,
                sum(is_exact_match) as exact_matches,
                round(sum(is_exact_match) / count() * 100, 2) as match_rate
            FROM backtest.parity_checks
            WHERE checked_at >= today() - INTERVAL 1 DAY"
        
    } > "$report_file"
    
    log_success "Report generated: $report_file"
}

# Setup nightly audit cron job
setup_nightly_audit() {
    log_info "Setting up nightly audit cron job..."
    
    # Create audit script
    cat > "${DATA_DIR}/nightly_audit.sh" << 'EOF'
#!/bin/bash
# Nightly audit script for OHLCV data quality

CLICKHOUSE_URL="${CLICKHOUSE_URL:-localhost:9000}"
LOG_FILE="/app/data/reports/nightly_audit_$(date +%Y%m%d).log"

echo "Starting nightly audit at $(date)" >> "$LOG_FILE"

# Run nightly audit
if docker compose exec go-services /app/nightly_audit "$CLICKHOUSE_URL" >> "$LOG_FILE" 2>&1; then
    echo "Nightly audit completed successfully at $(date)" >> "$LOG_FILE"
else
    echo "Nightly audit failed at $(date)" >> "$LOG_FILE"
    exit 1
fi
EOF
    
    chmod +x "${DATA_DIR}/nightly_audit.sh"
    
    # Add to crontab (runs at 2 AM daily)
    (crontab -l 2>/dev/null; echo "0 2 * * * ${DATA_DIR}/nightly_audit.sh") | crontab -
    
    log_success "Nightly audit cron job configured"
}

# Main deployment function
main() {
    local action=${1:-"full"}
    local start_year=${2:-2024}
    local start_month=${3:-1}
    local end_year=${4:-2025}
    local end_month=${5:-12}
    local sample_count=${6:-100}
    
    log_info "Starting professional OHLCV pipeline deployment..."
    log_info "Action: $action"
    log_info "Data range: ${start_year}-${start_month} to ${end_year}-${end_month}"
    log_info "Parity samples: $sample_count"
    
    case $action in
        "schema")
            check_prerequisites
            deploy_schema
            ;;
        "build")
            build_services
            ;;
        "download")
            check_prerequisites
            download_binance_data "$start_year" "$start_month" "$end_year" "$end_month"
            ;;
        "ingest")
            check_prerequisites
            ingest_data
            ;;
        "quality")
            run_quality_checks
            ;;
        "parity")
            run_parity_validation "$sample_count"
            ;;
        "report")
            generate_report
            ;;
        "audit")
            setup_nightly_audit
            ;;
        "full")
            check_prerequisites
            deploy_schema
            build_services
            download_binance_data "$start_year" "$start_month" "$end_year" "$end_month"
            ingest_data
            run_quality_checks
            run_parity_validation "$sample_count"
            generate_report
            setup_nightly_audit
            ;;
        *)
            echo "Usage: $0 {schema|build|download|ingest|quality|parity|report|audit|full} [start_year] [start_month] [end_year] [end_month] [sample_count]"
            echo ""
            echo "Actions:"
            echo "  schema   - Deploy ClickHouse schema only"
            echo "  build    - Build Go services only"
            echo "  download - Download Binance data only"
            echo "  ingest   - Run data ingestion only"
            echo "  quality  - Run data quality checks only"
            echo "  parity   - Run parity validation only"
            echo "  report   - Generate quality report only"
            echo "  audit    - Setup nightly audit only"
            echo "  full     - Run complete pipeline (default)"
            echo ""
            echo "Examples:"
            echo "  $0 full 2024 1 2025 12 100"
            echo "  $0 download 2024 1 2024 12"
            echo "  $0 quality"
            exit 1
            ;;
    esac
    
    log_success "Professional OHLCV pipeline deployment completed!"
}

# Run main function with all arguments
main "$@"
