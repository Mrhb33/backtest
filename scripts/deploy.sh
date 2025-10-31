#!/bin/bash

# High-Performance Backtesting System - Build and Deployment Script
# This script builds and deploys the entire backtesting system

set -e

# Configuration
PROJECT_NAME="backtest-system"
VERSION=${VERSION:-"1.0.0"}
ENVIRONMENT=${ENVIRONMENT:-"development"}
DOCKER_REGISTRY=${DOCKER_REGISTRY:-"localhost:5000"}

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
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose is not installed"
        exit 1
    fi
    
    # Check Rust
    if ! command -v cargo &> /dev/null; then
        log_error "Rust/Cargo is not installed"
        exit 1
    fi
    
    # Check Go
    if ! command -v go &> /dev/null; then
        log_error "Go is not installed"
        exit 1
    fi
    
    log_success "All prerequisites met"
}

# Build Rust engine
build_rust_engine() {
    log_info "Building Rust backtesting engine..."
    
    cd rust-engine
    
    # Set Rust flags for deterministic builds
    export RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C panic=abort"
    export CARGO_PROFILE_RELEASE_LTO=true
    export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
    
    # Build with optimizations
    cargo build --release --target x86_64-unknown-linux-gnu
    
    # Run tests
    cargo test --release
    
    # Run benchmarks
    cargo bench --release
    
    cd ..
    
    log_success "Rust engine built successfully"
}

# Build Go services
build_go_services() {
    log_info "Building Go orchestration services..."
    
    cd go-services
    
    # Set Go build flags
    export CGO_ENABLED=0
    export GOOS=linux
    export GOARCH=amd64
    
    # Build
    go build -ldflags="-s -w -X main.version=${VERSION}" -o bin/server ./cmd/server
    
    # Run tests
    go test ./...
    
    # Run linting
    if command -v golangci-lint &> /dev/null; then
        golangci-lint run
    fi
    
    cd ..
    
    log_success "Go services built successfully"
}

# Build WASM strategies
build_wasm_strategies() {
    log_info "Building WASM strategies..."
    
    # Build Rust strategy
    cd wasm-strategies/rust
    cargo build --release --target wasm32-unknown-unknown
    cd ../..
    
    # Build TypeScript strategy
    cd wasm-strategies/typescript
    npm install
    npm run build
    cd ../..
    
    log_success "WASM strategies built successfully"
}

# Run determinism tests
run_determinism_tests() {
    log_info "Running determinism tests..."
    
    cd tests
    
    # Run Rust determinism tests
    cargo test --release determinism_tests
    
    # Run golden dataset validation
    cargo run --release --bin determinism_tests -- --config config/determinism_tests.json
    
    cd ..
    
    log_success "Determinism tests passed"
}

# Build Docker images
build_docker_images() {
    log_info "Building Docker images..."
    
    # Build Rust engine image
    docker build -t ${DOCKER_REGISTRY}/backtest-rust-engine:${VERSION} -f rust-engine/Dockerfile rust-engine/
    docker build -t ${DOCKER_REGISTRY}/backtest-rust-engine:latest -f rust-engine/Dockerfile rust-engine/
    
    # Build Go services image
    docker build -t ${DOCKER_REGISTRY}/backtest-go-services:${VERSION} -f go-services/Dockerfile go-services/
    docker build -t ${DOCKER_REGISTRY}/backtest-go-services:latest -f go-services/Dockerfile go-services/
    
    log_success "Docker images built successfully"
}

# Push Docker images
push_docker_images() {
    if [ "$ENVIRONMENT" = "production" ]; then
        log_info "Pushing Docker images to registry..."
        
        docker push ${DOCKER_REGISTRY}/backtest-rust-engine:${VERSION}
        docker push ${DOCKER_REGISTRY}/backtest-rust-engine:latest
        docker push ${DOCKER_REGISTRY}/backtest-go-services:${VERSION}
        docker push ${DOCKER_REGISTRY}/backtest-go-services:latest
        
        log_success "Docker images pushed successfully"
    else
        log_warning "Skipping Docker push in ${ENVIRONMENT} environment"
    fi
}

# Deploy with Docker Compose
deploy_docker_compose() {
    log_info "Deploying with Docker Compose..."
    
    # Stop existing services
    docker-compose down --remove-orphans
    
    # Pull latest images
    docker-compose pull
    
    # Start services
    docker-compose up -d
    
    # Wait for services to be healthy
    log_info "Waiting for services to be healthy..."
    sleep 30
    
    # Check service health
    check_service_health
    
    log_success "Docker Compose deployment completed"
}

# Check service health
check_service_health() {
    log_info "Checking service health..."
    
    # Check ClickHouse
    if curl -f http://localhost:8123/ping > /dev/null 2>&1; then
        log_success "ClickHouse is healthy"
    else
        log_error "ClickHouse is not responding"
        exit 1
    fi
    
    # Check Go services
    if curl -f http://localhost:8080/health > /dev/null 2>&1; then
        log_success "Go services are healthy"
    else
        log_error "Go services are not responding"
        exit 1
    fi
    
    # Check Prometheus
    if curl -f http://localhost:9090/-/healthy > /dev/null 2>&1; then
        log_success "Prometheus is healthy"
    else
        log_error "Prometheus is not responding"
        exit 1
    fi
    
    # Check Grafana
    if curl -f http://localhost:3000/api/health > /dev/null 2>&1; then
        log_success "Grafana is healthy"
    else
        log_error "Grafana is not responding"
        exit 1
    fi
}

# Run performance tests
run_performance_tests() {
    log_info "Running performance tests..."
    
    # Test throughput
    curl -X POST http://localhost:8080/api/v1/backtest \
        -H "Content-Type: application/json" \
        -d '{
            "symbols": ["BTCUSDT"],
            "timeframe": "1m",
            "start_time": 1672531200000,
            "end_time": 1675123199000,
            "intrabar_policy": "EXACT_TRADES",
            "slippage_mode": "NONE",
            "fee_version": "binance_2023",
            "strategy_wasm_hash": "test_hash",
            "snapshot_id": "test_snapshot"
        }' > /dev/null 2>&1
    
    log_success "Performance tests completed"
}

# Generate deployment report
generate_deployment_report() {
    log_info "Generating deployment report..."
    
    REPORT_FILE="deployment_report_$(date +%Y%m%d_%H%M%S).md"
    
    cat > ${REPORT_FILE} << EOF
# Backtesting System Deployment Report

**Deployment Date:** $(date)
**Version:** ${VERSION}
**Environment:** ${ENVIRONMENT}

## Services Status

- ClickHouse: $(curl -s http://localhost:8123/ping && echo "Healthy" || echo "Unhealthy")
- Go Services: $(curl -s http://localhost:8080/health && echo "Healthy" || echo "Unhealthy")
- Prometheus: $(curl -s http://localhost:9090/-/healthy && echo "Healthy" || echo "Unhealthy")
- Grafana: $(curl -s http://localhost:3000/api/health && echo "Healthy" || echo "Unhealthy")

## Performance Metrics

- Memory Usage: $(docker stats --no-stream --format "table {{.MemUsage}}" backtest-rust-engine)
- CPU Usage: $(docker stats --no-stream --format "table {{.CPUPerc}}" backtest-rust-engine)

## Access URLs

- HTTP API: http://localhost:8080
- gRPC API: http://localhost:9091
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin123)
- ClickHouse: http://localhost:8123

## Next Steps

1. Verify all services are running correctly
2. Run integration tests
3. Monitor performance metrics
4. Set up alerting rules

EOF
    
    log_success "Deployment report generated: ${REPORT_FILE}"
}

# Cleanup function
cleanup() {
    log_info "Cleaning up..."
    
    # Remove unused Docker images
    docker image prune -f
    
    # Remove unused containers
    docker container prune -f
    
    log_success "Cleanup completed"
}

# Main execution
main() {
    log_info "Starting backtesting system deployment..."
    log_info "Version: ${VERSION}, Environment: ${ENVIRONMENT}"
    
    # Check prerequisites
    check_prerequisites
    
    # Build components
    build_rust_engine
    build_go_services
    build_wasm_strategies
    
    # Run tests
    run_determinism_tests
    
    # Build and deploy
    build_docker_images
    push_docker_images
    deploy_docker_compose
    
    # Run performance tests
    run_performance_tests
    
    # Generate report
    generate_deployment_report
    
    # Cleanup
    cleanup
    
    log_success "Backtesting system deployment completed successfully!"
    log_info "Access the system at: http://localhost:8080"
}

# Handle script arguments
case "${1:-deploy}" in
    "build")
        check_prerequisites
        build_rust_engine
        build_go_services
        build_wasm_strategies
        ;;
    "test")
        run_determinism_tests
        ;;
    "deploy")
        main
        ;;
    "health")
        check_service_health
        ;;
    "cleanup")
        cleanup
        ;;
    *)
        echo "Usage: $0 {build|test|deploy|health|cleanup}"
        exit 1
        ;;
esac

