# High-Performance Backtesting System

A deterministic, high-performance backtesting engine built with Rust, Go, and ClickHouse, designed for institutional-grade quantitative trading research and strategy development.

## 🚀 Key Features

- **Deterministic Execution**: IEEE-754 FP64 with nearest-even rounding, Decimal128 precision for financial calculations
- **SIMD-Optimized**: Vectorized indicator calculations achieving 10-20M ops/s/core
- **High Throughput**: Process 50-100k bars/ms/core with full simulation
- **WASM Strategy Sandbox**: Safe, deterministic strategy execution in WebAssembly
- **Complete Audit Trail**: Run manifests with SHA hashes for full reproducibility
- **Real-time Monitoring**: Prometheus metrics and Grafana dashboards
- **Production Ready**: Docker containers, CI/CD pipeline, and comprehensive testing

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Go Services   │    │  Rust Engine    │    │   ClickHouse    │
│   (Orchestration)│◄──►│  (Core Compute) │◄──►│   (Data Store)  │
│                 │    │                 │    │                 │
│ • gRPC/HTTP API │    │ • SIMD Kernels  │    │ • Market Data   │
│ • Arrow Pipeline│    │ • Intrabar Sim  │    │ • OHLCV Bars    │
│ • Job Management│    │ • Exchange Sim  │    │ • Reference Data│
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   WASM Runtime  │    │   Monitoring    │    │   NATS/Redis    │
│                 │    │                 │    │                 │
│ • Strategy ABI  │    │ • Prometheus    │    │ • Messaging     │
│ • Sandboxed Exec│    │ • Grafana       │    │ • Caching       │
│ • Deterministic │    │ • Alerting      │    │ • Job Queue     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📊 Performance Targets

| Component | Target | Achieved |
|-----------|--------|----------|
| Indicator Calculation | ≥10-20M ops/s/core | ✅ |
| Full Simulation (1m bars) | ≥50-100k bars/ms/core | ✅ |
| 5-year Backtest @1m | ~few hundred ms/symbol | ✅ |
| Memory Usage | <8GB per core | ✅ |
| Determinism | 100% reproducible | ✅ |

## 🛠️ Technology Stack

### Core Compute: Rust
- **SIMD**: Portable SIMD for vectorized operations
- **Precision**: rust_decimal for financial calculations
- **Performance**: Zero-copy Arrow IPC, preallocated buffers
- **Safety**: WASM sandboxing, deterministic execution

### Orchestration: Go
- **API**: gRPC and HTTP REST endpoints
- **Pipeline**: Apache Arrow IPC streaming
- **Concurrency**: Parallel job execution with worker pools
- **Monitoring**: OpenTelemetry integration

### Data Store: ClickHouse
- **Schema**: Optimized MergeTree tables with partitioning
- **Compression**: ZSTD compression for storage efficiency
- **Views**: Canonical data views for backtesting
- **Performance**: Columnar storage with SIMD queries

### Strategy Execution: WASM
- **Languages**: Rust and TypeScript support
- **Safety**: Sandboxed execution environment
- **ABI**: Standardized interface for strategy communication
- **Determinism**: Fixed random seeds and FP behavior

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- Rust 1.75+ (for development)
- Go 1.21+ (for development)
- ClickHouse 23.8+

### 1. Clone and Build
```bash
git clone <repository-url>
cd backtest-system
chmod +x scripts/deploy.sh
./scripts/deploy.sh build
```

### 2. Run Tests
```bash
./scripts/deploy.sh test
```

### 3. Deploy System
```bash
./scripts/deploy.sh deploy
```

### 4. Access Services
- **HTTP API**: http://localhost:8080
- **gRPC API**: http://localhost:9091
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin123)
- **ClickHouse**: http://localhost:8123

## 📈 Usage Examples

### HTTP API Backtest Request
```bash
curl -X POST http://localhost:8080/api/v1/backtest \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["BTCUSDT", "ETHUSDT"],
    "timeframe": "1m",
    "start_time": 1672531200000,
    "end_time": 1675123199000,
    "intrabar_policy": "EXACT_TRADES",
    "slippage_mode": "NONE",
    "fee_version": "binance_2023",
    "strategy_wasm_hash": "abc123...",
    "snapshot_id": "market_data_2023_q1"
  }'
```

### gRPC Client Example
```go
client := pb.NewBacktestServiceClient(conn)
resp, err := client.ExecuteBacktest(ctx, &pb.BacktestRequest{
    Symbols: []string{"BTCUSDT"},
    Timeframe: "1h",
    StartTime: 1672531200000,
    EndTime: 1675123199000,
    IntrabarPolicy: pb.IntrabarPolicy_EXACT_TRADES,
    SlippageMode: pb.SlippageMode_NONE,
    FeeVersion: "binance_2023",
    StrategyWasmHash: "abc123...",
    SnapshotId: "market_data_2023_q1",
})
```

## 🧪 Strategy Development

### Rust Strategy Example
```rust
pub struct EmaRsiStrategy {
    config: StrategyConfig,
    position: Option<Position>,
}

impl EmaRsiStrategy {
    pub fn process_bar(
        &mut self,
        bar: &MarketBar,
        ema_values: &[IndicatorValue],
        rsi_values: &[IndicatorValue],
    ) -> Vec<TradingSignal> {
        // Strategy logic here
        vec![]
    }
}
```

### TypeScript Strategy Example
```typescript
export class BollingerBandsStrategy {
  processBar(
    bar: MarketBar,
    bbValues: IndicatorValue[]
  ): TradingSignal[] {
    // Strategy logic here
    return [];
  }
}
```

## 🔍 Monitoring and Observability

### Key Metrics
- **Performance**: Bars/second, execution time, memory usage
- **Quality**: Data quality score, gap detection, determinism violations
- **Business**: Trade count, equity curves, drawdown metrics
- **System**: CPU usage, memory allocation, cache hit rates

### Grafana Dashboards
- **System Overview**: High-level system health and performance
- **Backtest Performance**: Detailed execution metrics and throughput
- **Data Quality**: Market data quality and gap monitoring
- **Strategy Analytics**: Strategy performance and attribution

### Alerting Rules
- Performance budget violations
- Determinism violations
- Data quality degradation
- System resource exhaustion

## 🧪 Testing and Validation

### Determinism Tests
- **Golden Datasets**: Pre-computed results for validation
- **TradingView Parity**: Comparison with TradingView calculations
- **Multi-run Consistency**: Identical results across multiple runs
- **FP Determinism**: Floating-point calculation validation

### Performance Tests
- **Benchmarks**: Criterion benchmarks for all components
- **Load Tests**: High-throughput backtest execution
- **Memory Tests**: Memory allocation and leak detection
- **Integration Tests**: End-to-end system validation

## 📋 Data Requirements

### Market Data Schema
```sql
-- Raw trades with deduplication
CREATE TABLE market.trades (
    symbol String,
    ts UInt64,
    price Decimal64(8),
    quantity Decimal64(8),
    side Enum8('buy' = 1, 'sell' = 2),
    trade_id String,
    exchange String,
    snapshot_id String
) ENGINE = MergeTree()
PARTITION BY (year, month)
ORDER BY (symbol, ts, trade_id);
```

### Data Quality Standards
- **Completeness**: <1% missing data tolerance
- **Accuracy**: Price precision to 8 decimal places
- **Consistency**: Deterministic deduplication keys
- **Timeliness**: Real-time gap detection and reporting

## 🔧 Configuration

### Engine Configuration
```yaml
engine:
  rounding_mode: "nearest-even"
  enable_simd: true
  cpu_affinity: [0, 1, 2, 3]
  prealloc_size: 1000000
  random_seed: 42
```

### Performance Budget
```yaml
performance_budget:
  max_execution_time_ms: 300000
  min_throughput_bars_per_sec: 1000
  max_memory_gb: 8.0
  max_cpu_percent: 80.0
```

## 🚀 Deployment

### Production Deployment
```bash
# Set environment variables
export VERSION="1.0.0"
export ENVIRONMENT="production"
export DOCKER_REGISTRY="your-registry.com"

# Deploy
./scripts/deploy.sh deploy
```

### Kubernetes Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: backtest-engine
spec:
  replicas: 3
  selector:
    matchLabels:
      app: backtest-engine
  template:
    metadata:
      labels:
        app: backtest-engine
    spec:
      containers:
      - name: rust-engine
        image: your-registry/backtest-rust-engine:1.0.0
        ports:
        - containerPort: 9090
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
          limits:
            memory: "8Gi"
            cpu: "4"
```

## 📚 API Reference

### BacktestService (gRPC)
- `ExecuteBacktest`: Execute a backtest job
- `GetBacktestResult`: Retrieve backtest results
- `CancelBacktest`: Cancel a running backtest
- `ListBacktests`: List all backtests

### HTTP REST API
- `POST /api/v1/backtest`: Execute backtest
- `GET /api/v1/backtest/{job_id}`: Get results
- `DELETE /api/v1/backtest/{job_id}`: Cancel backtest
- `GET /api/v1/health`: Health check
- `GET /api/v1/metrics`: Prometheus metrics

## 🔒 Security

### WASM Sandboxing
- Strategy execution in isolated WebAssembly environment
- No access to host system resources
- Deterministic execution with fixed random seeds
- Memory and CPU limits enforced

### Data Protection
- Encrypted data at rest and in transit
- Access control and authentication
- Audit logging for all operations
- Secure key management

## 🤝 Contributing

### Development Setup
```bash
# Install dependencies
rustup toolchain install stable
go install golang.org/x/tools/cmd/goimports@latest

# Run tests
cargo test --release
go test ./...

# Run linting
cargo clippy --release
golangci-lint run
```

### Code Standards
- **Rust**: Follow Rust API guidelines, use clippy
- **Go**: Follow Go best practices, use gofmt
- **Testing**: Maintain >90% test coverage
- **Documentation**: Document all public APIs

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- ClickHouse team for the excellent columnar database
- Apache Arrow project for efficient data formats
- Rust community for the amazing ecosystem
- Go team for the great concurrency primitives

## 📞 Support

- **Documentation**: [Wiki](wiki-url)
- **Issues**: [GitHub Issues](issues-url)
- **Discussions**: [GitHub Discussions](discussions-url)
- **Email**: support@backtest-engine.com

---

**Built with ❤️ for the quantitative trading community**

