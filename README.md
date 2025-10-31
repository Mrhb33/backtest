# High-Performance Backtesting System

A deterministic, high-performance backtesting engine built with Rust, Go, and ClickHouse.

## Architecture

- **Core Compute**: Rust (SIMD indicators, intrabar simulator, fills)
- **Orchestration**: Go (gRPC/HTTP, NATS control plane, ClickHouse I/O)
- **Data Store**: ClickHouse (raw trades, OHLCV, reference tables)
- **Strategy Sandbox**: WASM (Rust/TypeScript compiled to WASM)
- **Hot Transfer**: Apache Arrow IPC (columnar batches)

## Key Features

- Deterministic floating-point arithmetic (IEEE-754 FP64, nearest-even rounding)
- Decimal128 precision for money/fees/quantities
- SIMD-optimized indicator kernels
- Intrabar simulation with exact trade paths or 1s bar interpolation
- Exchange simulation with maker/taker fees and slippage models
- WASM strategy sandbox for safe, deterministic execution
- Full audit trail with run manifests and reproducibility

## Performance Targets

- Indicators: ≥10-20M ops/s/core
- Full simulation: ≥50-100k bars/ms/core
- 5-year backtest @1m: ~few hundred ms per symbol

## Directory Structure

```
├── schemas/          # ClickHouse DDL and views
├── rust-engine/      # Core Rust engine with SIMD kernels
├── go-services/      # Go orchestration and API services
├── wasm-strategies/  # WASM strategy examples and ABI
├── tests/           # Determinism tests and golden datasets
├── manifests/       # Run manifests and versioning
└── monitoring/      # Prometheus/OpenTelemetry configs
```

## Getting Started

1. Set up ClickHouse with provided schemas
2. Build Rust engine: `cd rust-engine && cargo build --release`
3. Build Go services: `cd go-services && go build`
4. Run determinism tests to verify correctness
5. Execute backtests via gRPC API

## Determinism Guarantees

- Fixed floating-point rounding modes
- Deterministic random seeds (PCG)
- Immutable data snapshots
- Versioned engine and strategy artifacts
- Complete audit trail with SHA hashes

