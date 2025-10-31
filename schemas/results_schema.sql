-- ClickHouse schemas for backtest results

CREATE DATABASE IF NOT EXISTS results;

-- Orders table
CREATE TABLE IF NOT EXISTS results.orders (
    job_id String,
    symbol String,
    order_id String,
    order_type String,
    side String,
    price Float64,
    quantity Float64,
    timestamp UInt64,
    status String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (job_id, symbol, timestamp);

-- Fills table
CREATE TABLE IF NOT EXISTS results.fills (
    job_id String,
    symbol String,
    order_id String,
    fill_id String,
    price Float64,
    quantity Float64,
    fee Float64,
    slippage Float64,
    timestamp UInt64,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (job_id, symbol, timestamp);

-- Positions table
CREATE TABLE IF NOT EXISTS results.positions (
    job_id String,
    symbol String,
    timestamp UInt64,
    side String,
    quantity Float64,
    avg_price Float64,
    unrealized_pnl Float64,
    realized_pnl Float64,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (job_id, symbol, timestamp);

-- Equity curve table
CREATE TABLE IF NOT EXISTS results.equity_curve (
    job_id String,
    symbol String,
    timestamp UInt64,
    equity Float64,
    drawdown Float64,
    exposure Float64,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (job_id, symbol, timestamp);

-- Trade attribution table
CREATE TABLE IF NOT EXISTS results.trade_attribution (
    job_id String,
    symbol String,
    trade_id String,
    mfe Float64,
    mae Float64,
    duration_ms UInt64,
    entry_reason String,
    exit_reason String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (job_id, symbol, trade_id);

-- Run manifests table
CREATE TABLE IF NOT EXISTS results.run_manifests (
    job_id String,
    snapshot_id String,
    engine_version String,
    strategy_hash String,
    intrabar_policy String,
    fee_version String,
    slippage_mode String,
    cpu_features Array(String),
    fp_flags String,
    created_at UInt64,
    created_at_dt DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (job_id, created_at);
