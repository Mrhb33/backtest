-- ClickHouse Schema Definitions for High-Performance Backtesting System
-- Optimized for deterministic, high-throughput backtesting with proper partitioning

-- Raw trades table with MergeTree for high-performance inserts and queries
CREATE TABLE IF NOT EXISTS backtest.trades (
    symbol String,
    ts UInt64,                    -- Unix timestamp in milliseconds
    price Decimal64(8),           -- Price with 8 decimal precision
    quantity Decimal64(8),        -- Quantity with 8 decimal precision
    side Enum8('buy' = 1, 'sell' = 2),
    trade_id String,             -- Exchange trade ID for deduplication
    exchange String,             -- Exchange identifier
    snapshot_id String,          -- Data snapshot identifier for reproducibility
    
    -- Metadata for partitioning and ordering
    date Date MATERIALIZED toDate(ts / 1000),
    month UInt8 MATERIALIZED toMonth(date),
    year UInt16 MATERIALIZED toYear(date)
) ENGINE = MergeTree()
PARTITION BY (year, month)
ORDER BY (symbol, ts, trade_id)
SETTINGS 
    index_granularity = 8192,
    merge_with_ttl_timeout = 86400;

-- 1-second OHLCV bars with SummingMergeTree for efficient aggregation
CREATE TABLE IF NOT EXISTS backtest.ohlcv_1s (
    symbol String,
    ts UInt64,                    -- Unix timestamp in milliseconds (aligned to 1s)
    open Decimal64(8),
    high Decimal64(8),
    low Decimal64(8),
    close Decimal64(8),
    volume Decimal64(8),
    trade_count UInt32,
    snapshot_id String,
    
    -- Metadata
    date Date MATERIALIZED toDate(ts / 1000),
    month UInt8 MATERIALIZED toMonth(date),
    year UInt16 MATERIALIZED toYear(date)
) ENGINE = SummingMergeTree()
PARTITION BY (year, month)
ORDER BY (symbol, ts)
SETTINGS 
    index_granularity = 8192;

-- 1-minute OHLCV bars with SummingMergeTree
CREATE TABLE IF NOT EXISTS backtest.ohlcv_1m (
    symbol String,
    ts UInt64,                    -- Unix timestamp in milliseconds (aligned to 1m)
    open Decimal64(8),
    high Decimal64(8),
    low Decimal64(8),
    close Decimal64(8),
    volume Decimal64(8),
    trade_count UInt32,
    snapshot_id String,
    
    -- Metadata
    date Date MATERIALIZED toDate(ts / 1000),
    month UInt8 MATERIALIZED toMonth(date),
    year UInt16 MATERIALIZED toYear(date)
) ENGINE = SummingMergeTree()
PARTITION BY (year, month)
ORDER BY (symbol, ts)
SETTINGS 
    index_granularity = 8192;

-- Exchange rules and trading parameters
CREATE TABLE IF NOT EXISTS backtest.ref_exchange_rules (
    symbol String,
    exchange String,
    tick_size Decimal64(8),       -- Minimum price increment
    lot_size Decimal64(8),        -- Minimum quantity increment
    min_notional Decimal64(8),    -- Minimum order value
    maker_fee Decimal64(8),       -- Maker fee rate (e.g., 0.001 = 0.1%)
    taker_fee Decimal64(8),       -- Taker fee rate
    precision_price UInt8,        -- Price decimal places
    precision_quantity UInt8,     -- Quantity decimal places
    effective_from UInt64,        -- Unix timestamp when rules become effective
    effective_to UInt64 DEFAULT 0, -- Unix timestamp when rules expire (0 = active)
    snapshot_id String,
    
    -- Metadata
    date Date MATERIALIZED toDate(effective_from / 1000),
    month UInt8 MATERIALIZED toMonth(date),
    year UInt16 MATERIALIZED toYear(date)
) ENGINE = ReplacingMergeTree(effective_to)
PARTITION BY (year, month)
ORDER BY (symbol, exchange, effective_from)
SETTINGS 
    index_granularity = 8192;

-- Symbol metadata and changes over time
CREATE TABLE IF NOT EXISTS backtest.symbol_metadata (
    symbol String,
    exchange String,
    base_asset String,            -- Base currency (e.g., BTC)
    quote_asset String,           -- Quote currency (e.g., USDT)
    status Enum8('active' = 1, 'inactive' = 2, 'delisted' = 3),
    listing_date UInt64,         -- Unix timestamp when symbol was listed
    delisting_date UInt64 DEFAULT 0, -- Unix timestamp when symbol was delisted
    snapshot_id String,
    
    -- Metadata
    date Date MATERIALIZED toDate(listing_date / 1000),
    month UInt8 MATERIALIZED toMonth(date),
    year UInt16 MATERIALIZED toYear(date)
) ENGINE = ReplacingMergeTree(delisting_date)
PARTITION BY (year, month)
ORDER BY (symbol, exchange, listing_date)
SETTINGS 
    index_granularity = 8192;

-- Data snapshots for reproducibility
CREATE TABLE IF NOT EXISTS system.data_snapshots (
    snapshot_id String,
    snapshot_name String,
    created_at UInt64,           -- Unix timestamp
    description String,
    data_start UInt64,           -- Start timestamp of data
    data_end UInt64,             -- End timestamp of data
    symbols Array(String),       -- Symbols included in snapshot
    exchanges Array(String),     -- Exchanges included in snapshot
    checksum String,            -- SHA256 checksum of snapshot data
    size_bytes UInt64           -- Total size of snapshot data
) ENGINE = MergeTree()
ORDER BY (snapshot_id, created_at)
SETTINGS 
    index_granularity = 8192;

-- Gap detection and reporting
CREATE TABLE IF NOT EXISTS system.data_gaps (
    symbol String,
    exchange String,
    gap_start UInt64,            -- Start of gap (Unix timestamp)
    gap_end UInt64,              -- End of gap (Unix timestamp)
    gap_duration_ms UInt64,      -- Duration of gap in milliseconds
    gap_type Enum8('missing_trades' = 1, 'missing_bars' = 2, 'invalid_data' = 3),
    snapshot_id String,
    detected_at UInt64,          -- When gap was detected
    severity Enum8('low' = 1, 'medium' = 2, 'high' = 3, 'critical' = 4)
) ENGINE = MergeTree()
ORDER BY (symbol, exchange, gap_start)
SETTINGS 
    index_granularity = 8192;

-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS backtest;
CREATE DATABASE IF NOT EXISTS system;

