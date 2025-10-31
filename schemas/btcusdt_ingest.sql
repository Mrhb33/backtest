-- 0) Database
CREATE DATABASE IF NOT EXISTS backtest;

-- 1) Staging tables
CREATE TABLE IF NOT EXISTS backtest.raw_klines (
    open_time_ms String,
    open String,
    high String,
    low String,
    close String,
    volume String,
    close_time_ms String,
    quote_asset_volume String,
    number_of_trades String,
    taker_buy_base_asset_volume String,
    taker_buy_quote_asset_volume String,
    ignore String,
    file_month String,
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (file_month, open_time_ms);

CREATE TABLE IF NOT EXISTS backtest.staging_backfill AS backtest.raw_klines;

-- Ingest ledger for idempotency
CREATE TABLE IF NOT EXISTS backtest.ingest_ledger (
    month String,
    file_sha256 String,
    row_count UInt64,
    source Enum('binance_csv','binance_rest'),
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (month, source);

-- 2) Canonical tables
CREATE TABLE IF NOT EXISTS backtest.data (
    symbol Enum('BTCUSDT'),
    interval Enum8('1m','5m','15m'),
    open_time_ms UInt64,
    open Decimal(18,8),
    high Decimal(18,8),
    low Decimal(18,8),
    close Decimal(18,8),
    volume Decimal(24,8),
    quote_volume Decimal(24,8),
    trades UInt32,
    taker_base Decimal(24,8),
    taker_quote Decimal(24,8),
    close_time_ms UInt64,
    version UInt64 DEFAULT toUInt64(materialize(1)),
    ingested_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(toDateTime(open_time_ms/1000))
ORDER BY (symbol, interval, open_time_ms)
SETTINGS index_granularity = 8192, allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS backtest.anomalies (
    ts DateTime DEFAULT now(),
    stage String,
    reason String,
    open_time_ms UInt64,
    details String
) ENGINE = MergeTree()
ORDER BY (stage, open_time_ms);

CREATE TABLE IF NOT EXISTS backtest.daily_hashes (
    day Date,
    symbol Enum('BTCUSDT'),
    interval Enum8('1m','5m','15m'),
    sha String,
    count UInt32
) ENGINE = MergeTree()
ORDER BY (symbol, interval, day);

CREATE TABLE IF NOT EXISTS backtest.data_exceptions (
    month String,
    reason String,
    evidence String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (month);

-- Missing minutes sink
CREATE TABLE IF NOT EXISTS backtest.missing_1m (
    symbol Enum('BTCUSDT'),
    open_time_ms UInt64,
    detected_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (symbol, open_time_ms);

-- 3) Canonicalization insert-select (1m) - This will be run by the ingestion pipeline
-- Parameters: symbol='BTCUSDT'
-- INSERT INTO backtest.data
-- SELECT ... (moved to ingestion pipeline)

-- 4) Daily completeness views
CREATE OR REPLACE VIEW backtest.daily_completeness_1m AS
SELECT
    toDate(toDateTime(open_time_ms/1000)) AS day,
    count() AS actual_1m,
    1440 AS expected_1m,
    1440 - count() AS missing
FROM backtest.data
WHERE symbol='BTCUSDT' AND interval='1m'
GROUP BY day
ORDER BY day;

-- Missing minutes exact list - simplified for now
CREATE OR REPLACE VIEW backtest.find_missing_1m AS
SELECT 'BTCUSDT' AS symbol,
       0 AS open_time_ms,
       now() AS detected_at
WHERE 1=0;

-- 5) Derivation 5m/15m (use MATERIALIZED VIEW or INSERT-SELECT)
CREATE OR REPLACE VIEW backtest.derive_5m AS
SELECT
    'BTCUSDT' AS symbol,
    '5m' AS interval,
    (intDiv(open_time_ms, 300000) * 300000) AS block_start,
    anyHeavy(open) AS open,
    max(high) AS high,
    min(low) AS low,
    anyLast(close) AS close,
    sum(volume) AS volume,
    sum(quote_volume) AS quote_volume,
    sum(trades) AS trades,
    sum(taker_base) AS taker_base,
    sum(taker_quote) AS taker_quote,
    block_start + 300000 - 1 AS close_time_ms
FROM backtest.data
WHERE symbol='BTCUSDT' AND interval='1m'
GROUP BY block_start;

CREATE OR REPLACE VIEW backtest.derive_15m AS
SELECT
    'BTCUSDT' AS symbol,
    '15m' AS interval,
    (intDiv(open_time_ms, 900000) * 900000) AS block_start,
    anyHeavy(open) AS open,
    max(high) AS high,
    min(low) AS low,
    anyLast(close) AS close,
    sum(volume) AS volume,
    sum(quote_volume) AS quote_volume,
    sum(trades) AS trades,
    sum(taker_base) AS taker_base,
    sum(taker_quote) AS taker_quote,
    block_start + 900000 - 1 AS close_time_ms
FROM backtest.data
WHERE symbol='BTCUSDT' AND interval='1m'
GROUP BY block_start;


