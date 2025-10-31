-- Professional OHLCV pipeline schema for 1m → 5m derivation with parity-ready rules
-- Staging (raw mirror), Canonical 1m with dedup/alignment, Derived 5m, completeness views, and published view

-- 0) Create database (optional)
CREATE DATABASE IF NOT EXISTS market;

-- 1) STAGING: Raw mirror of Binance 1m klines (exactly-as-downloaded + ingest metadata)
CREATE TABLE IF NOT EXISTS market.staging_klines_1m (
    symbol                     String,
    interval                   LowCardinality(String), -- expect '1m'
    open_time_ms               UInt64,
    open                       String,                 -- keep raw text in staging
    high                       String,
    low                        String,
    close                      String,
    volume                     String,
    close_time_ms              UInt64,
    quote_asset_volume         String,
    number_of_trades           UInt64,
    taker_buy_base_asset_vol   String,
    taker_buy_quote_asset_vol  String,
    ignore_raw                 String,
    -- ingest metadata
    file_month                 String,                 -- e.g. '2025-09'
    source                     LowCardinality(String), -- e.g. 'binance-bulk'
    ingested_at                DateTime                -- now() at ingest time
)
ENGINE = MergeTree
ORDER BY (symbol, interval, open_time_ms);

-- 2) CANONICAL 1m: Strict types, alignment, dedup by (symbol, interval, open_time_ms)
CREATE TABLE IF NOT EXISTS market.ohlcv_1m_canonical (
    symbol                     String,
    interval                   LowCardinality(String), -- '1m'
    open_time_ms               UInt64,
    open                       Decimal64(8),
    high                       Decimal64(8),
    low                        Decimal64(8),
    close                      Decimal64(8),
    volume                     Decimal64(8),
    close_time_ms              UInt64,
    quote_asset_volume         Decimal64(8),
    number_of_trades           UInt64,
    taker_buy_base_asset_vol   Decimal64(8),
    taker_buy_quote_asset_vol  Decimal64(8),
    -- dedup/versioning
    ingested_at                DateTime
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (symbol, interval, open_time_ms);

-- Helper view to canonicalize from staging with alignment & sanity checks
-- Note: We deliberately keep INSERT…SELECT as a separate step for idempotency
CREATE VIEW IF NOT EXISTS market.vw_stage_to_1m_canonical AS
SELECT
    symbol,
    interval,
    open_time_ms,
    toDecimal64(open, 8)  AS open,
    toDecimal64(high, 8)  AS high,
    toDecimal64(low, 8)   AS low,
    toDecimal64(close, 8) AS close,
    toDecimal64(volume, 8)                    AS volume,
    close_time_ms,
    toDecimal64(quote_asset_volume, 8)        AS quote_asset_volume,
    number_of_trades,
    toDecimal64(taker_buy_base_asset_vol, 8)  AS taker_buy_base_asset_vol,
    toDecimal64(taker_buy_quote_asset_vol, 8) AS taker_buy_quote_asset_vol,
    ingested_at
FROM market.staging_klines_1m
WHERE
    interval = '1m'
    AND open_time_ms % 60000 = 0
    AND close_time_ms = open_time_ms + 60000 - 1
    AND high >= greatest(open, close, low)
    AND low  <= least(open, close, high);

-- 3) MISSING MINUTES (1m completeness)
-- Provide expected grid and left join to list holes
CREATE TABLE IF NOT EXISTS market.expected_minutes_1m (
    open_time_ms UInt64,
    symbol       String,
    interval     LowCardinality(String) -- '1m'
)
ENGINE = MergeTree
ORDER BY (symbol, interval, open_time_ms);

CREATE VIEW IF NOT EXISTS market.vw_missing_minutes_1m AS
SELECT g.symbol, g.open_time_ms
FROM market.expected_minutes_1m AS g
LEFT JOIN market.ohlcv_1m_canonical AS c
    ON c.symbol = g.symbol AND c.interval = g.interval AND c.open_time_ms = g.open_time_ms
WHERE c.open_time_ms IS NULL
ORDER BY g.open_time_ms;

-- 4) DERIVED 5m from canonical 1m with exact bucketing
CREATE TABLE IF NOT EXISTS market.ohlcv_5m_canonical (
    symbol                     String,
    interval                   LowCardinality(String), -- '5m'
    open_time_ms               UInt64,
    open                       Decimal64(8),
    high                       Decimal64(8),
    low                        Decimal64(8),
    close                      Decimal64(8),
    volume                     Decimal64(8),
    close_time_ms              UInt64,
    quote_asset_volume         Decimal64(8),
    number_of_trades           UInt64,
    taker_buy_base_asset_vol   Decimal64(8),
    taker_buy_quote_asset_vol  Decimal64(8),
    derived_at                 DateTime
)
ENGINE = ReplacingMergeTree(derived_at)
ORDER BY (symbol, interval, open_time_ms);

-- Deterministic 5m derivation (run as INSERT … SELECT when ready)
CREATE VIEW IF NOT EXISTS market.vw_derive_5m_from_1m AS
SELECT
    c.symbol,
    '5m' AS interval,
    bucket AS open_time_ms,
    argMin(open, open_time_ms)   AS open,   -- first open by time
    max(high)                    AS high,
    min(low)                     AS low,
    argMax(close, open_time_ms)  AS close,  -- last close by time
    sum(volume)                  AS volume,
    bucket + 300000 - 1          AS close_time_ms,
    sum(quote_asset_volume)      AS quote_asset_volume,
    sum(number_of_trades)        AS number_of_trades,
    sum(taker_buy_base_asset_vol)  AS taker_buy_base_asset_vol,
    sum(taker_buy_quote_asset_vol) AS taker_buy_quote_asset_vol,
    now()                        AS derived_at
FROM (
    SELECT *, intDiv(open_time_ms, 300000) * 300000 AS bucket
    FROM market.ohlcv_1m_canonical
) c
GROUP BY c.symbol, bucket
ORDER BY c.symbol, bucket;

-- 5) MISSING 5m BUCKETS completeness view (build expected grid similarly to 1m)
CREATE TABLE IF NOT EXISTS market.expected_buckets_5m (
    open_time_ms UInt64,
    symbol       String,
    interval     LowCardinality(String) -- '5m'
)
ENGINE = MergeTree
ORDER BY (symbol, interval, open_time_ms);

CREATE VIEW IF NOT EXISTS market.vw_missing_buckets_5m AS
SELECT g.symbol, g.open_time_ms
FROM market.expected_buckets_5m AS g
LEFT JOIN market.ohlcv_5m_canonical AS c
    ON c.symbol = g.symbol AND c.interval = g.interval AND c.open_time_ms = g.open_time_ms
WHERE c.open_time_ms IS NULL
ORDER BY g.open_time_ms;

-- 6) PUBLISHED read-only view for consumers
CREATE VIEW IF NOT EXISTS market.vw_published_5m AS
SELECT
    symbol,
    interval,
    open_time_ms,
    close_time_ms,
    open, high, low, close,
    volume,
    quote_asset_volume,
    number_of_trades,
    taker_buy_base_asset_vol,
    taker_buy_quote_asset_vol
FROM market.ohlcv_5m_canonical
ORDER BY symbol, open_time_ms;

-- Notes:
-- - Populate expected_minutes_1m/expected_buckets_5m with your fixed 5-year UTC window grids.
-- - Use INSERT … SELECT from vw_stage_to_1m_canonical into ohlcv_1m_canonical for idempotent canonicalization.
-- - Then INSERT … SELECT from vw_derive_5m_from_1m into ohlcv_5m_canonical.
-- - ReplacingMergeTree ensures deterministic dedup on re-runs (by ingested_at/derived_at versioning).

-- Professional OHLCV Schema - Binance Parity
-- Exact 1:1 mapping with Binance klines data
-- Implements ReplacingMergeTree for deduplication and proper partitioning

-- ============================================================================
-- A) CANONICAL RAW TABLE (Single source of truth for all intervals)
-- ============================================================================

CREATE TABLE IF NOT EXISTS backtest.ohlcv_raw (
    symbol             LowCardinality(String),
    interval           LowCardinality(String),  -- '1m','5m','15m',...
    open_time_ms       UInt64,                  -- Binance open time (ms)
    open               Decimal(20,8),
    high               Decimal(20,8),
    low                Decimal(20,8),
    close              Decimal(20,8),
    volume_base        Decimal(28,12),         -- base asset volume
    quote_volume       Decimal(28,12),         -- quote asset volume
    trades             UInt32,
    taker_base_vol     Decimal(28,12),
    taker_quote_vol    Decimal(28,12),
    close_time_ms      UInt64,                  -- open_time_ms + interval_ms - 1
    source             LowCardinality(String),  -- 'binance-bulk', 'binance-api', etc.
    file_month         Date,                    -- 2025-09-01 for Sept 2025 bulk zip
    ingested_at        DateTime64(3, 'UTC')     -- version column for dedup
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (symbol, interval, open_time_ms)
PARTITION BY (symbol, toYYYYMM(toDateTime(open_time_ms/1000)))
SETTINGS 
    index_granularity = 8192,
    allow_nullable_key = 1;

-- ============================================================================
-- B) STAGING TABLE (Safe bulk inserts with validation)
-- ============================================================================

CREATE TABLE IF NOT EXISTS backtest.staging_klines (
    symbol             LowCardinality(String),
    interval           LowCardinality(String),
    open_time_ms       UInt64,
    open               Decimal(20,8),
    high               Decimal(20,8),
    low                Decimal(20,8),
    close              Decimal(20,8),
    volume_base        Decimal(28,12),
    quote_volume       Decimal(28,12),
    trades             UInt32,
    taker_base_vol     Decimal(28,12),
    taker_quote_vol    Decimal(28,12),
    close_time_ms      UInt64,
    source             LowCardinality(String),
    file_month         Date,
    ingested_at        DateTime64(3, 'UTC'),
    row_hash           FixedString(32)          -- MD5 hash for validation
) ENGINE = MergeTree
ORDER BY (symbol, interval, open_time_ms)
PARTITION BY (symbol, toYYYYMM(toDateTime(open_time_ms/1000)))
SETTINGS 
    index_granularity = 8192;

-- ============================================================================
-- C) DERIVED TABLES (5m and 15m from 1m)
-- ============================================================================

CREATE TABLE IF NOT EXISTS backtest.ohlcv_5m AS backtest.ohlcv_raw;
CREATE TABLE IF NOT EXISTS backtest.ohlcv_15m AS backtest.ohlcv_raw;

-- ============================================================================
-- D) DATA QUALITY TABLES
-- ============================================================================

-- Missing minutes detection
CREATE TABLE IF NOT EXISTS backtest.missing_minutes (
    symbol             LowCardinality(String),
    interval           LowCardinality(String),
    expected_time_ms   UInt64,
    detected_at        DateTime64(3, 'UTC')
) ENGINE = MergeTree
ORDER BY (symbol, interval, expected_time_ms)
SETTINGS index_granularity = 8192;

-- Data anomalies (negative wicks, zero volume bursts, spikes)
CREATE TABLE IF NOT EXISTS backtest.data_anomalies (
    symbol             LowCardinality(String),
    interval           LowCardinality(String),
    open_time_ms       UInt64,
    anomaly_type       LowCardinality(String),  -- 'negative_wick', 'zero_volume', 'price_spike'
    severity           Enum8('low' = 1, 'medium' = 2, 'high' = 3, 'critical' = 4),
    details            String,
    detected_at        DateTime64(3, 'UTC')
) ENGINE = MergeTree
ORDER BY (symbol, interval, open_time_ms)
SETTINGS index_granularity = 8192;

-- Daily checksums for drift detection
CREATE TABLE IF NOT EXISTS backtest.daily_checksums (
    symbol             LowCardinality(String),
    interval           LowCardinality(String),
    day                Date,
    checksum           FixedString(32),         -- MD5 of daily data
    row_count          UInt32,
    first_time_ms      UInt64,
    last_time_ms       UInt64,
    computed_at        DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (symbol, interval, day)
SETTINGS index_granularity = 8192;

-- Parity validation vs Binance API
CREATE TABLE IF NOT EXISTS backtest.parity_checks (
    symbol             LowCardinality(String),
    interval           LowCardinality(String),
    open_time_ms       UInt64,
    binance_open       Decimal(20,8),
    binance_high       Decimal(20,8),
    binance_low        Decimal(20,8),
    binance_close      Decimal(20,8),
    binance_volume     Decimal(28,12),
    binance_trades     UInt32,
    our_open           Decimal(20,8),
    our_high           Decimal(20,8),
    our_low            Decimal(20,8),
    our_close          Decimal(20,8),
    our_volume         Decimal(28,12),
    our_trades         UInt32,
    open_diff          Decimal(20,8),
    high_diff          Decimal(20,8),
    low_diff           Decimal(20,8),
    close_diff         Decimal(20,8),
    volume_diff        Decimal(28,12),
    trades_diff        Int32,
    is_exact_match     UInt8,
    checked_at         DateTime64(3, 'UTC')
) ENGINE = MergeTree
ORDER BY (symbol, interval, open_time_ms)
SETTINGS index_granularity = 8192;

-- ============================================================================
-- E) QUALITY VIEWS
-- ============================================================================

-- Missing minutes finder for 1m
CREATE OR REPLACE VIEW backtest.find_missing_1m AS
WITH expected_minutes AS (
    SELECT 
        symbol,
        open_time_ms,
        toDateTime(open_time_ms/1000) as dt
    FROM (
        SELECT 
            'BTCUSDT' as symbol,
            toUnixTimestamp64Milli(toStartOfMinute(toDateTime(open_time_ms/1000))) as open_time_ms
        FROM backtest.ohlcv_raw 
        WHERE symbol = 'BTCUSDT' AND interval = '1m'
        GROUP BY toStartOfMinute(toDateTime(open_time_ms/1000))
    )
    WHERE open_time_ms % 60000 = 0  -- Ensure minute alignment
),
actual_minutes AS (
    SELECT 
        symbol,
        open_time_ms
    FROM backtest.ohlcv_raw 
    WHERE symbol = 'BTCUSDT' AND interval = '1m'
)
SELECT 
    e.symbol,
    e.open_time_ms,
    now64(3) as detected_at
FROM expected_minutes e
LEFT JOIN actual_minutes a ON e.symbol = a.symbol AND e.open_time_ms = a.open_time_ms
WHERE a.open_time_ms IS NULL;

-- Daily completeness check
CREATE OR REPLACE VIEW backtest.daily_completeness AS
SELECT
    symbol,
    interval,
    toDate(toDateTime(open_time_ms/1000)) AS day,
    count() AS actual_bars,
    CASE interval
        WHEN '1m' THEN 1440
        WHEN '5m' THEN 288
        WHEN '15m' THEN 96
        ELSE 0
    END AS expected_bars,
    expected_bars - actual_bars AS missing_bars,
    round((actual_bars / expected_bars) * 100, 2) AS completeness_pct
FROM backtest.ohlcv_raw
GROUP BY symbol, interval, day
ORDER BY symbol, interval, day;

-- Data anomalies detection
CREATE OR REPLACE VIEW backtest.detect_anomalies AS
SELECT
    symbol,
    interval,
    open_time_ms,
    CASE 
        WHEN low > high THEN 'negative_wick'
        WHEN low > open OR low > close THEN 'negative_wick'
        WHEN high < open OR high < close THEN 'negative_wick'
        WHEN volume_base = 0 AND trades > 0 THEN 'zero_volume_with_trades'
        WHEN volume_base = 0 AND quote_volume > 0 THEN 'zero_volume_with_quote'
        WHEN high / low > 1.5 THEN 'price_spike'
        WHEN trades = 0 AND volume_base > 0 THEN 'trades_zero_volume_positive'
        ELSE 'normal'
    END as anomaly_type,
    CASE 
        WHEN low > high THEN 4  -- critical
        WHEN low > open OR low > close THEN 4  -- critical
        WHEN high < open OR high < close THEN 4  -- critical
        WHEN volume_base = 0 AND trades > 0 THEN 3  -- high
        WHEN high / low > 1.5 THEN 3  -- high
        ELSE 1  -- low
    END as severity,
    concat('open=', toString(open), ', high=', toString(high), ', low=', toString(low), ', close=', toString(close), ', volume=', toString(volume_base), ', trades=', toString(trades)) as details
FROM backtest.ohlcv_raw
WHERE 
    (low > high OR low > open OR low > close OR high < open OR high < close) OR
    (volume_base = 0 AND (trades > 0 OR quote_volume > 0)) OR
    (trades = 0 AND volume_base > 0) OR
    (high / low > 1.5);

-- Duplicate detection
CREATE OR REPLACE VIEW backtest.find_duplicates AS
SELECT
    symbol,
    interval,
    open_time_ms,
    count() as duplicate_count,
    groupArray(ingested_at) as ingestion_times
FROM backtest.ohlcv_raw
GROUP BY symbol, interval, open_time_ms
HAVING count() > 1
ORDER BY duplicate_count DESC;

-- ============================================================================
-- F) UTILITY FUNCTIONS
-- ============================================================================

-- Function to calculate row hash (for validation)
CREATE OR REPLACE FUNCTION backtest.calculate_row_hash(
    p_symbol String,
    p_interval String,
    p_open_time_ms UInt64,
    p_open Decimal(20,8),
    p_high Decimal(20,8),
    p_low Decimal(20,8),
    p_close Decimal(20,8),
    p_volume_base Decimal(28,12),
    p_quote_volume Decimal(28,12),
    p_trades UInt32,
    p_taker_base_vol Decimal(28,12),
    p_taker_quote_vol Decimal(28,12),
    p_close_time_ms UInt64
) RETURNS FixedString(32)
AS md5(concat(
    p_symbol, '|',
    p_interval, '|',
    toString(p_open_time_ms), '|',
    toString(p_open), '|',
    toString(p_high), '|',
    toString(p_low), '|',
    toString(p_close), '|',
    toString(p_volume_base), '|',
    toString(p_quote_volume), '|',
    toString(p_trades), '|',
    toString(p_taker_base_vol), '|',
    toString(p_taker_quote_vol), '|',
    toString(p_close_time_ms)
));

-- Function to validate OHLCV data integrity
CREATE OR REPLACE FUNCTION backtest.validate_ohlcv(
    p_open Decimal(20,8),
    p_high Decimal(20,8),
    p_low Decimal(20,8),
    p_close Decimal(20,8),
    p_volume_base Decimal(28,12),
    p_trades UInt32
) RETURNS UInt8
AS (
    p_high >= p_low AND
    p_high >= p_open AND
    p_high >= p_close AND
    p_low <= p_open AND
    p_low <= p_close AND
    p_volume_base >= 0 AND
    p_trades >= 0
);

-- ============================================================================
-- G) INDEXES FOR PERFORMANCE
-- ============================================================================

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_ohlcv_raw_symbol_time ON backtest.ohlcv_raw (symbol, open_time_ms) TYPE minmax GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_ohlcv_raw_interval ON backtest.ohlcv_raw (interval) TYPE set(0) GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_staging_symbol_time ON backtest.staging_klines (symbol, open_time_ms) TYPE minmax GRANULARITY 1;

-- ============================================================================
-- H) CLEANUP FUNCTIONS
-- ============================================================================

-- Function to clean up old staging data
CREATE OR REPLACE FUNCTION backtest.cleanup_staging_data(
    p_days_to_keep UInt32 DEFAULT 7
) RETURNS UInt64
AS (
    (SELECT count() FROM backtest.staging_klines WHERE ingested_at < now64(3) - INTERVAL p_days_to_keep DAY)
);

-- ============================================================================
-- I) AUDIT TRAIL
-- ============================================================================

-- Track all schema changes and data operations
CREATE TABLE IF NOT EXISTS backtest.audit_log (
    operation_id       String,
    operation_type     LowCardinality(String),  -- 'schema_change', 'data_load', 'derivation', 'cleanup'
    table_name         LowCardinality(String),
    affected_rows      UInt64,
    operation_details  String,
    executed_by        LowCardinality(String),
    executed_at        DateTime64(3, 'UTC')
) ENGINE = MergeTree
ORDER BY (operation_id, executed_at)
SETTINGS index_granularity = 8192;
