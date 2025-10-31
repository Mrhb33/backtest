-- ClickHouse Views for Backtesting System
-- Optimized views for efficient data access patterns

-- Canonical bars view with session masks and timezone handling
CREATE VIEW IF NOT EXISTS backtest.view_bars AS
SELECT 
    symbol,
    ts,
    open,
    high,
    low,
    close,
    volume,
    trade_count,
    snapshot_id,
    -- Session mask: 1 = trading session, 0 = non-trading hours
    CASE 
        WHEN toHour(toDateTime(ts / 1000)) BETWEEN 9 AND 16 THEN 1
        ELSE 0
    END AS session_mask,
    -- Timezone-adjusted timestamp (UTC)
    ts AS ts_utc,
    -- Bar duration in milliseconds
    CASE 
        WHEN ts % 60000 = 0 THEN 60000  -- 1-minute bars
        WHEN ts % 1000 = 0 THEN 1000    -- 1-second bars
        ELSE 1000
    END AS bar_duration_ms
FROM backtest.ohlcv_1m
WHERE snapshot_id = (
    SELECT snapshot_id 
    FROM system.data_snapshots 
    ORDER BY created_at DESC 
    LIMIT 1
);

-- Intrabar data view for detailed trade path simulation
CREATE VIEW IF NOT EXISTS backtest.view_intrabar AS
SELECT 
    symbol,
    ts,
    price,
    quantity,
    side,
    trade_id,
    exchange,
    snapshot_id,
    -- Trade sequence within the bar
    row_number() OVER (
        PARTITION BY symbol, toStartOfMinute(toDateTime(ts / 1000)) 
        ORDER BY ts, trade_id
    ) AS trade_sequence,
    -- Bar timestamp (start of minute)
    toUInt64(toStartOfMinute(toDateTime(ts / 1000))) * 1000 AS bar_ts
FROM backtest.trades
WHERE snapshot_id = (
    SELECT snapshot_id 
    FROM system.data_snapshots 
    ORDER BY created_at DESC 
    LIMIT 1
);

-- Active exchange rules view (current rules only)
CREATE VIEW IF NOT EXISTS backtest.view_active_rules AS
SELECT 
    symbol,
    exchange,
    tick_size,
    lot_size,
    min_notional,
    maker_fee,
    taker_fee,
    precision_price,
    precision_quantity,
    effective_from,
    snapshot_id
FROM backtest.ref_exchange_rules
WHERE effective_to = 0  -- Active rules only
AND snapshot_id = (
    SELECT snapshot_id 
    FROM system.data_snapshots 
    ORDER BY created_at DESC 
    LIMIT 1
);

-- Active symbols view
CREATE VIEW IF NOT EXISTS backtest.view_active_symbols AS
SELECT 
    symbol,
    exchange,
    base_asset,
    quote_asset,
    listing_date,
    snapshot_id
FROM backtest.symbol_metadata
WHERE status = 'active'
AND delisting_date = 0
AND snapshot_id = (
    SELECT snapshot_id 
    FROM system.data_snapshots 
    ORDER BY created_at DESC 
    LIMIT 1
);

-- Data quality summary view
CREATE VIEW IF NOT EXISTS system.view_data_quality AS
SELECT 
    symbol,
    exchange,
    COUNT(*) as total_gaps,
    SUM(gap_duration_ms) as total_gap_duration_ms,
    MAX(gap_duration_ms) as max_gap_duration_ms,
    COUNT(CASE WHEN severity = 'critical' THEN 1 END) as critical_gaps,
    COUNT(CASE WHEN severity = 'high' THEN 1 END) as high_gaps,
    COUNT(CASE WHEN severity = 'medium' THEN 1 END) as medium_gaps,
    COUNT(CASE WHEN severity = 'low' THEN 1 END) as low_gaps,
    snapshot_id
FROM system.data_gaps
GROUP BY symbol, exchange, snapshot_id;

-- Performance metrics view for monitoring
CREATE VIEW IF NOT EXISTS system.view_performance_metrics AS
SELECT 
    'trades' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT symbol) as symbol_count,
    MIN(ts) as min_timestamp,
    MAX(ts) as max_timestamp,
    snapshot_id
FROM backtest.trades
GROUP BY snapshot_id

UNION ALL

SELECT 
    'ohlcv_1m' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT symbol) as symbol_count,
    MIN(ts) as min_timestamp,
    MAX(ts) as max_timestamp,
    snapshot_id
FROM backtest.ohlcv_1m
GROUP BY snapshot_id

UNION ALL

SELECT 
    'ohlcv_1s' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT symbol) as symbol_count,
    MIN(ts) as min_timestamp,
    MAX(ts) as max_timestamp,
    snapshot_id
FROM backtest.ohlcv_1s
GROUP BY snapshot_id;

