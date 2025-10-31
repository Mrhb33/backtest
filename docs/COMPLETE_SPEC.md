# Complete Backtesting System Specification

## WASM ABI Specification

### Host Functions
- `series_read(timeframe, column, idx) -> f64`
- `indicator(id, args_ptr, args_len) -> f64`
- `order(opcode, args_ptr, args_len) -> i32`
- `prng_next() -> f64`
- `clock_now() -> i64`

### Memory Layout
- Linear memory with fixed regions
- ABI_VERSION=1 header
- Sandbox limits: fuel/gas, stack/heap caps

## IR Opcodes

### Value Types
- Void, Bool, I64, F64, Decimal128, TimestampMs, SeriesF64, SeriesBool

### Operations
- LoadParam, LoadSeries, Const, Math, Compare, Logical
- Indicator, EnterLong, EnterShort, Exit, UpdateOrders
- AdjustSLTP, TrailStop, If, Sequence

## Fill Truth Tables

### Market Orders
- Fill at next tradable price (open if gapped)
- Apply slippage model

### Limit Orders
- Long: fill if low ≤ limit, price=min(limit, open if open ≤ limit)
- Short: fill if high ≥ limit, price=max(limit, open if open ≥ limit)

### Stop Orders
- Long: trigger if high ≥ stop, price=max(stop, open if open ≥ stop)
- Short: trigger if low ≤ stop, price=min(stop, open if open ≤ stop)

### Priority Resolution
- If TP and SL hit intrabar, use path ordering for first-touch

## Parity Suite Cookbook

### Test Cases
1. TP-first vs SL-first scenarios
2. Intrabar reversals
3. Gap handling
4. Trailing stop updates
5. OCO collisions

### Validation
- Bit-identical event logs across runs
- TradingView cross-checks within tolerances

## Runbook for Failed Determinism

### Common Issues
1. Look-ahead violations
2. Non-deterministic PRNG usage
3. Time-based dependencies
4. Floating-point precision issues

### Debugging Steps
1. Check IR validation logs
2. Verify warm-up periods
3. Validate intrabar path ordering
4. Review event log consistency
