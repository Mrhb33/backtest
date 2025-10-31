# PowerShell script to run manual backtest with verbose logging
# This will show every candle's calculations in a detailed table format

Write-Host "🚀 Starting Manual Backtest with Verbose Logging..." -ForegroundColor Green

# Run the strategy with verbose mode enabled
go run go-services/cmd/strategy_runner/main.go `
  --csv data/btc_1m.csv `
  --verbose `
  --enable-traces `
  --trace-prefix manual_backtest `
  --warmup 300 `
  --slippage-mode TRADE_SWEEP `
  --intrabar-policy LINEAR_INTERPOLATION `
  --last-days 1

Write-Host "✅ Manual backtest completed!" -ForegroundColor Green
Write-Host "📊 Check the logs above for the detailed candle-by-candle table" -ForegroundColor Yellow
Write-Host "📁 Trace files exported: manual_backtest_*.csv" -ForegroundColor Cyan
