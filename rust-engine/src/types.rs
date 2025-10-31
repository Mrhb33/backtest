//! Type definitions for the backtesting engine

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Market data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketData {
    pub symbol: String,
    pub timeframe: String,
    pub bars: Vec<Bar>,
    pub trades: Vec<Trade>,
    pub rules: ExchangeRules,
}

/// OHLCV bar data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bar {
    pub timestamp: u64,           // Unix milliseconds
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub trade_count: u32,
}

/// Trade data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub timestamp: u64,          // Unix milliseconds
    pub price: Decimal,
    pub quantity: Decimal,
    pub side: TradeSide,
    pub trade_id: String,
}

/// Trade side enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TradeSide {
    Buy,
    Sell,
}

/// Exchange trading rules
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeRules {
    pub tick_size: Decimal,
    pub lot_size: Decimal,
    pub min_notional: Decimal,
    pub maker_fee: Decimal,
    pub taker_fee: Decimal,
    pub precision_price: u8,
    pub precision_quantity: u8,
}

impl Default for ExchangeRules {
    fn default() -> Self {
        Self {
            tick_size: Decimal::new(1, 8),      // 0.00000001
            lot_size: Decimal::new(1, 8),       // 0.00000001
            min_notional: Decimal::new(10, 0),   // 10.0
            maker_fee: Decimal::new(1, 4),       // 0.0001 (0.01%)
            taker_fee: Decimal::new(1, 4),       // 0.0001 (0.01%)
            precision_price: 8,
            precision_quantity: 8,
        }
    }
}

/// Backtest result for a single symbol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolResult {
    pub symbol: String,
    pub trades: Vec<ExecutedTrade>,
    pub positions: Vec<Position>,
    pub equity_curve: Vec<EquityPoint>,
    pub drawdown: Decimal,
    pub exposure: Decimal,
    pub attribution: HashMap<String, Decimal>,
    pub trade_table: Option<TradeTableResult>,
}

/// Complete backtest result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResult {
    pub job_id: String,
    pub execution_time_ms: u64,
    pub symbol_results: Vec<SymbolResult>,
    pub performance_metrics: PerformanceMetrics,
    pub manifest: RunManifest,
}

/// Executed trade with fees and slippage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutedTrade {
    pub timestamp: u64,
    pub symbol: String,
    pub side: TradeSide,
    pub quantity: Decimal,
    pub price: Decimal,
    pub fee: Decimal,
    pub slippage: Decimal,
    pub reason_code: String,
}

/// Position at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub timestamp: u64,
    pub symbol: String,
    pub quantity: Decimal,
    pub avg_price: Decimal,
    pub unrealized_pnl: Decimal,
    pub realized_pnl: Decimal,
}

/// Equity curve point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityPoint {
    pub timestamp: u64,
    pub equity: Decimal,
    pub drawdown: Decimal,
    pub exposure: Decimal,
}

/// Strategy signal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategySignal {
    pub side: TradeSide,
    pub size: Decimal,
    pub entry_price: Option<Decimal>,
    pub take_profit: Option<Decimal>,
    pub stop_loss: Option<Decimal>,
    pub time_to_live: Option<u64>, // milliseconds
}

/// Trade type enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TradeType {
    Long,
    Short,
}

/// Exit reason enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ExitReason {
    TakeProfit,
    StopLoss,
    StrategyExit,
    Liquidation,
    Timeout,
}

/// Hit TP/SL status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HitTpSl {
    TakeProfit,
    StopLoss,
    None,
}

/// Complete trade record for the trade table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    /// Trade close date in ISO UTC
    pub date: String,
    /// Long or Short
    pub trade_type: TradeType,
    /// Entry price with entry time
    pub entry_price: Decimal,
    pub entry_time_utc: String,
    /// Exit price with exit time
    pub exit_price: Decimal,
    pub exit_time_utc: String,
    /// Reason for exit
    pub exit_reason: ExitReason,
    /// Whether TP or SL was hit
    pub hit_tp_sl: HitTpSl,
    /// Size in USD (always 1000 by default)
    pub size_usd: Decimal,
    /// Quantity executed
    pub qty: Decimal,
    /// Total fees (entry + exit)
    pub fees_usd: Decimal,
    /// Net PnL in USD
    pub pnl_usd: Decimal,
    /// PnL as percentage
    pub pnl_pct: Decimal,
    /// Symbol (hidden column for per-symbol breakdowns)
    pub symbol: String,
}

/// Trade summary totals
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeSummary {
    /// Total number of trades
    pub total_trades: u32,
    /// Number of winning trades
    pub wins: u32,
    /// Number of losing trades
    pub losses: u32,
    /// Win rate as percentage
    pub win_rate: Decimal,
    /// Net PnL in USD
    pub net_pnl_usd: Decimal,
    /// Average win in USD
    pub avg_win_usd: Decimal,
    /// Average loss in USD
    pub avg_loss_usd: Decimal,
    /// Expectancy (win_rate * avg_win + (1 - win_rate) * avg_loss)
    pub expectancy: Decimal,
    /// Maximum drawdown
    pub max_drawdown: Decimal,
    /// Profit factor (gross profit / gross loss)
    pub profit_factor: Decimal,
    /// Average holding time in hours
    pub avg_holding_time_hours: Decimal,
}

/// Active position with TP/SL tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivePosition {
    pub symbol: String,
    pub trade_type: TradeType,
    pub entry_time: u64,
    pub entry_price: Decimal,
    pub quantity: Decimal,
    pub take_profit: Option<Decimal>,
    pub stop_loss: Option<Decimal>,
    pub time_to_live: Option<u64>,
    pub entry_fee: Decimal,
    pub size_usd: Decimal,
}

/// Trade table generation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeTableResult {
    pub trades: Vec<TradeRecord>,
    pub summary: TradeSummary,
    pub rejected_trades: Vec<RejectedTrade>,
}

/// Rejected trade with reason
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RejectedTrade {
    pub timestamp: u64,
    pub symbol: String,
    pub side: TradeSide,
    pub reason: String,
    pub notional: Decimal,
}

/// Indicator value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndicatorValue {
    pub timestamp: u64,
    pub value: Decimal,
}

/// Indicator calculation parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndicatorParams {
    pub period: usize,
    pub alpha: Option<Decimal>,
    pub threshold: Option<Decimal>,
}

/// Simulation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationResult {
    pub trades: Vec<ExecutedTrade>,
    pub positions: Vec<Position>,
    pub equity_curve: Vec<EquityPoint>,
    pub max_drawdown: Decimal,
    pub exposure: Decimal,
    pub attribution: HashMap<String, Decimal>,
}

// Re-export commonly used types from lib.rs
pub use crate::PerformanceMetrics;
pub use crate::RunManifest;
pub use crate::IntrabarPolicy;
pub use crate::SlippageMode;

