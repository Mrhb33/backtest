//! Example WASM trading strategy in Rust
//! 
//! This demonstrates how to write a deterministic trading strategy that compiles to WASM
//! for safe execution in the backtesting engine.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    pub ema_period: usize,
    pub rsi_period: usize,
    pub rsi_oversold: f64,
    pub rsi_overbought: f64,
    pub position_size: f64,
    pub stop_loss_pct: f64,
    pub take_profit_pct: f64,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            ema_period: 20,
            rsi_period: 14,
            rsi_oversold: 30.0,
            rsi_overbought: 70.0,
            position_size: 0.1, // 10% of equity
            stop_loss_pct: 0.02, // 2% stop loss
            take_profit_pct: 0.04, // 4% take profit
        }
    }
}

/// Market data structure passed to strategy
#[derive(Debug, Clone)]
pub struct MarketBar {
    pub timestamp: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// Indicator values
#[derive(Debug, Clone)]
pub struct IndicatorValue {
    pub timestamp: u64,
    pub value: f64,
}

/// Trading signal
#[derive(Debug, Clone)]
pub struct TradingSignal {
    pub side: TradeSide,
    pub size: f64,
    pub entry_price: Option<f64>,
    pub stop_loss: Option<f64>,
    pub take_profit: Option<f64>,
    pub time_to_live: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum TradeSide {
    Buy,
    Sell,
}

/// Current position state
#[derive(Debug, Clone)]
pub struct Position {
    pub symbol: String,
    pub quantity: f64,
    pub avg_price: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
}

/// Main strategy implementation
pub struct EmaRsiStrategy {
    config: StrategyConfig,
    position: Option<Position>,
    ema_values: Vec<IndicatorValue>,
    rsi_values: Vec<IndicatorValue>,
    last_signal_time: u64,
}

impl EmaRsiStrategy {
    pub fn new(config: StrategyConfig) -> Self {
        Self {
            config,
            position: None,
            ema_values: Vec::new(),
            rsi_values: Vec::new(),
            last_signal_time: 0,
        }
    }
    
    /// Process a new bar and generate signals
    pub fn process_bar(
        &mut self,
        bar: &MarketBar,
        ema_values: &[IndicatorValue],
        rsi_values: &[IndicatorValue],
    ) -> Vec<TradingSignal> {
        let mut signals = Vec::new();
        
        // Update indicator values
        self.ema_values = ema_values.to_vec();
        self.rsi_values = rsi_values.to_vec();
        
        // Need at least 2 values for trend analysis
        if self.ema_values.len() < 2 || self.rsi_values.len() < 2 {
            return signals;
        }
        
        let current_ema = self.ema_values.last().unwrap().value;
        let previous_ema = self.ema_values[self.ema_values.len() - 2].value;
        let current_rsi = self.rsi_values.last().unwrap().value;
        
        // Calculate trend direction
        let ema_trend_up = current_ema > previous_ema;
        let price_above_ema = bar.close > current_ema;
        
        // Generate signals based on strategy logic
        match &self.position {
            None => {
                // No position - look for entry signals
                if self.should_enter_long(bar, current_rsi, ema_trend_up, price_above_ema) {
                    signals.push(self.create_long_signal(bar));
                } else if self.should_enter_short(bar, current_rsi, ema_trend_up, price_above_ema) {
                    signals.push(self.create_short_signal(bar));
                }
            },
            Some(position) => {
                // Have position - check for exit signals
                if self.should_exit_position(bar, position) {
                    signals.push(self.create_exit_signal(bar, position));
                }
            }
        }
        
        self.last_signal_time = bar.timestamp;
        signals
    }
    
    /// Determine if we should enter a long position
    fn should_enter_long(
        &self,
        bar: &MarketBar,
        rsi: f64,
        ema_trend_up: bool,
        price_above_ema: bool,
    ) -> bool {
        // Long entry conditions:
        // 1. RSI is oversold (below threshold)
        // 2. EMA trend is up
        // 3. Price is above EMA (momentum)
        // 4. Haven't signaled recently (avoid overtrading)
        
        let time_since_last_signal = bar.timestamp - self.last_signal_time;
        let min_time_between_signals = 300_000; // 5 minutes in milliseconds
        
        rsi < self.config.rsi_oversold
            && ema_trend_up
            && price_above_ema
            && time_since_last_signal > min_time_between_signals
    }
    
    /// Determine if we should enter a short position
    fn should_enter_short(
        &self,
        bar: &MarketBar,
        rsi: f64,
        ema_trend_up: bool,
        price_above_ema: bool,
    ) -> bool {
        // Short entry conditions:
        // 1. RSI is overbought (above threshold)
        // 2. EMA trend is down
        // 3. Price is below EMA (momentum)
        // 4. Haven't signaled recently
        
        let time_since_last_signal = bar.timestamp - self.last_signal_time;
        let min_time_between_signals = 300_000; // 5 minutes
        
        rsi > self.config.rsi_overbought
            && !ema_trend_up
            && !price_above_ema
            && time_since_last_signal > min_time_between_signals
    }
    
    /// Determine if we should exit current position
    fn should_exit_position(&self, bar: &MarketBar, position: &Position) -> bool {
        // Exit conditions:
        // 1. Stop loss hit
        // 2. Take profit hit
        // 3. RSI reversal (for mean reversion)
        
        let current_rsi = self.rsi_values.last().unwrap().value;
        let pnl_pct = (bar.close - position.avg_price) / position.avg_price;
        
        // Stop loss
        if pnl_pct <= -self.config.stop_loss_pct {
            return true;
        }
        
        // Take profit
        if pnl_pct >= self.config.take_profit_pct {
            return true;
        }
        
        // RSI reversal (for mean reversion strategies)
        if position.quantity > 0.0 && current_rsi > self.config.rsi_overbought {
            return true;
        }
        if position.quantity < 0.0 && current_rsi < self.config.rsi_oversold {
            return true;
        }
        
        false
    }
    
    /// Create a long entry signal
    fn create_long_signal(&self, bar: &MarketBar) -> TradingSignal {
        let stop_loss = bar.close * (1.0 - self.config.stop_loss_pct);
        let take_profit = bar.close * (1.0 + self.config.take_profit_pct);
        
        TradingSignal {
            side: TradeSide::Buy,
            size: self.config.position_size,
            entry_price: Some(bar.close),
            stop_loss: Some(stop_loss),
            take_profit: Some(take_profit),
            time_to_live: Some(3600_000), // 1 hour
        }
    }
    
    /// Create a short entry signal
    fn create_short_signal(&self, bar: &MarketBar) -> TradingSignal {
        let stop_loss = bar.close * (1.0 + self.config.stop_loss_pct);
        let take_profit = bar.close * (1.0 - self.config.take_profit_pct);
        
        TradingSignal {
            side: TradeSide::Sell,
            size: self.config.position_size,
            entry_price: Some(bar.close),
            stop_loss: Some(stop_loss),
            take_profit: Some(take_profit),
            time_to_live: Some(3600_000), // 1 hour
        }
    }
    
    /// Create an exit signal
    fn create_exit_signal(&self, bar: &MarketBar, position: &Position) -> TradingSignal {
        TradingSignal {
            side: if position.quantity > 0.0 { TradeSide::Sell } else { TradeSide::Buy },
            size: position.quantity.abs(),
            entry_price: Some(bar.close),
            stop_loss: None,
            take_profit: None,
            time_to_live: Some(60_000), // 1 minute
        }
    }
    
    /// Update position after trade execution
    pub fn update_position(&mut self, position: Option<Position>) {
        self.position = position;
    }
    
    /// Get strategy metadata
    pub fn get_metadata(&self) -> StrategyMetadata {
        StrategyMetadata {
            name: "EMA_RSI_Strategy".to_string(),
            version: "1.0.0".to_string(),
            description: "EMA trend following with RSI mean reversion".to_string(),
            author: "backtest-engine".to_string(),
            required_indicators: vec!["ema".to_string(), "rsi".to_string()],
            parameters: self.config_to_params(),
        }
    }
    
    /// Convert config to parameter map
    fn config_to_params(&self) -> HashMap<String, String> {
        let mut params = HashMap::new();
        params.insert("ema_period".to_string(), self.config.ema_period.to_string());
        params.insert("rsi_period".to_string(), self.config.rsi_period.to_string());
        params.insert("rsi_oversold".to_string(), self.config.rsi_oversold.to_string());
        params.insert("rsi_overbought".to_string(), self.config.rsi_overbought.to_string());
        params.insert("position_size".to_string(), self.config.position_size.to_string());
        params.insert("stop_loss_pct".to_string(), self.config.stop_loss_pct.to_string());
        params.insert("take_profit_pct".to_string(), self.config.take_profit_pct.to_string());
        params
    }
}

/// Strategy metadata
#[derive(Debug, Clone)]
pub struct StrategyMetadata {
    pub name: String,
    pub version: String,
    pub description: String,
    pub author: String,
    pub required_indicators: Vec<String>,
    pub parameters: HashMap<String, String>,
}

/// WASM ABI functions that the engine will call
#[no_mangle]
pub extern "C" fn strategy_init(config_ptr: *const u8, config_len: usize) -> *mut EmaRsiStrategy {
    let config_bytes = unsafe { std::slice::from_raw_parts(config_ptr, config_len) };
    let config: StrategyConfig = serde_json::from_slice(config_bytes)
        .unwrap_or_default();
    
    let strategy = EmaRsiStrategy::new(config);
    Box::into_raw(Box::new(strategy))
}

#[no_mangle]
pub extern "C" fn strategy_process_bar(
    strategy_ptr: *mut EmaRsiStrategy,
    bar_ptr: *const u8,
    bar_len: usize,
    ema_ptr: *const u8,
    ema_len: usize,
    rsi_ptr: *const u8,
    rsi_len: usize,
) -> *mut u8 {
    let strategy = unsafe { &mut *strategy_ptr };
    let bar_bytes = unsafe { std::slice::from_raw_parts(bar_ptr, bar_len) };
    let ema_bytes = unsafe { std::slice::from_raw_parts(ema_ptr, ema_len) };
    let rsi_bytes = unsafe { std::slice::from_raw_parts(rsi_ptr, rsi_len) };
    
    let bar: MarketBar = serde_json::from_slice(bar_bytes).unwrap();
    let ema_values: Vec<IndicatorValue> = serde_json::from_slice(ema_bytes).unwrap();
    let rsi_values: Vec<IndicatorValue> = serde_json::from_slice(rsi_bytes).unwrap();
    
    let signals = strategy.process_bar(&bar, &ema_values, &rsi_values);
    let signals_json = serde_json::to_vec(&signals).unwrap();
    
    Box::into_raw(signals_json.into_boxed_slice()) as *mut u8
}

#[no_mangle]
pub extern "C" fn strategy_get_metadata(strategy_ptr: *mut EmaRsiStrategy) -> *mut u8 {
    let strategy = unsafe { &*strategy_ptr };
    let metadata = strategy.get_metadata();
    let metadata_json = serde_json::to_vec(&metadata).unwrap();
    Box::into_raw(metadata_json.into_boxed_slice()) as *mut u8
}

#[no_mangle]
pub extern "C" fn strategy_destroy(strategy_ptr: *mut EmaRsiStrategy) {
    unsafe {
        drop(Box::from_raw(strategy_ptr));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_strategy_initialization() {
        let config = StrategyConfig::default();
        let strategy = EmaRsiStrategy::new(config);
        assert_eq!(strategy.config.ema_period, 20);
        assert_eq!(strategy.config.rsi_period, 14);
    }
    
    #[test]
    fn test_long_entry_conditions() {
        let config = StrategyConfig::default();
        let mut strategy = EmaRsiStrategy::new(config);
        
        let bar = MarketBar {
            timestamp: 1000,
            open: 100.0,
            high: 105.0,
            low: 95.0,
            close: 102.0,
            volume: 1000.0,
        };
        
        let ema_values = vec![
            IndicatorValue { timestamp: 999, value: 100.0 },
            IndicatorValue { timestamp: 1000, value: 101.0 },
        ];
        
        let rsi_values = vec![
            IndicatorValue { timestamp: 999, value: 35.0 },
            IndicatorValue { timestamp: 1000, value: 25.0 }, // Oversold
        ];
        
        let signals = strategy.process_bar(&bar, &ema_values, &rsi_values);
        assert_eq!(signals.len(), 1);
        assert!(matches!(signals[0].side, TradeSide::Buy));
    }
}

