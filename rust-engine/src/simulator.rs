//! Exchange simulator with precision handling and fee calculation
//! 
//! Simulates exchange behavior including order matching, fee calculation,
//! slippage modeling, and position tracking with deterministic precision.

use std::collections::HashMap;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use anyhow::Result;
use tracing::{debug, warn, error};

use crate::types::*;
use crate::trade_table::TradeTableGenerator;

/// Exchange simulator for backtesting
pub struct ExchangeSimulator {
    positions: HashMap<String, Position>,
    equity_history: Vec<EquityPoint>,
    current_equity: Decimal,
    max_drawdown: Decimal,
    peak_equity: Decimal,
    trade_table_generator: TradeTableGenerator,
}

impl ExchangeSimulator {
    pub fn new() -> Result<Self> {
        Ok(Self {
            positions: HashMap::new(),
            equity_history: Vec::new(),
            current_equity: dec!(10000.0), // Starting equity
            max_drawdown: dec!(0.0),
            peak_equity: dec!(10000.0),
            trade_table_generator: TradeTableGenerator::new(),
        })
    }
    
    /// Simulate trading with given market data and strategy signals
    pub async fn simulate(
        &mut self,
        market_data: &MarketData,
        indicator_values: &HashMap<String, Vec<IndicatorValue>>,
        strategy: &crate::wasm::Strategy,
        intrabar_policy: &IntrabarPolicy,
        slippage_mode: &SlippageMode,
    ) -> Result<SimulationResult> {
        debug!("Starting simulation for symbol: {}", market_data.symbol);
        
        let mut trades = Vec::new();
        let mut positions = Vec::new();
        
        // Process each bar
        for (bar_idx, bar) in market_data.bars.iter().enumerate() {
            // Get strategy signals for this bar
            let signals = self.get_strategy_signals(
                strategy,
                bar,
                indicator_values,
                bar_idx,
            ).await?;
            
            // Process intrabar simulation
            let bar_trades = self.simulate_intrabar(
                bar,
                &signals,
                intrabar_policy,
                slippage_mode,
                &market_data.rules,
            ).await?;
            
            trades.extend(bar_trades);
            
            // Process bar with trade table generator
            self.trade_table_generator.process_bar(
                bar,
                &signals,
                intrabar_policy,
                slippage_mode,
                &market_data.rules,
            )?;
            
            // Update positions and equity
            self.update_positions(&market_data.symbol, bar.timestamp)?;
            self.update_equity(bar.timestamp);
            
            // Record position snapshot
            if let Some(position) = self.positions.get(&market_data.symbol) {
                positions.push(position.clone());
            }
        }
        
        Ok(SimulationResult {
            trades,
            positions,
            equity_curve: self.equity_history.clone(),
            max_drawdown: self.max_drawdown,
            exposure: self.calculate_exposure(),
            attribution: self.calculate_attribution(),
        })
    }
    
    /// Get strategy signals for a given bar
    async fn get_strategy_signals(
        &self,
        strategy: &crate::wasm::Strategy,
        bar: &Bar,
        indicator_values: &HashMap<String, Vec<IndicatorValue>>,
        bar_idx: usize,
    ) -> Result<Vec<StrategySignal>> {
        // This would call the WASM strategy with current market state
        // For now, return empty signals
        Ok(Vec::new())
    }
    
    /// Simulate intrabar trading
    async fn simulate_intrabar(
        &mut self,
        bar: &Bar,
        signals: &[StrategySignal],
        intrabar_policy: &IntrabarPolicy,
        slippage_mode: &SlippageMode,
        rules: &ExchangeRules,
    ) -> Result<Vec<ExecutedTrade>> {
        let mut trades = Vec::new();
        
        match intrabar_policy {
            IntrabarPolicy::ExactTrades => {
                // Use exact trade paths - would require trade data
                trades.extend(self.simulate_exact_trades(bar, signals, slippage_mode, rules).await?);
            },
            IntrabarPolicy::OneSecondBars => {
                // Use 1s bars with fixed path order
                trades.extend(self.simulate_one_second_bars(bar, signals, slippage_mode, rules).await?);
            },
            IntrabarPolicy::LinearInterpolation => {
                // Linear interpolation between OHLC
                trades.extend(self.simulate_linear_interpolation(bar, signals, slippage_mode, rules).await?);
            },
        }
        
        Ok(trades)
    }
    
    /// Simulate exact trade execution
    async fn simulate_exact_trades(
        &mut self,
        bar: &Bar,
        signals: &[StrategySignal],
        slippage_mode: &SlippageMode,
        rules: &ExchangeRules,
    ) -> Result<Vec<ExecutedTrade>> {
        let mut trades = Vec::new();
        
        // This would use actual trade data for precise execution
        // For now, simulate at bar close with slippage
        for signal in signals {
            let executed_price = self.calculate_execution_price(
                bar.close,
                &signal.side,
                slippage_mode,
                rules,
            )?;
            
            let fee = self.calculate_fee(signal.size, executed_price, rules)?;
            let slippage = (executed_price - bar.close).abs();
            
            trades.push(ExecutedTrade {
                timestamp: bar.timestamp,
                symbol: "BTCUSDT".to_string(), // Would come from context
                side: signal.side.clone(),
                quantity: signal.size,
                price: executed_price,
                fee,
                slippage,
                reason_code: "strategy_signal".to_string(),
            });
        }
        
        Ok(trades)
    }
    
    /// Simulate using 1-second bars
    async fn simulate_one_second_bars(
        &mut self,
        bar: &Bar,
        signals: &[StrategySignal],
        slippage_mode: &SlippageMode,
        rules: &ExchangeRules,
    ) -> Result<Vec<ExecutedTrade>> {
        let mut trades = Vec::new();
        
        // Simulate 60 1-second executions within the minute bar
        let bar_duration_ms = 60000; // 1 minute
        let second_duration_ms = 1000;
        
        for (second, signal) in signals.iter().enumerate() {
            if second >= 60 {
                break; // Limit to 60 seconds
            }
            
            let timestamp = bar.timestamp + (second * second_duration_ms) as u64;
            
            // Interpolate price within the bar
            let progress = second as f64 / 60.0;
            let interpolated_price = bar.open + (bar.close - bar.open) * Decimal::from_f64(progress).unwrap_or(dec!(0.0));
            
            let executed_price = self.calculate_execution_price(
                interpolated_price,
                &signal.side,
                slippage_mode,
                rules,
            )?;
            
            let fee = self.calculate_fee(signal.size, executed_price, rules)?;
            let slippage = (executed_price - interpolated_price).abs();
            
            trades.push(ExecutedTrade {
                timestamp,
                symbol: "BTCUSDT".to_string(),
                side: signal.side.clone(),
                quantity: signal.size,
                price: executed_price,
                fee,
                slippage,
                reason_code: "one_second_bar".to_string(),
            });
        }
        
        Ok(trades)
    }
    
    /// Simulate using linear interpolation
    async fn simulate_linear_interpolation(
        &mut self,
        bar: &Bar,
        signals: &[StrategySignal],
        slippage_mode: &SlippageMode,
        rules: &ExchangeRules,
    ) -> Result<Vec<ExecutedTrade>> {
        let mut trades = Vec::new();
        
        // Simple linear interpolation from open to close
        for (i, signal) in signals.iter().enumerate() {
            let progress = if signals.len() > 1 {
                i as f64 / (signals.len() - 1) as f64
            } else {
                0.0
            };
            
            let interpolated_price = bar.open + (bar.close - bar.open) * Decimal::from_f64(progress).unwrap_or(dec!(0.0));
            
            let executed_price = self.calculate_execution_price(
                interpolated_price,
                &signal.side,
                slippage_mode,
                rules,
            )?;
            
            let fee = self.calculate_fee(signal.size, executed_price, rules)?;
            let slippage = (executed_price - interpolated_price).abs();
            
            trades.push(ExecutedTrade {
                timestamp: bar.timestamp,
                symbol: "BTCUSDT".to_string(),
                side: signal.side.clone(),
                quantity: signal.size,
                price: executed_price,
                fee,
                slippage,
                reason_code: "linear_interpolation".to_string(),
            });
        }
        
        Ok(trades)
    }
    
    /// Calculate execution price with slippage
    fn calculate_execution_price(
        &self,
        base_price: Decimal,
        side: &TradeSide,
        slippage_mode: &SlippageMode,
        rules: &ExchangeRules,
    ) -> Result<Decimal> {
        let slippage = match slippage_mode {
            SlippageMode::None => dec!(0.0),
            SlippageMode::TradeSweep => {
                // Simulate trade sweep slippage (0.01% - 0.1%)
                let slippage_rate = dec!(0.0001); // 0.01%
                base_price * slippage_rate
            },
            SlippageMode::SyntheticBook => {
                // Simulate synthetic order book slippage (0.05% - 0.5%)
                let slippage_rate = dec!(0.0005); // 0.05%
                base_price * slippage_rate
            },
        };
        
        let execution_price = match side {
            TradeSide::Buy => base_price + slippage,
            TradeSide::Sell => base_price - slippage,
        };
        
        // Quantize to tick size
        let quantized_price = self.quantize_price(execution_price, rules)?;
        
        Ok(quantized_price)
    }
    
    /// Calculate trading fees
    fn calculate_fee(
        &self,
        quantity: Decimal,
        price: Decimal,
        rules: &ExchangeRules,
    ) -> Result<Decimal> {
        let notional = quantity * price;
        let fee_rate = rules.taker_fee; // Assume taker for simplicity
        let fee = notional * fee_rate;
        
        // Quantize fee to precision
        let quantized_fee = self.quantize_fee(fee, rules)?;
        
        Ok(quantized_fee)
    }
    
    /// Quantize price to tick size
    fn quantize_price(&self, price: Decimal, rules: &ExchangeRules) -> Result<Decimal> {
        let quantized = (price / rules.tick_size).round() * rules.tick_size;
        Ok(quantized)
    }
    
    /// Quantize quantity to lot size
    fn quantize_quantity(&self, quantity: Decimal, rules: &ExchangeRules) -> Result<Decimal> {
        let quantized = (quantity / rules.lot_size).round() * rules.lot_size;
        Ok(quantized)
    }
    
    /// Quantize fee to precision
    fn quantize_fee(&self, fee: Decimal, rules: &ExchangeRules) -> Result<Decimal> {
        let precision = Decimal::from(10_u64.pow(rules.precision_price as u32));
        let quantized = (fee * precision).round() / precision;
        Ok(quantized)
    }
    
    /// Update positions after trade execution
    fn update_positions(&mut self, symbol: &str, timestamp: u64) -> Result<()> {
        // This would update positions based on executed trades
        // For now, maintain current position
        if let Some(position) = self.positions.get_mut(symbol) {
            position.timestamp = timestamp;
            // Update unrealized PnL based on current market price
        }
        
        Ok(())
    }
    
    /// Update equity curve
    fn update_equity(&mut self, timestamp: u64) {
        // Calculate total equity from all positions
        let mut total_equity = self.current_equity;
        
        for position in self.positions.values() {
            total_equity += position.realized_pnl;
            total_equity += position.unrealized_pnl;
        }
        
        // Update peak equity and drawdown
        if total_equity > self.peak_equity {
            self.peak_equity = total_equity;
        }
        
        let current_drawdown = (self.peak_equity - total_equity) / self.peak_equity;
        if current_drawdown > self.max_drawdown {
            self.max_drawdown = current_drawdown;
        }
        
        // Record equity point
        self.equity_history.push(EquityPoint {
            timestamp,
            equity: total_equity,
            drawdown: current_drawdown,
            exposure: self.calculate_exposure(),
        });
        
        self.current_equity = total_equity;
    }
    
    /// Calculate current exposure
    fn calculate_exposure(&self) -> Decimal {
        self.positions.values()
            .map(|p| p.quantity.abs() * p.avg_price)
            .sum()
    }
    
    /// Calculate attribution by rule/signal
    fn calculate_attribution(&self) -> HashMap<String, Decimal> {
        let mut attribution = HashMap::new();
        
        // This would calculate attribution based on strategy rules
        attribution.insert("momentum".to_string(), dec!(0.0));
        attribution.insert("mean_reversion".to_string(), dec!(0.0));
        attribution.insert("breakout".to_string(), dec!(0.0));
        
        attribution
    }
    
    /// Get trade table result
    pub fn get_trade_table_result(&self) -> TradeTableResult {
        self.trade_table_generator.generate_result()
    }
}

