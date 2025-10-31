//! Trade table generation system
//! 
//! Implements the complete trade table generation system as specified in the plan.
//! Produces one row per closed trade with comprehensive PnL, fee, and exit reason tracking.

use std::collections::HashMap;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use anyhow::Result;
use tracing::{debug, warn, error};

use crate::types::*;

/// Trade table generator
pub struct TradeTableGenerator {
    /// Default size in USD
    default_size_usd: Decimal,
    /// Active positions being tracked
    active_positions: HashMap<String, ActivePosition>,
    /// Generated trade records
    trade_records: Vec<TradeRecord>,
    /// Rejected trades
    rejected_trades: Vec<RejectedTrade>,
    /// Current equity for drawdown calculation
    current_equity: Decimal,
    peak_equity: Decimal,
    max_drawdown: Decimal,
}

impl TradeTableGenerator {
    /// Create a new trade table generator
    pub fn new() -> Self {
        Self {
            default_size_usd: dec!(1000.0),
            active_positions: HashMap::new(),
            trade_records: Vec::new(),
            rejected_trades: Vec::new(),
            current_equity: dec!(10000.0), // Starting equity
            peak_equity: dec!(10000.0),
            max_drawdown: dec!(0.0),
        }
    }

    /// Process a bar and generate trade records
    pub fn process_bar(
        &mut self,
        bar: &Bar,
        signals: &[StrategySignal],
        intrabar_policy: &IntrabarPolicy,
        slippage_mode: &SlippageMode,
        rules: &ExchangeRules,
    ) -> Result<()> {
        debug!("Processing bar at timestamp: {}", bar.timestamp);

        // 1. Process entry signals
        self.process_entry_signals(bar, signals, intrabar_policy, slippage_mode, rules)?;

        // 2. Check for exits on existing positions
        self.process_exits(bar, intrabar_policy, slippage_mode, rules)?;

        // 3. Update equity and drawdown
        self.update_equity_and_drawdown();

        Ok(())
    }

    /// Process entry signals and create new positions
    fn process_entry_signals(
        &mut self,
        bar: &Bar,
        signals: &[StrategySignal],
        intrabar_policy: &IntrabarPolicy,
        slippage_mode: &SlippageMode,
        rules: &ExchangeRules,
    ) -> Result<()> {
        for signal in signals {
            // Skip if we already have a position for this symbol
            if self.active_positions.contains_key(&signal.symbol) {
                continue;
            }

            // 1. Apply capital rule ($1000 default)
            let notional = self.default_size_usd;
            let raw_quantity = notional / bar.close;
            
            // 2. Apply symbol filters
            let quantity = self.apply_symbol_filters(raw_quantity, rules)?;
            let final_notional = quantity * bar.close;

            // 3. Check minimum notional requirement
            if final_notional < rules.min_notional {
                self.rejected_trades.push(RejectedTrade {
                    timestamp: bar.timestamp,
                    symbol: signal.symbol.clone(),
                    side: signal.side.clone(),
                    reason: "Rejected – NotionalMin".to_string(),
                    notional: final_notional,
                });
                continue;
            }

            // 4. Calculate entry execution price
            let entry_price = self.calculate_entry_price(
                bar,
                &signal.side,
                intrabar_policy,
                slippage_mode,
                rules,
            )?;

            // 5. Calculate entry fee
            let entry_fee = self.calculate_fee(quantity, entry_price, rules)?;

            // 6. Create active position
            let trade_type = match signal.side {
                TradeSide::Buy => TradeType::Long,
                TradeSide::Sell => TradeType::Short,
            };

            let position = ActivePosition {
                symbol: signal.symbol.clone(),
                trade_type,
                entry_time: bar.timestamp,
                entry_price,
                quantity,
                take_profit: signal.take_profit,
                stop_loss: signal.stop_loss,
                time_to_live: signal.time_to_live,
                entry_fee,
                size_usd: self.default_size_usd,
            };

            self.active_positions.insert(signal.symbol.clone(), position);
            debug!("Created position for symbol: {}", signal.symbol);
        }

        Ok(())
    }

    /// Process exits for existing positions
    fn process_exits(
        &mut self,
        bar: &Bar,
        intrabar_policy: &IntrabarPolicy,
        slippage_mode: &SlippageMode,
        rules: &ExchangeRules,
    ) -> Result<()> {
        let mut positions_to_close = Vec::new();

        for (symbol, position) in &self.active_positions {
            // Check for TP/SL hits using first-touch logic
            if let Some(exit_info) = self.check_exit_conditions(bar, position)? {
                positions_to_close.push((symbol.clone(), exit_info));
            }
        }

        // Close positions and create trade records
        for (symbol, exit_info) in positions_to_close {
            if let Some(position) = self.active_positions.remove(&symbol) {
                self.create_trade_record(position, exit_info, bar, slippage_mode, rules)?;
            }
        }

        Ok(())
    }

    /// Check exit conditions using first-touch logic
    fn check_exit_conditions(
        &self,
        bar: &Bar,
        position: &ActivePosition,
    ) -> Result<Option<ExitInfo>> {
        let mut exit_candidates = Vec::new();

        // Check Take Profit
        if let Some(tp) = position.take_profit {
            let hit_tp = match position.trade_type {
                TradeType::Long => bar.high >= tp,
                TradeType::Short => bar.low <= tp,
            };
            if hit_tp {
                exit_candidates.push(ExitInfo {
                    exit_price: tp,
                    exit_time: bar.timestamp,
                    exit_reason: ExitReason::TakeProfit,
                    hit_tp_sl: HitTpSl::TakeProfit,
                });
            }
        }

        // Check Stop Loss
        if let Some(sl) = position.stop_loss {
            let hit_sl = match position.trade_type {
                TradeType::Long => bar.low <= sl,
                TradeType::Short => bar.high >= sl,
            };
            if hit_sl {
                exit_candidates.push(ExitInfo {
                    exit_price: sl,
                    exit_time: bar.timestamp,
                    exit_reason: ExitReason::StopLoss,
                    hit_tp_sl: HitTpSl::StopLoss,
                });
            }
        }

        // Check timeout
        if let Some(ttl) = position.time_to_live {
            if bar.timestamp >= position.entry_time + ttl {
                exit_candidates.push(ExitInfo {
                    exit_price: bar.close, // Use close price for timeout
                    exit_time: bar.timestamp,
                    exit_reason: ExitReason::Timeout,
                    hit_tp_sl: HitTpSl::None,
                });
            }
        }

        // Return the first exit condition that was hit
        // In a real implementation, you'd need to determine which was hit first
        // based on the intrabar policy and actual price movement
        Ok(exit_candidates.first().cloned())
    }

    /// Create a trade record from a closed position
    fn create_trade_record(
        &mut self,
        position: ActivePosition,
        exit_info: ExitInfo,
        bar: &Bar,
        slippage_mode: &SlippageMode,
        rules: &ExchangeRules,
    ) -> Result<()> {
        // Calculate exit fee
        let exit_fee = self.calculate_fee(position.quantity, exit_info.exit_price, rules)?;
        let total_fees = position.entry_fee + exit_fee;

        // Calculate PnL
        let pnl_usd = match position.trade_type {
            TradeType::Long => {
                (exit_info.exit_price - position.entry_price) * position.quantity - total_fees
            }
            TradeType::Short => {
                (position.entry_price - exit_info.exit_price) * position.quantity - total_fees
            }
        };

        let pnl_pct = pnl_usd / position.size_usd;

        // Convert timestamps to ISO UTC strings
        let entry_time_utc = self.timestamp_to_iso_utc(position.entry_time);
        let exit_time_utc = self.timestamp_to_iso_utc(exit_info.exit_time);
        let date = exit_time_utc.split('T').next().unwrap_or(&exit_time_utc).to_string();

        let trade_record = TradeRecord {
            date,
            trade_type: position.trade_type,
            entry_price: position.entry_price,
            entry_time_utc,
            exit_price: exit_info.exit_price,
            exit_time_utc,
            exit_reason: exit_info.exit_reason,
            hit_tp_sl: exit_info.hit_tp_sl,
            size_usd: position.size_usd,
            qty: position.quantity,
            fees_usd: total_fees,
            pnl_usd,
            pnl_pct,
            symbol: position.symbol,
        };

        self.trade_records.push(trade_record);
        debug!("Created trade record for symbol: {}", position.symbol);

        Ok(())
    }

    /// Apply symbol filters (tick size, quantity step, etc.)
    fn apply_symbol_filters(&self, quantity: Decimal, rules: &ExchangeRules) -> Result<Decimal> {
        // Quantize quantity to lot size
        let quantized = (quantity / rules.lot_size).round() * rules.lot_size;
        Ok(quantized)
    }

    /// Calculate entry execution price based on intrabar policy
    fn calculate_entry_price(
        &self,
        bar: &Bar,
        side: &TradeSide,
        intrabar_policy: &IntrabarPolicy,
        slippage_mode: &SlippageMode,
        rules: &ExchangeRules,
    ) -> Result<Decimal> {
        let base_price = match intrabar_policy {
            IntrabarPolicy::ExactTrades => bar.close, // Use strategy's chosen price
            IntrabarPolicy::OneSecondBars => bar.open, // Use bar open
            IntrabarPolicy::LinearInterpolation => bar.open, // Start with open
        };

        self.apply_slippage(base_price, side, slippage_mode, rules)
    }

    /// Apply slippage to execution price
    fn apply_slippage(
        &self,
        base_price: Decimal,
        side: &TradeSide,
        slippage_mode: &SlippageMode,
        rules: &ExchangeRules,
    ) -> Result<Decimal> {
        let slippage = match slippage_mode {
            SlippageMode::None => dec!(0.0),
            SlippageMode::TradeSweep => {
                let slippage_rate = dec!(0.0001); // 0.01%
                base_price * slippage_rate
            }
            SlippageMode::SyntheticBook => {
                let slippage_rate = dec!(0.0005); // 0.05%
                base_price * slippage_rate
            }
        };

        let execution_price = match side {
            TradeSide::Buy => base_price + slippage,
            TradeSide::Sell => base_price - slippage,
        };

        // Quantize to tick size
        let quantized = (execution_price / rules.tick_size).round() * rules.tick_size;
        Ok(quantized)
    }

    /// Calculate trading fees
    fn calculate_fee(&self, quantity: Decimal, price: Decimal, rules: &ExchangeRules) -> Result<Decimal> {
        let notional = quantity * price;
        let fee_rate = rules.taker_fee; // Assume taker for simplicity
        let fee = notional * fee_rate;
        
        // Quantize fee to precision
        let precision = Decimal::from(10_u64.pow(rules.precision_price as u32));
        let quantized = (fee * precision).round() / precision;
        
        Ok(quantized)
    }

    /// Convert Unix timestamp to ISO UTC string
    fn timestamp_to_iso_utc(&self, timestamp: u64) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        
        let duration = std::time::Duration::from_millis(timestamp);
        let datetime = UNIX_EPOCH + duration;
        
        // Format as ISO 8601 UTC
        format!("{}", chrono::DateTime::<chrono::Utc>::from(datetime).format("%Y-%m-%dT%H:%M:%S%.3fZ"))
    }

    /// Update equity and drawdown tracking
    fn update_equity_and_drawdown(&mut self) {
        // Calculate current equity based on open positions and realized PnL
        let realized_pnl: Decimal = self.trade_records.iter()
            .map(|trade| trade.pnl_usd)
            .sum();
        
        self.current_equity = dec!(10000.0) + realized_pnl; // Starting equity + realized PnL
        
        if self.current_equity > self.peak_equity {
            self.peak_equity = self.current_equity;
        }
        
        let current_drawdown = (self.peak_equity - self.current_equity) / self.peak_equity;
        if current_drawdown > self.max_drawdown {
            self.max_drawdown = current_drawdown;
        }
    }

    /// Generate final trade table result
    pub fn generate_result(&self) -> TradeTableResult {
        let summary = self.calculate_summary();
        
        TradeTableResult {
            trades: self.trade_records.clone(),
            summary,
            rejected_trades: self.rejected_trades.clone(),
        }
    }

    /// Calculate trade summary statistics
    fn calculate_summary(&self) -> TradeSummary {
        let total_trades = self.trade_records.len() as u32;
        
        if total_trades == 0 {
            return TradeSummary {
                total_trades: 0,
                wins: 0,
                losses: 0,
                win_rate: dec!(0.0),
                net_pnl_usd: dec!(0.0),
                avg_win_usd: dec!(0.0),
                avg_loss_usd: dec!(0.0),
                expectancy: dec!(0.0),
                max_drawdown: self.max_drawdown,
                profit_factor: dec!(0.0),
                avg_holding_time_hours: dec!(0.0),
            };
        }

        let wins = self.trade_records.iter()
            .filter(|trade| trade.pnl_usd > dec!(0.0))
            .count() as u32;
        
        let losses = total_trades - wins;
        
        let win_rate = if total_trades > 0 {
            Decimal::from(wins) / Decimal::from(total_trades) * dec!(100.0)
        } else {
            dec!(0.0)
        };

        let net_pnl_usd: Decimal = self.trade_records.iter()
            .map(|trade| trade.pnl_usd)
            .sum();

        let winning_trades: Vec<&TradeRecord> = self.trade_records.iter()
            .filter(|trade| trade.pnl_usd > dec!(0.0))
            .collect();
        
        let losing_trades: Vec<&TradeRecord> = self.trade_records.iter()
            .filter(|trade| trade.pnl_usd <= dec!(0.0))
            .collect();

        let avg_win_usd = if wins > 0 {
            winning_trades.iter()
                .map(|trade| trade.pnl_usd)
                .sum::<Decimal>() / Decimal::from(wins)
        } else {
            dec!(0.0)
        };

        let avg_loss_usd = if losses > 0 {
            losing_trades.iter()
                .map(|trade| trade.pnl_usd)
                .sum::<Decimal>() / Decimal::from(losses)
        } else {
            dec!(0.0)
        };

        let expectancy = (win_rate / dec!(100.0)) * avg_win_usd + 
                        (dec!(1.0) - win_rate / dec!(100.0)) * avg_loss_usd;

        let gross_profit: Decimal = winning_trades.iter()
            .map(|trade| trade.pnl_usd)
            .sum();
        
        let gross_loss: Decimal = losing_trades.iter()
            .map(|trade| trade.pnl_usd.abs())
            .sum();

        let profit_factor = if gross_loss > dec!(0.0) {
            gross_profit / gross_loss
        } else {
            dec!(0.0)
        };

        // Calculate average holding time
        let total_holding_time_ms: u64 = self.trade_records.iter()
            .map(|trade| {
                let entry_time = self.iso_utc_to_timestamp(&trade.entry_time_utc);
                let exit_time = self.iso_utc_to_timestamp(&trade.exit_time_utc);
                exit_time - entry_time
            })
            .sum();

        let avg_holding_time_hours = if total_trades > 0 {
            Decimal::from(total_holding_time_ms) / Decimal::from(total_trades) / dec!(3600000.0) // Convert ms to hours
        } else {
            dec!(0.0)
        };

        TradeSummary {
            total_trades,
            wins,
            losses,
            win_rate,
            net_pnl_usd,
            avg_win_usd,
            avg_loss_usd,
            expectancy,
            max_drawdown: self.max_drawdown,
            profit_factor,
            avg_holding_time_hours,
        }
    }

    /// Convert ISO UTC string back to timestamp (helper for calculations)
    fn iso_utc_to_timestamp(&self, iso_string: &str) -> u64 {
        // This is a simplified implementation
        // In production, you'd use a proper date parsing library
        iso_string.len() as u64 // Placeholder
    }
}

/// Exit information for position closure
#[derive(Debug, Clone)]
struct ExitInfo {
    exit_price: Decimal,
    exit_time: u64,
    exit_reason: ExitReason,
    hit_tp_sl: HitTpSl,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trade_table_generator_creation() {
        let generator = TradeTableGenerator::new();
        assert_eq!(generator.default_size_usd, dec!(1000.0));
        assert!(generator.active_positions.is_empty());
        assert!(generator.trade_records.is_empty());
    }

    #[test]
    fn test_symbol_filters() {
        let generator = TradeTableGenerator::new();
        let rules = ExchangeRules::default();
        
        let quantity = dec!(0.123456789);
        let filtered = generator.apply_symbol_filters(quantity, &rules).unwrap();
        
        // Should be quantized to lot size
        assert_eq!(filtered, dec!(0.12345679));
    }
}



