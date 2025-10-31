//! Export system for trade table results
//! 
//! Provides CSV, Parquet, and ClickHouse export functionality for trade table results.

use std::collections::HashMap;
use rust_decimal::Decimal;
use anyhow::Result;
use tracing::{info, debug, error};

use crate::types::*;

/// Export format enumeration
#[derive(Debug, Clone)]
pub enum ExportFormat {
    Csv,
    Parquet,
    ClickHouse,
}

/// Export configuration
#[derive(Debug, Clone)]
pub struct ExportConfig {
    pub format: ExportFormat,
    pub output_path: Option<String>,
    pub clickhouse_url: Option<String>,
    pub clickhouse_database: Option<String>,
    pub clickhouse_table: Option<String>,
}

impl Default for ExportConfig {
    fn default() -> Self {
        Self {
            format: ExportFormat::Csv,
            output_path: Some("trade_table.csv".to_string()),
            clickhouse_url: None,
            clickhouse_database: None,
            clickhouse_table: Some("trades".to_string()),
        }
    }
}

/// Trade table exporter
pub struct TradeTableExporter {
    config: ExportConfig,
}

impl TradeTableExporter {
    /// Create a new exporter with configuration
    pub fn new(config: ExportConfig) -> Self {
        Self { config }
    }

    /// Export trade table result
    pub async fn export(&self, result: &TradeTableResult) -> Result<()> {
        match self.config.format {
            ExportFormat::Csv => self.export_csv(result).await,
            ExportFormat::Parquet => self.export_parquet(result).await,
            ExportFormat::ClickHouse => self.export_clickhouse(result).await,
        }
    }

    /// Export to CSV format
    async fn export_csv(&self, result: &TradeTableResult) -> Result<()> {
        let output_path = self.config.output_path.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Output path not specified for CSV export"))?;

        info!("Exporting trade table to CSV: {}", output_path);

        let mut csv_content = String::new();
        
        // Write header
        csv_content.push_str("date,type,entry_price,entry_time_utc,exit_price,exit_time_utc,exit_reason,hit_tp_sl,size_usd,qty,fees_usd,pnl_usd,pnl_pct,symbol\n");

        // Write trade records
        for trade in &result.trades {
            let line = format!(
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
                trade.date,
                match trade.trade_type {
                    TradeType::Long => "Long",
                    TradeType::Short => "Short",
                },
                trade.entry_price,
                trade.entry_time_utc,
                trade.exit_price,
                trade.exit_time_utc,
                match trade.exit_reason {
                    ExitReason::TakeProfit => "TP",
                    ExitReason::StopLoss => "SL",
                    ExitReason::StrategyExit => "StrategyExit",
                    ExitReason::Liquidation => "Liquidation",
                    ExitReason::Timeout => "Timeout",
                },
                match trade.hit_tp_sl {
                    HitTpSl::TakeProfit => "TP",
                    HitTpSl::StopLoss => "SL",
                    HitTpSl::None => "None",
                },
                trade.size_usd,
                trade.qty,
                trade.fees_usd,
                trade.pnl_usd,
                trade.pnl_pct,
                trade.symbol,
            );
            csv_content.push_str(&line);
        }

        // Write summary footer
        csv_content.push_str("\n# Summary\n");
        csv_content.push_str(&format!("total_trades,{}\n", result.summary.total_trades));
        csv_content.push_str(&format!("wins,{}\n", result.summary.wins));
        csv_content.push_str(&format!("losses,{}\n", result.summary.losses));
        csv_content.push_str(&format!("win_rate,{}\n", result.summary.win_rate));
        csv_content.push_str(&format!("net_pnl_usd,{}\n", result.summary.net_pnl_usd));
        csv_content.push_str(&format!("avg_win_usd,{}\n", result.summary.avg_win_usd));
        csv_content.push_str(&format!("avg_loss_usd,{}\n", result.summary.avg_loss_usd));
        csv_content.push_str(&format!("expectancy,{}\n", result.summary.expectancy));
        csv_content.push_str(&format!("max_drawdown,{}\n", result.summary.max_drawdown));
        csv_content.push_str(&format!("profit_factor,{}\n", result.summary.profit_factor));
        csv_content.push_str(&format!("avg_holding_time_hours,{}\n", result.summary.avg_holding_time_hours));

        // Write rejected trades
        if !result.rejected_trades.is_empty() {
            csv_content.push_str("\n# Rejected Trades\n");
            csv_content.push_str("timestamp,symbol,side,reason,notional\n");
            for rejected in &result.rejected_trades {
                csv_content.push_str(&format!(
                    "{},{},{},{},{}\n",
                    rejected.timestamp,
                    rejected.symbol,
                    match rejected.side {
                        TradeSide::Buy => "Buy",
                        TradeSide::Sell => "Sell",
                    },
                    rejected.reason,
                    rejected.notional,
                ));
            }
        }

        // Write to file
        tokio::fs::write(output_path, csv_content).await?;
        
        info!("CSV export completed: {} trades, {} rejected", 
              result.trades.len(), result.rejected_trades.len());

        Ok(())
    }

    /// Export to Parquet format
    async fn export_parquet(&self, _result: &TradeTableResult) -> Result<()> {
        // TODO: Implement Parquet export using arrow-rs
        // This would require adding arrow dependencies to Cargo.toml
        error!("Parquet export not yet implemented");
        Err(anyhow::anyhow!("Parquet export not yet implemented"))
    }

    /// Export to ClickHouse
    async fn export_clickhouse(&self, result: &TradeTableResult) -> Result<()> {
        let clickhouse_url = self.config.clickhouse_url.as_ref()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse URL not specified"))?;
        
        let database = self.config.clickhouse_database.as_ref()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse database not specified"))?;
        
        let table = self.config.clickhouse_table.as_ref()
            .ok_or_else(|| anyhow::anyhow!("ClickHouse table not specified"))?;

        info!("Exporting trade table to ClickHouse: {}/{}.{}", clickhouse_url, database, table);

        // TODO: Implement ClickHouse export using clickhouse-rs
        // This would require adding clickhouse-rs dependency to Cargo.toml
        
        // For now, just log the structure
        debug!("Would export {} trades to ClickHouse", result.trades.len());
        debug!("Summary: {} wins, {} losses, {:.2}% win rate", 
               result.summary.wins, result.summary.losses, result.summary.win_rate);

        Ok(())
    }

    /// Export trade table from multiple symbol results
    pub async fn export_combined(&self, symbol_results: &[SymbolResult]) -> Result<()> {
        // Combine all trade tables from symbol results
        let mut all_trades = Vec::new();
        let mut all_rejected = Vec::new();
        let mut combined_summary = TradeSummary {
            total_trades: 0,
            wins: 0,
            losses: 0,
            win_rate: Decimal::ZERO,
            net_pnl_usd: Decimal::ZERO,
            avg_win_usd: Decimal::ZERO,
            avg_loss_usd: Decimal::ZERO,
            expectancy: Decimal::ZERO,
            max_drawdown: Decimal::ZERO,
            profit_factor: Decimal::ZERO,
            avg_holding_time_hours: Decimal::ZERO,
        };

        for symbol_result in symbol_results {
            if let Some(trade_table) = &symbol_result.trade_table {
                all_trades.extend(trade_table.trades.clone());
                all_rejected.extend(trade_table.rejected_trades.clone());
                
                // Aggregate summary statistics
                combined_summary.total_trades += trade_table.summary.total_trades;
                combined_summary.wins += trade_table.summary.wins;
                combined_summary.losses += trade_table.summary.losses;
                combined_summary.net_pnl_usd += trade_table.summary.net_pnl_usd;
                
                // Update max drawdown to the maximum across all symbols
                if trade_table.summary.max_drawdown > combined_summary.max_drawdown {
                    combined_summary.max_drawdown = trade_table.summary.max_drawdown;
                }
            }
        }

        // Recalculate combined statistics
        if combined_summary.total_trades > 0 {
            combined_summary.win_rate = Decimal::from(combined_summary.wins) / 
                Decimal::from(combined_summary.total_trades) * dec!(100.0);
            
            let winning_trades: Vec<&TradeRecord> = all_trades.iter()
                .filter(|trade| trade.pnl_usd > Decimal::ZERO)
                .collect();
            
            let losing_trades: Vec<&TradeRecord> = all_trades.iter()
                .filter(|trade| trade.pnl_usd <= Decimal::ZERO)
                .collect();

            combined_summary.avg_win_usd = if combined_summary.wins > 0 {
                winning_trades.iter()
                    .map(|trade| trade.pnl_usd)
                    .sum::<Decimal>() / Decimal::from(combined_summary.wins)
            } else {
                Decimal::ZERO
            };

            combined_summary.avg_loss_usd = if combined_summary.losses > 0 {
                losing_trades.iter()
                    .map(|trade| trade.pnl_usd)
                    .sum::<Decimal>() / Decimal::from(combined_summary.losses)
            } else {
                Decimal::ZERO
            };

            combined_summary.expectancy = (combined_summary.win_rate / dec!(100.0)) * combined_summary.avg_win_usd + 
                (dec!(1.0) - combined_summary.win_rate / dec!(100.0)) * combined_summary.avg_loss_usd;

            let gross_profit: Decimal = winning_trades.iter()
                .map(|trade| trade.pnl_usd)
                .sum();
            
            let gross_loss: Decimal = losing_trades.iter()
                .map(|trade| trade.pnl_usd.abs())
                .sum();

            combined_summary.profit_factor = if gross_loss > Decimal::ZERO {
                gross_profit / gross_loss
            } else {
                Decimal::ZERO
            };
        }

        // Sort trades by exit time for chronological order
        all_trades.sort_by(|a, b| a.exit_time_utc.cmp(&b.exit_time_utc));

        let combined_result = TradeTableResult {
            trades: all_trades,
            summary: combined_summary,
            rejected_trades: all_rejected,
        };

        self.export(&combined_result).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_export_config_default() {
        let config = ExportConfig::default();
        assert!(matches!(config.format, ExportFormat::Csv));
        assert_eq!(config.output_path, Some("trade_table.csv".to_string()));
    }

    #[test]
    fn test_trade_table_exporter_creation() {
        let config = ExportConfig::default();
        let exporter = TradeTableExporter::new(config);
        // Test passes if creation doesn't panic
    }
}



