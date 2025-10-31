//! Trade Table Generation Demo
//! 
//! Demonstrates the complete trade table generation system with sample data.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use anyhow::Result;
use tracing::{info, debug};

use backtest_engine::types::*;
use backtest_engine::trade_table::TradeTableGenerator;
use backtest_engine::export::{ExportConfig, ExportFormat, TradeTableExporter};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();
    
    info!("Starting Trade Table Generation Demo");

    // Create sample market data
    let market_data = create_sample_market_data();
    
    // Create sample strategy signals
    let signals = create_sample_signals();
    
    // Create trade table generator
    let mut generator = TradeTableGenerator::new();
    
    // Process each bar
    for (i, bar) in market_data.bars.iter().enumerate() {
        debug!("Processing bar {} at timestamp: {}", i, bar.timestamp);
        
        // Get signals for this bar (simplified - in real usage, this would come from strategy)
        let bar_signals = if i == 10 { // Entry signal at bar 10
            vec![StrategySignal {
                side: TradeSide::Buy,
                size: dec!(1000.0), // $1000 position
                entry_price: Some(bar.close),
                take_profit: Some(bar.close * dec!(1.05)), // 5% TP
                stop_loss: Some(bar.close * dec!(0.95)),   // 5% SL
                time_to_live: Some(3600000), // 1 hour TTL
            }]
        } else if i == 20 { // Exit signal at bar 20
            vec![StrategySignal {
                side: TradeSide::Sell,
                size: dec!(1000.0),
                entry_price: Some(bar.close),
                take_profit: None,
                stop_loss: None,
                time_to_live: None,
            }]
        } else {
            vec![]
        };
        
        // Process the bar
        generator.process_bar(
            bar,
            &bar_signals,
            &IntrabarPolicy::ExactTrades,
            &SlippageMode::TradeSweep,
            &market_data.rules,
        )?;
    }
    
    // Generate final result
    let result = generator.generate_result();
    
    info!("Trade Table Generation Complete!");
    info!("Total trades: {}", result.trades.len());
    info!("Rejected trades: {}", result.rejected_trades.len());
    
    // Print summary
    print_summary(&result.summary);
    
    // Export to CSV
    let export_config = ExportConfig {
        format: ExportFormat::Csv,
        output_path: Some("demo_trade_table.csv".to_string()),
        clickhouse_url: None,
        clickhouse_database: None,
        clickhouse_table: None,
    };
    
    let exporter = TradeTableExporter::new(export_config);
    exporter.export(&result).await?;
    
    info!("Demo completed successfully!");
    Ok(())
}

fn create_sample_market_data() -> MarketData {
    let mut bars = Vec::new();
    let base_price = dec!(50000.0);
    
    // Create 30 bars of sample data
    for i in 0..30 {
        let timestamp = 1609459200000 + (i * 60000) as u64; // 1-minute bars starting from 2021-01-01
        let price_change = Decimal::from(i as i32) * dec!(100.0); // $100 price change per bar
        let price = base_price + price_change;
        
        bars.push(Bar {
            timestamp,
            open: price,
            high: price * dec!(1.01), // 1% higher
            low: price * dec!(0.99),  // 1% lower
            close: price,
            volume: dec!(1000.0),
            trade_count: 100,
        });
    }
    
    MarketData {
        symbol: "BTCUSDT".to_string(),
        timeframe: "1m".to_string(),
        bars,
        trades: Vec::new(),
        rules: ExchangeRules::default(),
    }
}

fn create_sample_signals() -> Vec<StrategySignal> {
    vec![
        StrategySignal {
            side: TradeSide::Buy,
            size: dec!(1000.0),
            entry_price: Some(dec!(50000.0)),
            take_profit: Some(dec!(52500.0)), // 5% TP
            stop_loss: Some(dec!(47500.0)),   // 5% SL
            time_to_live: Some(3600000), // 1 hour
        }
    ]
}

fn print_summary(summary: &TradeSummary) {
    println!("\n=== TRADE SUMMARY ===");
    println!("Total Trades: {}", summary.total_trades);
    println!("Wins: {}", summary.wins);
    println!("Losses: {}", summary.losses);
    println!("Win Rate: {:.2}%", summary.win_rate);
    println!("Net PnL: ${:.2}", summary.net_pnl_usd);
    println!("Average Win: ${:.2}", summary.avg_win_usd);
    println!("Average Loss: ${:.2}", summary.avg_loss_usd);
    println!("Expectancy: ${:.2}", summary.expectancy);
    println!("Max Drawdown: {:.2}%", summary.max_drawdown * dec!(100.0));
    println!("Profit Factor: {:.2}", summary.profit_factor);
    println!("Avg Holding Time: {:.2} hours", summary.avg_holding_time_hours);
    println!("===================\n");
}



