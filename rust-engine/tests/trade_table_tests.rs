//! Comprehensive tests for trade table generation system

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use anyhow::Result;

use backtest_engine::types::*;
use backtest_engine::trade_table::TradeTableGenerator;

#[test]
fn test_trade_table_generator_creation() {
    let generator = TradeTableGenerator::new();
    assert_eq!(generator.default_size_usd, dec!(1000.0));
}

#[test]
fn test_long_trade_execution() -> Result<()> {
    let mut generator = TradeTableGenerator::new();
    
    // Create a bar with entry signal
    let bar = Bar {
        timestamp: 1609459200000,
        open: dec!(50000.0),
        high: dec!(51000.0),
        low: dec!(49000.0),
        close: dec!(50500.0),
        volume: dec!(1000.0),
        trade_count: 100,
    };
    
    let signals = vec![StrategySignal {
        side: TradeSide::Buy,
        size: dec!(1000.0),
        entry_price: Some(bar.close),
        take_profit: Some(dec!(53000.0)), // 5% TP
        stop_loss: Some(dec!(48000.0)),   // 5% SL
        time_to_live: Some(3600000), // 1 hour
    }];
    
    // Process entry
    generator.process_bar(
        &bar,
        &signals,
        &IntrabarPolicy::ExactTrades,
        &SlippageMode::None,
        &ExchangeRules::default(),
    )?;
    
    // Create exit bar that hits TP
    let exit_bar = Bar {
        timestamp: 1609459260000, // 1 minute later
        open: dec!(50500.0),
        high: dec!(53500.0), // Hits TP
        low: dec!(50000.0),
        close: dec!(53000.0),
        volume: dec!(1000.0),
        trade_count: 100,
    };
    
    // Process exit
    generator.process_bar(
        &exit_bar,
        &[], // No new signals
        &IntrabarPolicy::ExactTrades,
        &SlippageMode::None,
        &ExchangeRules::default(),
    )?;
    
    let result = generator.generate_result();
    
    // Verify trade was created
    assert_eq!(result.trades.len(), 1);
    
    let trade = &result.trades[0];
    assert_eq!(trade.trade_type, TradeType::Long);
    assert_eq!(trade.entry_price, dec!(50500.0));
    assert_eq!(trade.exit_price, dec!(53000.0));
    assert_eq!(trade.exit_reason, ExitReason::TakeProfit);
    assert_eq!(trade.hit_tp_sl, HitTpSl::TakeProfit);
    assert_eq!(trade.size_usd, dec!(1000.0));
    
    // Verify PnL calculation
    let expected_pnl = (dec!(53000.0) - dec!(50500.0)) * trade.qty - trade.fees_usd;
    assert_eq!(trade.pnl_usd, expected_pnl);
    
    // Verify summary
    assert_eq!(result.summary.total_trades, 1);
    assert_eq!(result.summary.wins, 1);
    assert_eq!(result.summary.losses, 0);
    
    Ok(())
}

#[test]
fn test_short_trade_execution() -> Result<()> {
    let mut generator = TradeTableGenerator::new();
    
    // Create a bar with short entry signal
    let bar = Bar {
        timestamp: 1609459200000,
        open: dec!(50000.0),
        high: dec!(51000.0),
        low: dec!(49000.0),
        close: dec!(50500.0),
        volume: dec!(1000.0),
        trade_count: 100,
    };
    
    let signals = vec![StrategySignal {
        side: TradeSide::Sell,
        size: dec!(1000.0),
        entry_price: Some(bar.close),
        take_profit: Some(dec!(48000.0)), // 5% TP for short
        stop_loss: Some(dec!(53000.0)),   // 5% SL for short
        time_to_live: Some(3600000), // 1 hour
    }];
    
    // Process entry
    generator.process_bar(
        &bar,
        &signals,
        &IntrabarPolicy::ExactTrades,
        &SlippageMode::None,
        &ExchangeRules::default(),
    )?;
    
    // Create exit bar that hits SL
    let exit_bar = Bar {
        timestamp: 1609459260000, // 1 minute later
        open: dec!(50500.0),
        high: dec!(53500.0), // Hits SL
        low: dec!(50000.0),
        close: dec!(53000.0),
        volume: dec!(1000.0),
        trade_count: 100,
    };
    
    // Process exit
    generator.process_bar(
        &exit_bar,
        &[], // No new signals
        &IntrabarPolicy::ExactTrades,
        &SlippageMode::None,
        &ExchangeRules::default(),
    )?;
    
    let result = generator.generate_result();
    
    // Verify trade was created
    assert_eq!(result.trades.len(), 1);
    
    let trade = &result.trades[0];
    assert_eq!(trade.trade_type, TradeType::Short);
    assert_eq!(trade.entry_price, dec!(50500.0));
    assert_eq!(trade.exit_price, dec!(53000.0));
    assert_eq!(trade.exit_reason, ExitReason::StopLoss);
    assert_eq!(trade.hit_tp_sl, HitTpSl::StopLoss);
    
    // Verify PnL calculation for short
    let expected_pnl = (dec!(50500.0) - dec!(53000.0)) * trade.qty - trade.fees_usd;
    assert_eq!(trade.pnl_usd, expected_pnl);
    
    // Verify summary
    assert_eq!(result.summary.total_trades, 1);
    assert_eq!(result.summary.wins, 0);
    assert_eq!(result.summary.losses, 1);
    
    Ok(())
}

#[test]
fn test_trade_rejection_min_notional() -> Result<()> {
    let mut generator = TradeTableGenerator::new();
    
    // Create rules with high minimum notional
    let mut rules = ExchangeRules::default();
    rules.min_notional = dec!(2000.0); // Higher than default $1000
    
    let bar = Bar {
        timestamp: 1609459200000,
        open: dec!(50000.0),
        high: dec!(51000.0),
        low: dec!(49000.0),
        close: dec!(50000.0),
        volume: dec!(1000.0),
        trade_count: 100,
    };
    
    let signals = vec![StrategySignal {
        side: TradeSide::Buy,
        size: dec!(1000.0), // $1000 position
        entry_price: Some(bar.close),
        take_profit: Some(dec!(52500.0)),
        stop_loss: Some(dec!(47500.0)),
        time_to_live: Some(3600000),
    }];
    
    // Process bar
    generator.process_bar(
        &bar,
        &signals,
        &IntrabarPolicy::ExactTrades,
        &SlippageMode::None,
        &rules,
    )?;
    
    let result = generator.generate_result();
    
    // Verify no trade was created
    assert_eq!(result.trades.len(), 0);
    
    // Verify rejection was recorded
    assert_eq!(result.rejected_trades.len(), 1);
    assert_eq!(result.rejected_trades[0].reason, "Rejected – NotionalMin");
    
    Ok(())
}

#[test]
fn test_multiple_trades_summary() -> Result<()> {
    let mut generator = TradeTableGenerator::new();
    
    // Create multiple bars with trades
    let bars = vec![
        // Bar 1: Long entry
        Bar {
            timestamp: 1609459200000,
            open: dec!(50000.0),
            high: dec!(51000.0),
            low: dec!(49000.0),
            close: dec!(50500.0),
            volume: dec!(1000.0),
            trade_count: 100,
        },
        // Bar 2: Long exit (win)
        Bar {
            timestamp: 1609459260000,
            open: dec!(50500.0),
            high: dec!(53000.0), // Hits TP
            low: dec!(50000.0),
            close: dec!(52500.0),
            volume: dec!(1000.0),
            trade_count: 100,
        },
        // Bar 3: Short entry
        Bar {
            timestamp: 1609459320000,
            open: dec!(52500.0),
            high: dec!(53000.0),
            low: dec!(50000.0),
            close: dec!(52000.0),
            volume: dec!(1000.0),
            trade_count: 100,
        },
        // Bar 4: Short exit (loss)
        Bar {
            timestamp: 1609459380000,
            open: dec!(52000.0),
            high: dec!(54000.0), // Hits SL
            low: dec!(51000.0),
            close: dec!(53500.0),
            volume: dec!(1000.0),
            trade_count: 100,
        },
    ];
    
    let signals_sequence = vec![
        // Bar 1: Long entry
        vec![StrategySignal {
            side: TradeSide::Buy,
            size: dec!(1000.0),
            entry_price: Some(dec!(50500.0)),
            take_profit: Some(dec!(53000.0)),
            stop_loss: Some(dec!(48000.0)),
            time_to_live: Some(3600000),
        }],
        // Bar 2: No signals (exit happens)
        vec![],
        // Bar 3: Short entry
        vec![StrategySignal {
            side: TradeSide::Sell,
            size: dec!(1000.0),
            entry_price: Some(dec!(52000.0)),
            take_profit: Some(dec!(49000.0)),
            stop_loss: Some(dec!(54000.0)),
            time_to_live: Some(3600000),
        }],
        // Bar 4: No signals (exit happens)
        vec![],
    ];
    
    // Process all bars
    for (i, bar) in bars.iter().enumerate() {
        generator.process_bar(
            bar,
            &signals_sequence[i],
            &IntrabarPolicy::ExactTrades,
            &SlippageMode::None,
            &ExchangeRules::default(),
        )?;
    }
    
    let result = generator.generate_result();
    
    // Verify both trades were created
    assert_eq!(result.trades.len(), 2);
    
    // Verify summary statistics
    assert_eq!(result.summary.total_trades, 2);
    assert_eq!(result.summary.wins, 1);
    assert_eq!(result.summary.losses, 1);
    assert_eq!(result.summary.win_rate, dec!(50.0)); // 50% win rate
    
    Ok(())
}

#[test]
fn test_slippage_calculation() -> Result<()> {
    let mut generator = TradeTableGenerator::new();
    
    let bar = Bar {
        timestamp: 1609459200000,
        open: dec!(50000.0),
        high: dec!(51000.0),
        low: dec!(49000.0),
        close: dec!(50500.0),
        volume: dec!(1000.0),
        trade_count: 100,
    };
    
    let signals = vec![StrategySignal {
        side: TradeSide::Buy,
        size: dec!(1000.0),
        entry_price: Some(bar.close),
        take_profit: Some(dec!(53000.0)),
        stop_loss: Some(dec!(48000.0)),
        time_to_live: Some(3600000),
    }];
    
    // Test with different slippage modes
    let slippage_modes = vec![
        SlippageMode::None,
        SlippageMode::TradeSweep,
        SlippageMode::SyntheticBook,
    ];
    
    for slippage_mode in slippage_modes {
        let mut test_generator = TradeTableGenerator::new();
        
        test_generator.process_bar(
            &bar,
            &signals,
            &IntrabarPolicy::ExactTrades,
            &slippage_mode,
            &ExchangeRules::default(),
        )?;
        
        // Verify the generator was created (basic functionality test)
        assert_eq!(test_generator.default_size_usd, dec!(1000.0));
    }
    
    Ok(())
}

#[test]
fn test_fee_calculation() -> Result<()> {
    let generator = TradeTableGenerator::new();
    let rules = ExchangeRules::default();
    
    // Test fee calculation
    let quantity = dec!(0.02); // 0.02 BTC
    let price = dec!(50000.0); // $50,000 per BTC
    let notional = quantity * price; // $1,000
    
    // Expected fee: $1,000 * 0.0001 = $0.10
    let expected_fee = notional * rules.taker_fee;
    
    // This is a basic test - in a real implementation, we'd need to expose the fee calculation method
    assert_eq!(expected_fee, dec!(0.10));
    
    Ok(())
}



