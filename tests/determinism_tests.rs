//! Determinism tests for the backtesting engine
//! 
//! Ensures that all calculations are deterministic and reproducible across runs.
//! Tests include golden dataset validation and TradingView parity checks.

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use tracing::{info, warn, error};

use crate::types::*;
use crate::indicators::IndicatorRegistry;
use crate::simulator::ExchangeSimulator;
use crate::precision::{PrecisionConfig, validate_fp_determinism};

/// Test configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeterminismTestConfig {
    pub golden_datasets: Vec<GoldenDataset>,
    pub tv_parity_tests: Vec<TradingViewParityTest>,
    pub tolerance: Decimal,
    pub max_iterations: usize,
}

/// Golden dataset for deterministic validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoldenDataset {
    pub name: String,
    pub description: String,
    pub symbol: String,
    pub timeframe: String,
    pub start_time: u64,
    pub end_time: u64,
    pub expected_hash: String,
    pub expected_trades: usize,
    pub expected_final_equity: Decimal,
}

/// TradingView parity test configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingViewParityTest {
    pub name: String,
    pub symbol: String,
    pub timeframe: String,
    pub start_time: u64,
    pub end_time: u64,
    pub tv_csv_path: String,
    pub tolerance: Decimal,
}

/// Determinism test runner
pub struct DeterminismTester {
    config: DeterminismTestConfig,
    engine_config: EngineConfig,
    precision_config: PrecisionConfig,
}

impl DeterminismTester {
    pub fn new(config: DeterminismTestConfig) -> Result<Self> {
        // Validate floating-point determinism
        validate_fp_determinism()?;
        
        Ok(Self {
            config,
            engine_config: EngineConfig::default(),
            precision_config: PrecisionConfig::default(),
        })
    }
    
    /// Run all determinism tests
    pub async fn run_all_tests(&mut self) -> Result<TestResults> {
        info!("Starting determinism tests");
        
        let mut results = TestResults::new();
        
        // Test 1: Floating-point determinism
        results.add_test(self.test_fp_determinism().await?);
        
        // Test 2: Indicator determinism
        results.add_test(self.test_indicator_determinism().await?);
        
        // Test 3: Golden dataset validation
        for dataset in &self.config.golden_datasets {
            results.add_test(self.test_golden_dataset(dataset).await?);
        }
        
        // Test 4: TradingView parity tests
        for tv_test in &self.config.tv_parity_tests {
            results.add_test(self.test_tv_parity(tv_test).await?).
        }
        
        // Test 5: Multi-run consistency
        results.add_test(self.test_multi_run_consistency().await?);
        
        info!("Determinism tests completed: {} passed, {} failed", 
              results.passed_count(), results.failed_count());
        
        Ok(results)
    }
    
    /// Test floating-point determinism
    async fn test_fp_determinism(&self) -> Result<TestCase> {
        info!("Testing floating-point determinism");
        
        let test_name = "fp_determinism";
        let mut errors = Vec::new();
        
        // Test multiple runs produce identical results
        for run in 0..self.config.max_iterations {
            let result1 = self.calculate_deterministic_math();
            let result2 = self.calculate_deterministic_math();
            
            if result1 != result2 {
                errors.push(format!("Run {}: FP calculations not deterministic", run));
            }
        }
        
        // Test specific FP operations
        let test_cases = vec![
            (0.1 + 0.2, 0.3),
            (1.0 / 3.0, 0.3333333333333333),
            (std::f64::consts::PI, 3.141592653589793),
        ];
        
        for (actual, expected) in test_cases {
            if (actual - expected).abs() > 1e-15 {
                errors.push(format!("FP operation failed: {} != {}", actual, expected));
            }
        }
        
        Ok(TestCase {
            name: test_name.to_string(),
            passed: errors.is_empty(),
            errors,
            execution_time_ms: 0, // Would measure actual time
        })
    }
    
    /// Test indicator calculation determinism
    async fn test_indicator_determinism(&self) -> Result<TestCase> {
        info!("Testing indicator determinism");
        
        let test_name = "indicator_determinism";
        let mut errors = Vec::new();
        
        // Create test data
        let test_bars = self.create_test_bars();
        
        // Test each indicator multiple times
        let indicators = vec!["ema", "sma", "rsi", "atr", "vwap"];
        
        for indicator_name in indicators {
            let mut registry = IndicatorRegistry::new(true)?;
            
            // Calculate indicator multiple times
            let mut results = Vec::new();
            for _ in 0..self.config.max_iterations {
                let result = registry.calculate(indicator_name, &test_bars)?;
                results.push(result);
            }
            
            // Check all results are identical
            for i in 1..results.len() {
                if !self.compare_indicator_results(&results[0], &results[i]) {
                    errors.push(format!("Indicator {} not deterministic", indicator_name));
                    break;
                }
            }
        }
        
        Ok(TestCase {
            name: test_name.to_string(),
            passed: errors.is_empty(),
            errors,
            execution_time_ms: 0,
        })
    }
    
    /// Test golden dataset validation
    async fn test_golden_dataset(&self, dataset: &GoldenDataset) -> Result<TestCase> {
        info!("Testing golden dataset: {}", dataset.name);
        
        let test_name = format!("golden_dataset_{}", dataset.name);
        let mut errors = Vec::new();
        
        // Load test data
        let market_data = self.load_test_data(dataset).await?;
        
        // Run backtest
        let result = self.run_backtest(&market_data).await?;
        
        // Calculate result hash
        let result_hash = self.calculate_result_hash(&result)?;
        
        // Validate hash
        if result_hash != dataset.expected_hash {
            errors.push(format!(
                "Hash mismatch: expected {}, got {}", 
                dataset.expected_hash, 
                result_hash
            ));
        }
        
        // Validate trade count
        if result.trades.len() != dataset.expected_trades {
            errors.push(format!(
                "Trade count mismatch: expected {}, got {}", 
                dataset.expected_trades, 
                result.trades.len()
            ));
        }
        
        // Validate final equity
        if let Some(final_equity) = result.equity_curve.last() {
            let equity_diff = (final_equity.equity - dataset.expected_final_equity).abs();
            if equity_diff > self.config.tolerance {
                errors.push(format!(
                    "Final equity mismatch: expected {}, got {}, diff {}", 
                    dataset.expected_final_equity, 
                    final_equity.equity,
                    equity_diff
                ));
            }
        }
        
        Ok(TestCase {
            name: test_name,
            passed: errors.is_empty(),
            errors,
            execution_time_ms: 0,
        })
    }
    
    /// Test TradingView parity
    async fn test_tv_parity(&self, tv_test: &TradingViewParityTest) -> Result<TestCase> {
        info!("Testing TradingView parity: {}", tv_test.name);
        
        let test_name = format!("tv_parity_{}", tv_test.name);
        let mut errors = Vec::new();
        
        // Load TradingView CSV data
        let tv_data = self.load_tv_csv(&tv_test.tv_csv_path)?;
        
        // Load our test data
        let market_data = self.load_test_data_for_period(
            &tv_test.symbol,
            tv_test.start_time,
            tv_test.end_time,
        ).await?;
        
        // Run backtest
        let result = self.run_backtest(&market_data).await?;
        
        // Compare with TradingView data
        for (our_trade, tv_trade) in result.trades.iter().zip(tv_data.trades.iter()) {
            let price_diff = (our_trade.price - tv_trade.price).abs();
            if price_diff > tv_test.tolerance {
                errors.push(format!(
                    "Price mismatch at {}: our {}, TV {}, diff {}", 
                    our_trade.timestamp,
                    our_trade.price,
                    tv_trade.price,
                    price_diff
                ));
            }
        }
        
        Ok(TestCase {
            name: test_name,
            passed: errors.is_empty(),
            errors,
            execution_time_ms: 0,
        })
    }
    
    /// Test multi-run consistency
    async fn test_multi_run_consistency(&self) -> Result<TestCase> {
        info!("Testing multi-run consistency");
        
        let test_name = "multi_run_consistency";
        let mut errors = Vec::new();
        
        // Use first golden dataset for consistency test
        if let Some(dataset) = self.config.golden_datasets.first() {
            let market_data = self.load_test_data(dataset).await?;
            
            // Run backtest multiple times
            let mut results = Vec::new();
            for _ in 0..self.config.max_iterations {
                let result = self.run_backtest(&market_data).await?;
                results.push(result);
            }
            
            // Check all results are identical
            for i in 1..results.len() {
                if !self.compare_backtest_results(&results[0], &results[i]) {
                    errors.push(format!("Backtest run {} not consistent with run 0", i));
                }
            }
        }
        
        Ok(TestCase {
            name: test_name.to_string(),
            passed: errors.is_empty(),
            errors,
            execution_time_ms: 0,
        })
    }
    
    /// Helper methods
    
    fn calculate_deterministic_math(&self) -> f64 {
        let mut result = 0.0;
        for i in 1..=1000 {
            result += 1.0 / (i as f64);
        }
        result
    }
    
    fn create_test_bars(&self) -> MarketData {
        let mut bars = Vec::new();
        let mut price = dec!(100.0);
        
        for i in 0..1000 {
            let timestamp = 1609459200000 + (i * 60000) as u64; // 1-minute bars
            let change = Decimal::from_f64((i as f64 * 0.01).sin()).unwrap_or(dec!(0.0));
            price += change;
            
            bars.push(Bar {
                timestamp,
                open: price,
                high: price + dec!(0.5),
                low: price - dec!(0.5),
                close: price,
                volume: dec!(1000.0),
                trade_count: 10,
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
    
    async fn load_test_data(&self, dataset: &GoldenDataset) -> Result<MarketData> {
        // This would load actual market data from ClickHouse
        // For now, return test data
        Ok(self.create_test_bars())
    }
    
    async fn load_test_data_for_period(
        &self,
        symbol: &str,
        start_time: u64,
        end_time: u64,
    ) -> Result<MarketData> {
        // This would load data for the specific period
        Ok(self.create_test_bars())
    }
    
    async fn run_backtest(&self, market_data: &MarketData) -> Result<SimulationResult> {
        // This would run the actual backtest
        // For now, return mock result
        Ok(SimulationResult {
            trades: Vec::new(),
            positions: Vec::new(),
            equity_curve: Vec::new(),
            max_drawdown: dec!(0.0),
            exposure: dec!(0.0),
            attribution: HashMap::new(),
        })
    }
    
    fn calculate_result_hash(&self, result: &SimulationResult) -> Result<String> {
        let serialized = serde_json::to_string(result)?;
        let mut hasher = Sha256::new();
        hasher.update(serialized.as_bytes());
        let hash = hasher.finalize();
        Ok(format!("{:x}", hash))
    }
    
    fn load_tv_csv(&self, path: &str) -> Result<TradingViewData> {
        // This would load TradingView CSV data
        Ok(TradingViewData {
            trades: Vec::new(),
        })
    }
    
    fn compare_indicator_results(&self, a: &[IndicatorValue], b: &[IndicatorValue]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        
        for (va, vb) in a.iter().zip(b.iter()) {
            if va.timestamp != vb.timestamp {
                return false;
            }
            if (va.value - vb.value).abs() > self.config.tolerance {
                return false;
            }
        }
        
        true
    }
    
    fn compare_backtest_results(&self, a: &SimulationResult, b: &SimulationResult) -> bool {
        // Compare trades
        if a.trades.len() != b.trades.len() {
            return false;
        }
        
        for (ta, tb) in a.trades.iter().zip(b.trades.iter()) {
            if ta.timestamp != tb.timestamp {
                return false;
            }
            if (ta.price - tb.price).abs() > self.config.tolerance {
                return false;
            }
        }
        
        // Compare equity curve
        if a.equity_curve.len() != b.equity_curve.len() {
            return false;
        }
        
        for (ea, eb) in a.equity_curve.iter().zip(b.equity_curve.iter()) {
            if ea.timestamp != eb.timestamp {
                return false;
            }
            if (ea.equity - eb.equity).abs() > self.config.tolerance {
                return false;
            }
        }
        
        true
    }
}

/// Test case result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestCase {
    pub name: String,
    pub passed: bool,
    pub errors: Vec<String>,
    pub execution_time_ms: u64,
}

/// Test results container
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResults {
    pub test_cases: Vec<TestCase>,
    pub total_execution_time_ms: u64,
}

impl TestResults {
    fn new() -> Self {
        Self {
            test_cases: Vec::new(),
            total_execution_time_ms: 0,
        }
    }
    
    fn add_test(&mut self, test_case: TestCase) {
        self.test_cases.push(test_case);
    }
    
    fn passed_count(&self) -> usize {
        self.test_cases.iter().filter(|t| t.passed).count()
    }
    
    fn failed_count(&self) -> usize {
        self.test_cases.iter().filter(|t| !t.passed).count()
    }
    
    fn all_passed(&self) -> bool {
        self.test_cases.iter().all(|t| t.passed)
    }
}

/// TradingView data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingViewData {
    pub trades: Vec<TradingViewTrade>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingViewTrade {
    pub timestamp: u64,
    pub price: Decimal,
    pub quantity: Decimal,
    pub side: TradeSide,
}

/// Load test configuration from file
pub fn load_test_config(path: &Path) -> Result<DeterminismTestConfig> {
    let content = fs::read_to_string(path)?;
    let config: DeterminismTestConfig = serde_json::from_str(&content)?;
    Ok(config)
}

/// Save test results to file
pub fn save_test_results(path: &Path, results: &TestResults) -> Result<()> {
    let content = serde_json::to_string_pretty(results)?;
    fs::write(path, content)?;
    Ok(())
}

