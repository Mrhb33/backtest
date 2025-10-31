//! High-Performance Backtesting Engine
//! 
//! Core compute engine with SIMD-optimized indicators, deterministic floating-point arithmetic,
//! and precision handling for financial calculations.

use std::collections::HashMap;
use std::sync::Arc;
use anyhow::Result;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::{info, warn, error};

pub mod indicators;
pub mod simulator;
pub mod precision;
pub mod wasm;
pub mod types;
pub mod trade_table;
pub mod export;

use types::*;

/// Main engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Floating-point rounding mode (must be "nearest-even" for determinism)
    pub rounding_mode: String,
    /// Enable SIMD optimizations
    pub enable_simd: bool,
    /// CPU affinity settings
    pub cpu_affinity: Option<Vec<usize>>,
    /// Memory preallocation size
    pub prealloc_size: usize,
    /// Deterministic random seed
    pub random_seed: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            rounding_mode: "nearest-even".to_string(),
            enable_simd: true,
            cpu_affinity: None,
            prealloc_size: 1_000_000, // 1M bars
            random_seed: 42,
        }
    }
}

/// Backtesting job specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestJob {
    /// Unique job identifier
    pub job_id: String,
    /// Symbols to backtest
    pub symbols: Vec<String>,
    /// Timeframe (e.g., "1m", "1s")
    pub timeframe: String,
    /// Start timestamp (Unix milliseconds)
    pub start_time: u64,
    /// End timestamp (Unix milliseconds)
    pub end_time: u64,
    /// Intrabar simulation policy
    pub intrabar_policy: IntrabarPolicy,
    /// Fee model version
    pub fee_version: String,
    /// Slippage mode
    pub slippage_mode: SlippageMode,
    /// Strategy WASM hash
    pub strategy_wasm_hash: String,
    /// Data snapshot ID
    pub snapshot_id: String,
}

/// Intrabar simulation policies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IntrabarPolicy {
    /// Use exact trade paths (best accuracy)
    ExactTrades,
    /// Use 1s bars with fixed path order
    OneSecondBars,
    /// Linear interpolation between OHLC
    LinearInterpolation,
}

/// Slippage simulation modes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SlippageMode {
    /// No slippage
    None,
    /// Trade sweep depth (L1)
    TradeSweep,
    /// Synthetic order book (L2)
    SyntheticBook,
}

/// Main backtesting engine
pub struct BacktestEngine {
    config: EngineConfig,
    indicators: indicators::IndicatorRegistry,
    simulator: simulator::ExchangeSimulator,
    wasm_runtime: wasm::WasmRuntime,
    performance_metrics: PerformanceMetrics,
}

impl BacktestEngine {
    /// Create a new backtesting engine
    pub fn new(config: EngineConfig) -> Result<Self> {
        info!("Initializing backtesting engine with config: {:?}", config);
        
        // Set CPU affinity if specified
        if let Some(affinity) = &config.cpu_affinity {
            Self::set_cpu_affinity(affinity)?;
        }
        
        // Initialize components
        let indicators = indicators::IndicatorRegistry::new(config.enable_simd)?;
        let simulator = simulator::ExchangeSimulator::new()?;
        let wasm_runtime = wasm::WasmRuntime::new()?;
        
        Ok(Self {
            config,
            indicators,
            simulator,
            wasm_runtime,
            performance_metrics: PerformanceMetrics::new(),
        })
    }
    
    /// Execute a backtesting job
    pub async fn execute_job(&mut self, job: BacktestJob) -> Result<BacktestResult> {
        info!("Executing backtest job: {}", job.job_id);
        
        let start_time = std::time::Instant::now();
        
        // Load strategy WASM
        let strategy = self.wasm_runtime.load_strategy(&job.strategy_wasm_hash).await?;
        
        // Execute backtest for each symbol
        let mut symbol_results = Vec::new();
        for symbol in &job.symbols {
            let symbol_result = self.execute_symbol_backtest(
                symbol,
                &job,
                &strategy,
            ).await?;
            symbol_results.push(symbol_result);
        }
        
        let execution_time = start_time.elapsed();
        
        // Compile results
        let result = BacktestResult {
            job_id: job.job_id.clone(),
            execution_time_ms: execution_time.as_millis() as u64,
            symbol_results,
            performance_metrics: self.performance_metrics.clone(),
            manifest: RunManifest::from_job(&job),
        };
        
        info!("Backtest completed in {}ms", execution_time.as_millis());
        Ok(result)
    }
    
    /// Execute backtest for a single symbol
    async fn execute_symbol_backtest(
        &mut self,
        symbol: &str,
        job: &BacktestJob,
        strategy: &wasm::Strategy,
    ) -> Result<SymbolResult> {
        info!("Backtesting symbol: {}", symbol);
        
        // Load market data
        let market_data = self.load_market_data(symbol, job).await?;
        
        // Initialize indicators
        let mut indicator_values = HashMap::new();
        for indicator_name in strategy.get_required_indicators() {
            let values = self.indicators.calculate(
                &indicator_name,
                &market_data,
            )?;
            indicator_values.insert(indicator_name, values);
        }
        
        // Run simulation
        let simulation_result = self.simulator.simulate(
            &market_data,
            &indicator_values,
            strategy,
            &job.intrabar_policy,
            &job.slippage_mode,
        ).await?;
        
        Ok(SymbolResult {
            symbol: symbol.to_string(),
            trades: simulation_result.trades,
            positions: simulation_result.positions,
            equity_curve: simulation_result.equity_curve,
            drawdown: simulation_result.max_drawdown,
            exposure: simulation_result.exposure,
            attribution: simulation_result.attribution,
            trade_table: Some(self.simulator.get_trade_table_result()),
        })
    }
    
    /// Load market data for a symbol
    async fn load_market_data(
        &self,
        symbol: &str,
        job: &BacktestJob,
    ) -> Result<MarketData> {
        // This would typically load from Arrow IPC stream
        // For now, return placeholder
        Ok(MarketData {
            symbol: symbol.to_string(),
            timeframe: job.timeframe.clone(),
            bars: Vec::new(),
            trades: Vec::new(),
            rules: ExchangeRules::default(),
        })
    }
    
    /// Set CPU affinity for deterministic performance
    fn set_cpu_affinity(cores: &[usize]) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            use std::process::Command;
            let pid = std::process::id();
            for &core in cores {
                Command::new("taskset")
                    .args(&["-cp", &core.to_string(), &pid.to_string()])
                    .output()?;
            }
        }
        Ok(())
    }
}

/// Performance metrics for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub bars_processed: u64,
    pub indicators_calculated: u64,
    pub trades_executed: u64,
    pub execution_time_ms: u64,
    pub memory_allocated_bytes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

impl PerformanceMetrics {
    fn new() -> Self {
        Self {
            bars_processed: 0,
            indicators_calculated: 0,
            trades_executed: 0,
            execution_time_ms: 0,
            memory_allocated_bytes: 0,
            cache_hits: 0,
            cache_misses: 0,
        }
    }
}

/// Run manifest for reproducibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunManifest {
    pub job_id: String,
    pub snapshot_id: String,
    pub engine_version: String,
    pub strategy_hash: String,
    pub intrabar_policy: String,
    pub fee_version: String,
    pub slippage_mode: String,
    pub created_at: u64,
    pub cpu_features: Vec<String>,
    pub fp_flags: String,
}

impl RunManifest {
    fn from_job(job: &BacktestJob) -> Self {
        Self {
            job_id: job.job_id.clone(),
            snapshot_id: job.snapshot_id.clone(),
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            strategy_hash: job.strategy_wasm_hash.clone(),
            intrabar_policy: format!("{:?}", job.intrabar_policy),
            fee_version: job.fee_version.clone(),
            slippage_mode: format!("{:?}", job.slippage_mode),
            created_at: chrono::Utc::now().timestamp_millis() as u64,
            cpu_features: vec!["simd".to_string()], // Would detect actual features
            fp_flags: "nearest-even".to_string(),
        }
    }
}

