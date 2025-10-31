//! Versioning and manifest system for reproducible backtests
//! 
//! Implements run manifests, audit chains, and versioning for complete reproducibility.

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use tracing::{info, warn, error};

use crate::types::*;

/// Run manifest for complete reproducibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunManifest {
    /// Unique run identifier
    pub run_id: String,
    /// Job identifier
    pub job_id: String,
    /// Data snapshot identifier
    pub snapshot_id: String,
    /// Engine version and build info
    pub engine_version: EngineVersion,
    /// Strategy information
    pub strategy: StrategyInfo,
    /// Configuration used
    pub configuration: RunConfiguration,
    /// Data information
    pub data_info: DataInfo,
    /// Execution environment
    pub environment: EnvironmentInfo,
    /// Timestamps
    pub timestamps: TimestampInfo,
    /// Result summary
    pub result_summary: ResultSummary,
    /// Audit chain
    pub audit_chain: AuditChain,
}

/// Engine version information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineVersion {
    pub version: String,
    pub git_commit: String,
    pub build_timestamp: DateTime<Utc>,
    pub rust_version: String,
    pub cpu_features: Vec<String>,
    pub fp_flags: String,
    pub simd_enabled: bool,
}

/// Strategy information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyInfo {
    pub name: String,
    pub version: String,
    pub wasm_hash: String,
    pub source_hash: String,
    pub language: String,
    pub parameters: HashMap<String, String>,
    pub required_indicators: Vec<String>,
}

/// Run configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunConfiguration {
    pub symbols: Vec<String>,
    pub timeframe: String,
    pub start_time: u64,
    pub end_time: u64,
    pub intrabar_policy: String,
    pub slippage_mode: String,
    pub fee_version: String,
    pub precision_config: PrecisionConfig,
    pub performance_budget: PerformanceBudget,
}

/// Data information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataInfo {
    pub snapshot_name: String,
    pub data_start: u64,
    pub data_end: u64,
    pub symbols: Vec<String>,
    pub exchanges: Vec<String>,
    pub data_quality_score: f64,
    pub gap_count: u32,
    pub total_bars: u64,
    pub total_trades: u64,
}

/// Environment information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentInfo {
    pub os: String,
    pub architecture: String,
    pub cpu_model: String,
    pub cpu_cores: u32,
    pub memory_gb: u32,
    pub rust_toolchain: String,
    pub go_version: String,
    pub clickhouse_version: String,
}

/// Timestamp information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampInfo {
    pub created_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub execution_duration_ms: u64,
}

/// Result summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultSummary {
    pub total_trades: u32,
    pub total_symbols: u32,
    pub final_equity: Decimal,
    pub max_drawdown: Decimal,
    pub sharpe_ratio: Decimal,
    pub win_rate: Decimal,
    pub profit_factor: Decimal,
    pub execution_time_ms: u64,
    pub bars_processed: u64,
    pub throughput_bars_per_sec: f64,
}

/// Audit chain for verification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditChain {
    pub manifest_hash: String,
    pub data_hash: String,
    pub engine_hash: String,
    pub strategy_hash: String,
    pub config_hash: String,
    pub result_hash: String,
    pub verification_hash: String,
}

/// Precision configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecisionConfig {
    pub rounding_mode: String,
    pub price_precision: u8,
    pub quantity_precision: u8,
    pub fee_precision: u8,
    pub fp_deterministic: bool,
}

/// Performance budget
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceBudget {
    pub max_execution_time_ms: u64,
    pub min_throughput_bars_per_sec: f64,
    pub max_memory_gb: f64,
    pub max_cpu_percent: f64,
}

/// Manifest manager
pub struct ManifestManager {
    storage_path: String,
}

impl ManifestManager {
    pub fn new(storage_path: String) -> Self {
        Self { storage_path }
    }
    
    /// Create a new run manifest
    pub fn create_manifest(
        &self,
        job: &BacktestJob,
        engine_version: EngineVersion,
        strategy_info: StrategyInfo,
        environment: EnvironmentInfo,
    ) -> Result<RunManifest> {
        let run_id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();
        
        let manifest = RunManifest {
            run_id: run_id.clone(),
            job_id: job.job_id.clone(),
            snapshot_id: job.snapshot_id.clone(),
            engine_version,
            strategy: strategy_info,
            configuration: self.build_run_configuration(job)?,
            data_info: self.load_data_info(&job.snapshot_id)?,
            environment,
            timestamps: TimestampInfo {
                created_at: now,
                started_at: now,
                completed_at: now,
                execution_duration_ms: 0,
            },
            result_summary: ResultSummary::default(),
            audit_chain: AuditChain::default(),
        };
        
        // Calculate initial hashes
        let manifest_with_hashes = self.calculate_audit_hashes(manifest)?;
        
        // Save manifest
        self.save_manifest(&manifest_with_hashes)?;
        
        info!("Created run manifest: {}", run_id);
        Ok(manifest_with_hashes)
    }
    
    /// Update manifest with execution results
    pub fn update_manifest_with_results(
        &self,
        run_id: &str,
        result: &BacktestResult,
        execution_time_ms: u64,
    ) -> Result<RunManifest> {
        let mut manifest = self.load_manifest(run_id)?;
        
        // Update timestamps
        manifest.timestamps.completed_at = Utc::now();
        manifest.timestamps.execution_duration_ms = execution_time_ms;
        
        // Update result summary
        manifest.result_summary = self.build_result_summary(result, execution_time_ms)?;
        
        // Recalculate audit hashes
        let updated_manifest = self.calculate_audit_hashes(manifest)?;
        
        // Save updated manifest
        self.save_manifest(&updated_manifest)?;
        
        info!("Updated manifest {} with results", run_id);
        Ok(updated_manifest)
    }
    
    /// Load manifest by run ID
    pub fn load_manifest(&self, run_id: &str) -> Result<RunManifest> {
        let path = self.get_manifest_path(run_id);
        let content = fs::read_to_string(&path)?;
        let manifest: RunManifest = serde_json::from_str(&content)?;
        Ok(manifest)
    }
    
    /// Verify manifest integrity
    pub fn verify_manifest(&self, manifest: &RunManifest) -> Result<VerificationResult> {
        let mut issues = Vec::new();
        
        // Verify audit chain
        let calculated_hashes = self.calculate_audit_hashes(manifest.clone())?;
        
        if calculated_hashes.audit_chain.manifest_hash != manifest.audit_chain.manifest_hash {
            issues.push("Manifest hash mismatch".to_string());
        }
        
        if calculated_hashes.audit_chain.data_hash != manifest.audit_chain.data_hash {
            issues.push("Data hash mismatch".to_string());
        }
        
        if calculated_hashes.audit_chain.engine_hash != manifest.audit_chain.engine_hash {
            issues.push("Engine hash mismatch".to_string());
        }
        
        if calculated_hashes.audit_chain.strategy_hash != manifest.audit_chain.strategy_hash {
            issues.push("Strategy hash mismatch".to_string());
        }
        
        if calculated_hashes.audit_chain.config_hash != manifest.audit_chain.config_hash {
            issues.push("Configuration hash mismatch".to_string());
        }
        
        if calculated_hashes.audit_chain.result_hash != manifest.audit_chain.result_hash {
            issues.push("Result hash mismatch".to_string());
        }
        
        // Verify performance budget
        let budget_status = self.check_performance_budget(&manifest.result_summary, &manifest.configuration.performance_budget);
        if !budget_status.passed {
            issues.extend(budget_status.violations);
        }
        
        // Verify data quality
        if manifest.data_info.data_quality_score < 0.95 {
            issues.push(format!("Data quality score too low: {}", manifest.data_info.data_quality_score));
        }
        
        Ok(VerificationResult {
            valid: issues.is_empty(),
            issues,
            manifest_id: manifest.run_id.clone(),
        })
    }
    
    /// Reproduce a run from manifest
    pub fn reproduce_run(&self, run_id: &str) -> Result<ReproductionResult> {
        let manifest = self.load_manifest(run_id)?;
        
        info!("Reproducing run: {}", run_id);
        
        // Verify we can reproduce the exact environment
        let environment_match = self.check_environment_compatibility(&manifest.environment)?;
        
        // Verify data availability
        let data_available = self.check_data_availability(&manifest.data_info)?;
        
        // Verify strategy availability
        let strategy_available = self.check_strategy_availability(&manifest.strategy)?;
        
        let can_reproduce = environment_match && data_available && strategy_available;
        
        Ok(ReproductionResult {
            can_reproduce,
            manifest,
            environment_match,
            data_available,
            strategy_available,
            reproduction_instructions: self.generate_reproduction_instructions(&manifest)?,
        })
    }
    
    /// Helper methods
    
    fn build_run_configuration(&self, job: &BacktestJob) -> Result<RunConfiguration> {
        Ok(RunConfiguration {
            symbols: job.symbols.clone(),
            timeframe: job.timeframe.clone(),
            start_time: job.start_time,
            end_time: job.end_time,
            intrabar_policy: format!("{:?}", job.intrabar_policy),
            slippage_mode: format!("{:?}", job.slippage_mode),
            fee_version: job.fee_version.clone(),
            precision_config: PrecisionConfig {
                rounding_mode: "nearest-even".to_string(),
                price_precision: 8,
                quantity_precision: 8,
                fee_precision: 8,
                fp_deterministic: true,
            },
            performance_budget: PerformanceBudget {
                max_execution_time_ms: 300_000, // 5 minutes
                min_throughput_bars_per_sec: 1000.0,
                max_memory_gb: 8.0,
                max_cpu_percent: 80.0,
            },
        })
    }
    
    fn load_data_info(&self, snapshot_id: &str) -> Result<DataInfo> {
        // This would load actual data info from ClickHouse
        Ok(DataInfo {
            snapshot_name: format!("snapshot_{}", snapshot_id),
            data_start: 1609459200000, // 2021-01-01
            data_end: 1704067199000,   // 2024-01-01
            symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
            exchanges: vec!["binance".to_string()],
            data_quality_score: 0.98,
            gap_count: 5,
            total_bars: 1_000_000,
            total_trades: 5_000_000,
        })
    }
    
    fn calculate_audit_hashes(&self, mut manifest: RunManifest) -> Result<RunManifest> {
        // Calculate manifest hash (excluding audit chain)
        let mut temp_manifest = manifest.clone();
        temp_manifest.audit_chain = AuditChain::default();
        let manifest_json = serde_json::to_string(&temp_manifest)?;
        manifest.audit_chain.manifest_hash = self.calculate_hash(&manifest_json);
        
        // Calculate data hash
        let data_json = serde_json::to_string(&manifest.data_info)?;
        manifest.audit_chain.data_hash = self.calculate_hash(&data_json);
        
        // Calculate engine hash
        let engine_json = serde_json::to_string(&manifest.engine_version)?;
        manifest.audit_chain.engine_hash = self.calculate_hash(&engine_json);
        
        // Calculate strategy hash
        let strategy_json = serde_json::to_string(&manifest.strategy)?;
        manifest.audit_chain.strategy_hash = self.calculate_hash(&strategy_json);
        
        // Calculate config hash
        let config_json = serde_json::to_string(&manifest.configuration)?;
        manifest.audit_chain.config_hash = self.calculate_hash(&config_json);
        
        // Calculate result hash
        let result_json = serde_json::to_string(&manifest.result_summary)?;
        manifest.audit_chain.result_hash = self.calculate_hash(&result_json);
        
        // Calculate verification hash
        let verification_json = serde_json::to_string(&manifest.audit_chain)?;
        manifest.audit_chain.verification_hash = self.calculate_hash(&verification_json);
        
        Ok(manifest)
    }
    
    fn calculate_hash(&self, data: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data.as_bytes());
        format!("{:x}", hasher.finalize())
    }
    
    fn build_result_summary(&self, result: &BacktestResult, execution_time_ms: u64) -> Result<ResultSummary> {
        let total_trades = result.symbol_results.iter().map(|r| r.trades.len() as u32).sum();
        let total_symbols = result.symbol_results.len() as u32;
        
        // Calculate final equity
        let final_equity = result.symbol_results.iter()
            .filter_map(|r| r.equity_curve.last())
            .map(|p| p.equity)
            .sum();
        
        // Calculate max drawdown
        let max_drawdown = result.symbol_results.iter()
            .map(|r| r.drawdown)
            .max()
            .unwrap_or(Decimal::ZERO);
        
        // Calculate performance metrics
        let bars_processed = result.performance_metrics.bars_processed;
        let throughput = if execution_time_ms > 0 {
            bars_processed as f64 / (execution_time_ms as f64 / 1000.0)
        } else {
            0.0
        };
        
        Ok(ResultSummary {
            total_trades,
            total_symbols,
            final_equity,
            max_drawdown,
            sharpe_ratio: Decimal::ZERO, // Would calculate actual Sharpe ratio
            win_rate: Decimal::ZERO,    // Would calculate actual win rate
            profit_factor: Decimal::ZERO, // Would calculate actual profit factor
            execution_time_ms,
            bars_processed,
            throughput_bars_per_sec: throughput,
        })
    }
    
    fn check_performance_budget(&self, summary: &ResultSummary, budget: &PerformanceBudget) -> BudgetCheckResult {
        let mut violations = Vec::new();
        
        if summary.execution_time_ms > budget.max_execution_time_ms {
            violations.push(format!(
                "Execution time exceeded budget: {}ms > {}ms",
                summary.execution_time_ms, budget.max_execution_time_ms
            ));
        }
        
        if summary.throughput_bars_per_sec < budget.min_throughput_bars_per_sec {
            violations.push(format!(
                "Throughput below budget: {} < {} bars/sec",
                summary.throughput_bars_per_sec, budget.min_throughput_bars_per_sec
            ));
        }
        
        BudgetCheckResult {
            passed: violations.is_empty(),
            violations,
        }
    }
    
    fn check_environment_compatibility(&self, environment: &EnvironmentInfo) -> Result<bool> {
        // Check if current environment matches the manifest environment
        // This would check OS, architecture, CPU features, etc.
        Ok(true) // Simplified for now
    }
    
    fn check_data_availability(&self, data_info: &DataInfo) -> Result<bool> {
        // Check if the required data snapshot is available
        Ok(true) // Simplified for now
    }
    
    fn check_strategy_availability(&self, strategy: &StrategyInfo) -> Result<bool> {
        // Check if the strategy WASM is available
        Ok(true) // Simplified for now
    }
    
    fn generate_reproduction_instructions(&self, manifest: &RunManifest) -> Result<String> {
        let instructions = format!(
            "To reproduce this run:\n\
            1. Use engine version: {}\n\
            2. Use strategy: {} (hash: {})\n\
            3. Use data snapshot: {}\n\
            4. Set configuration: symbols={:?}, timeframe={}, start={}, end={}\n\
            5. Ensure environment: {} on {}, {} cores, {}GB RAM",
            manifest.engine_version.version,
            manifest.strategy.name,
            manifest.strategy.wasm_hash,
            manifest.snapshot_id,
            manifest.configuration.symbols,
            manifest.configuration.timeframe,
            manifest.configuration.start_time,
            manifest.configuration.end_time,
            manifest.environment.os,
            manifest.environment.architecture,
            manifest.environment.cpu_cores,
            manifest.environment.memory_gb
        );
        
        Ok(instructions)
    }
    
    fn save_manifest(&self, manifest: &RunManifest) -> Result<()> {
        let path = self.get_manifest_path(&manifest.run_id);
        let content = serde_json::to_string_pretty(manifest)?;
        fs::write(path, content)?;
        Ok(())
    }
    
    fn get_manifest_path(&self, run_id: &str) -> String {
        format!("{}/manifests/{}.json", self.storage_path, run_id)
    }
}

/// Verification result
#[derive(Debug, Clone)]
pub struct VerificationResult {
    pub valid: bool,
    pub issues: Vec<String>,
    pub manifest_id: String,
}

/// Reproduction result
#[derive(Debug, Clone)]
pub struct ReproductionResult {
    pub can_reproduce: bool,
    pub manifest: RunManifest,
    pub environment_match: bool,
    pub data_available: bool,
    pub strategy_available: bool,
    pub reproduction_instructions: String,
}

/// Budget check result
#[derive(Debug, Clone)]
pub struct BudgetCheckResult {
    pub passed: bool,
    pub violations: Vec<String>,
}

impl Default for ResultSummary {
    fn default() -> Self {
        Self {
            total_trades: 0,
            total_symbols: 0,
            final_equity: Decimal::ZERO,
            max_drawdown: Decimal::ZERO,
            sharpe_ratio: Decimal::ZERO,
            win_rate: Decimal::ZERO,
            profit_factor: Decimal::ZERO,
            execution_time_ms: 0,
            bars_processed: 0,
            throughput_bars_per_sec: 0.0,
        }
    }
}

impl Default for AuditChain {
    fn default() -> Self {
        Self {
            manifest_hash: String::new(),
            data_hash: String::new(),
            engine_hash: String::new(),
            strategy_hash: String::new(),
            config_hash: String::new(),
            result_hash: String::new(),
            verification_hash: String::new(),
        }
    }
}

