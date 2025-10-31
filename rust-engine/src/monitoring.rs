//! Performance monitoring and metrics collection
//! 
//! Implements Prometheus metrics and OpenTelemetry tracing for the backtesting engine.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::Result;
use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec,
    Opts, Registry, TextEncoder,
};
use tracing::{info, warn, error, instrument};

/// Performance metrics collector
pub struct MetricsCollector {
    // Counters
    pub backtest_total: Counter,
    pub backtest_failures: Counter,
    pub trades_executed: Counter,
    pub indicators_calculated: Counter,
    pub determinism_violations: Counter,
    
    // Gauges
    pub active_backtests: Gauge,
    pub memory_usage_bytes: Gauge,
    pub cpu_usage_percent: Gauge,
    pub data_quality_score: Gauge,
    
    // Histograms
    pub backtest_duration: Histogram,
    pub indicator_calculation_time: HistogramVec,
    pub trade_execution_time: Histogram,
    pub memory_allocation_size: Histogram,
    
    // Custom metrics
    pub bars_per_second: GaugeVec,
    pub cache_hit_rate: GaugeVec,
    pub throughput_by_symbol: GaugeVec,
    
    registry: Registry,
}

impl MetricsCollector {
    pub fn new() -> Result<Self> {
        let registry = Registry::new();
        
        // Initialize counters
        let backtest_total = Counter::new(
            "backtest_total",
            "Total number of backtests executed"
        )?;
        
        let backtest_failures = Counter::new(
            "backtest_failures_total",
            "Total number of failed backtests"
        )?;
        
        let trades_executed = Counter::new(
            "trades_executed_total",
            "Total number of trades executed"
        )?;
        
        let indicators_calculated = Counter::new(
            "indicators_calculated_total",
            "Total number of indicator calculations"
        )?;
        
        let determinism_violations = Counter::new(
            "backtest_determinism_violations_total",
            "Total number of determinism violations"
        )?;
        
        // Initialize gauges
        let active_backtests = Gauge::new(
            "active_backtests",
            "Number of currently active backtests"
        )?;
        
        let memory_usage_bytes = Gauge::new(
            "memory_usage_bytes",
            "Current memory usage in bytes"
        )?;
        
        let cpu_usage_percent = Gauge::new(
            "cpu_usage_percent",
            "Current CPU usage percentage"
        )?;
        
        let data_quality_score = Gauge::new(
            "data_quality_score",
            "Data quality score (0-1)"
        )?;
        
        // Initialize histograms
        let backtest_duration = Histogram::with_opts(HistogramOpts::new(
            "backtest_duration_seconds",
            "Duration of backtest execution in seconds"
        ).buckets(vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 300.0]))?;
        
        let indicator_calculation_time = HistogramVec::new(
            HistogramOpts::new(
                "indicator_calculation_time_seconds",
                "Time taken to calculate indicators"
            ).buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]),
            &["indicator_name"]
        )?;
        
        let trade_execution_time = Histogram::with_opts(HistogramOpts::new(
            "trade_execution_time_seconds",
            "Time taken to execute trades"
        ).buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]))?;
        
        let memory_allocation_size = Histogram::with_opts(HistogramOpts::new(
            "memory_allocation_size_bytes",
            "Size of memory allocations"
        ).buckets(vec![1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0]))?;
        
        // Initialize custom metrics
        let bars_per_second = GaugeVec::new(
            Opts::new("backtest_bars_per_second", "Bars processed per second"),
            &["symbol", "timeframe"]
        )?;
        
        let cache_hit_rate = GaugeVec::new(
            Opts::new("cache_hit_rate", "Cache hit rate"),
            &["cache_type"]
        )?;
        
        let throughput_by_symbol = GaugeVec::new(
            Opts::new("throughput_by_symbol", "Throughput by symbol"),
            &["symbol"]
        )?;
        
        // Register metrics
        registry.register(Box::new(backtest_total.clone()))?;
        registry.register(Box::new(backtest_failures.clone()))?;
        registry.register(Box::new(trades_executed.clone()))?;
        registry.register(Box::new(indicators_calculated.clone()))?;
        registry.register(Box::new(determinism_violations.clone()))?;
        
        registry.register(Box::new(active_backtests.clone()))?;
        registry.register(Box::new(memory_usage_bytes.clone()))?;
        registry.register(Box::new(cpu_usage_percent.clone()))?;
        registry.register(Box::new(data_quality_score.clone()))?;
        
        registry.register(Box::new(backtest_duration.clone()))?;
        registry.register(Box::new(indicator_calculation_time.clone()))?;
        registry.register(Box::new(trade_execution_time.clone()))?;
        registry.register(Box::new(memory_allocation_size.clone()))?;
        
        registry.register(Box::new(bars_per_second.clone()))?;
        registry.register(Box::new(cache_hit_rate.clone()))?;
        registry.register(Box::new(throughput_by_symbol.clone()))?;
        
        Ok(Self {
            backtest_total,
            backtest_failures,
            trades_executed,
            indicators_calculated,
            determinism_violations,
            active_backtests,
            memory_usage_bytes,
            cpu_usage_percent,
            data_quality_score,
            backtest_duration,
            indicator_calculation_time,
            trade_execution_time,
            memory_allocation_size,
            bars_per_second,
            cache_hit_rate,
            throughput_by_symbol,
            registry,
        })
    }
    
    /// Record backtest start
    #[instrument]
    pub fn record_backtest_start(&self, job_id: &str) -> BacktestTimer {
        self.active_backtests.inc();
        self.backtest_total.inc();
        
        BacktestTimer {
            start_time: Instant::now(),
            job_id: job_id.to_string(),
            metrics: self,
        }
    }
    
    /// Record backtest completion
    pub fn record_backtest_completion(&self, timer: BacktestTimer, success: bool) {
        let duration = timer.start_time.elapsed();
        self.backtest_duration.observe(duration.as_secs_f64());
        self.active_backtests.dec();
        
        if !success {
            self.backtest_failures.inc();
        }
        
        info!(
            "Backtest {} completed in {:?}, success: {}",
            timer.job_id, duration, success
        );
    }
    
    /// Record indicator calculation
    pub fn record_indicator_calculation<F>(
        &self,
        indicator_name: &str,
        calculation: F,
    ) -> Result<F::Output>
    where
        F: FnOnce() -> Result<F::Output>,
    {
        let start_time = Instant::now();
        let result = calculation()?;
        let duration = start_time.elapsed();
        
        self.indicator_calculation_time
            .with_label_values(&[indicator_name])
            .observe(duration.as_secs_f64());
        
        self.indicators_calculated.inc();
        
        Ok(result)
    }
    
    /// Record trade execution
    pub fn record_trade_execution<F>(&self, execution: F) -> Result<F::Output>
    where
        F: FnOnce() -> Result<F::Output>,
    {
        let start_time = Instant::now();
        let result = execution()?;
        let duration = start_time.elapsed();
        
        self.trade_execution_time.observe(duration.as_secs_f64());
        self.trades_executed.inc();
        
        Ok(result)
    }
    
    /// Record memory allocation
    pub fn record_memory_allocation(&self, size_bytes: usize) {
        self.memory_allocation_size.observe(size_bytes as f64);
    }
    
    /// Update throughput metrics
    pub fn update_throughput(&self, symbol: &str, timeframe: &str, bars_per_second: f64) {
        self.bars_per_second
            .with_label_values(&[symbol, timeframe])
            .set(bars_per_second);
        
        self.throughput_by_symbol
            .with_label_values(&[symbol])
            .set(bars_per_second);
    }
    
    /// Update cache hit rate
    pub fn update_cache_hit_rate(&self, cache_type: &str, hit_rate: f64) {
        self.cache_hit_rate
            .with_label_values(&[cache_type])
            .set(hit_rate);
    }
    
    /// Update system metrics
    pub fn update_system_metrics(&self, memory_bytes: u64, cpu_percent: f64) {
        self.memory_usage_bytes.set(memory_bytes as f64);
        self.cpu_usage_percent.set(cpu_percent);
    }
    
    /// Update data quality score
    pub fn update_data_quality_score(&self, score: f64) {
        self.data_quality_score.set(score);
    }
    
    /// Record determinism violation
    pub fn record_determinism_violation(&self, job_id: &str) {
        self.determinism_violations.inc();
        error!("Determinism violation detected in job: {}", job_id);
    }
    
    /// Get metrics in Prometheus format
    pub fn get_metrics(&self) -> Result<String> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let encoded = encoder.encode_to_string(&metric_families)?;
        Ok(encoded)
    }
    
    /// Get performance budget status
    pub fn get_performance_budget_status(&self) -> PerformanceBudgetStatus {
        // Check if we're meeting performance targets
        let current_throughput = self.get_current_throughput();
        let target_throughput = 1000.0; // bars/second
        
        let throughput_ok = current_throughput >= target_throughput;
        let memory_ok = self.memory_usage_bytes.get() < 8_000_000_000.0; // 8GB
        let cpu_ok = self.cpu_usage_percent.get() < 80.0; // 80%
        
        PerformanceBudgetStatus {
            throughput_ok,
            memory_ok,
            cpu_ok,
            current_throughput,
            target_throughput,
            memory_usage_gb: self.memory_usage_bytes.get() / 1_000_000_000.0,
            cpu_usage_percent: self.cpu_usage_percent.get(),
        }
    }
    
    fn get_current_throughput(&self) -> f64 {
        // Calculate current throughput from recent metrics
        // This would be implemented based on actual throughput tracking
        1500.0 // Mock value
    }
}

/// Timer for tracking backtest execution
pub struct BacktestTimer<'a> {
    start_time: Instant,
    job_id: String,
    metrics: &'a MetricsCollector,
}

impl<'a> Drop for BacktestTimer<'a> {
    fn drop(&mut self) {
        // Timer is dropped when backtest completes
        // The actual completion is recorded by the caller
    }
}

/// Performance budget status
#[derive(Debug, Clone)]
pub struct PerformanceBudgetStatus {
    pub throughput_ok: bool,
    pub memory_ok: bool,
    pub cpu_ok: bool,
    pub current_throughput: f64,
    pub target_throughput: f64,
    pub memory_usage_gb: f64,
    pub cpu_usage_percent: f64,
}

impl PerformanceBudgetStatus {
    pub fn all_ok(&self) -> bool {
        self.throughput_ok && self.memory_ok && self.cpu_ok
    }
    
    pub fn get_violations(&self) -> Vec<String> {
        let mut violations = Vec::new();
        
        if !self.throughput_ok {
            violations.push(format!(
                "Throughput below target: {} < {} bars/sec",
                self.current_throughput, self.target_throughput
            ));
        }
        
        if !self.memory_ok {
            violations.push(format!(
                "Memory usage too high: {:.2} GB",
                self.memory_usage_gb
            ));
        }
        
        if !self.cpu_ok {
            violations.push(format!(
                "CPU usage too high: {:.1}%",
                self.cpu_usage_percent
            ));
        }
        
        violations
    }
}

/// OpenTelemetry tracing setup
pub fn setup_tracing() -> Result<()> {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
    
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "backtest_engine=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    
    Ok(())
}

/// Performance monitoring middleware
pub struct PerformanceMonitor {
    metrics: Arc<MetricsCollector>,
}

impl PerformanceMonitor {
    pub fn new(metrics: Arc<MetricsCollector>) -> Self {
        Self { metrics }
    }
    
    /// Monitor a function execution
    pub async fn monitor_execution<F, R>(
        &self,
        operation_name: &str,
        operation: F,
    ) -> Result<R>
    where
        F: std::future::Future<Output = Result<R>>,
    {
        let start_time = Instant::now();
        
        let result = operation.await;
        
        let duration = start_time.elapsed();
        
        match &result {
            Ok(_) => {
                info!("Operation {} completed successfully in {:?}", operation_name, duration);
            },
            Err(e) => {
                error!("Operation {} failed after {:?}: {}", operation_name, duration, e);
            }
        }
        
        result
    }
}

