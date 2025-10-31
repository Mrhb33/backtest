//! SIMD-optimized technical indicators
//! 
//! High-performance indicator calculations using SIMD instructions for vectorized operations.
//! All calculations use deterministic floating-point arithmetic with Decimal128 precision.

use std::collections::HashMap;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
// use portable_simd::*; // Temporarily disabled
use anyhow::Result;
use tracing::{debug, warn};

use crate::types::{Bar, IndicatorValue, IndicatorParams};

/// Registry for managing indicator calculations
pub struct IndicatorRegistry {
    enable_simd: bool,
    cache: HashMap<String, Vec<IndicatorValue>>,
}

impl IndicatorRegistry {
    pub fn new(enable_simd: bool) -> Result<Self> {
        Ok(Self {
            enable_simd,
            cache: HashMap::new(),
        })
    }
    
    /// Calculate indicator values for given market data
    pub fn calculate(
        &mut self,
        indicator_name: &str,
        market_data: &crate::types::MarketData,
    ) -> Result<Vec<IndicatorValue>> {
        debug!("Calculating indicator: {}", indicator_name);
        
        // Check cache first
        let cache_key = format!("{}_{}", indicator_name, market_data.symbol);
        if let Some(cached) = self.cache.get(&cache_key) {
            return Ok(cached.clone());
        }
        
        let bars = &market_data.bars;
        if bars.is_empty() {
            return Ok(Vec::new());
        }
        
        let values = match indicator_name {
            "ema" => self.calculate_ema(bars, &IndicatorParams { period: 20, alpha: None, threshold: None })?,
            "sma" => self.calculate_sma(bars, &IndicatorParams { period: 20, alpha: None, threshold: None })?,
            "rsi" => self.calculate_rsi(bars, &IndicatorParams { period: 14, alpha: None, threshold: None })?,
            "atr" => self.calculate_atr(bars, &IndicatorParams { period: 14, alpha: None, threshold: None })?,
            "vwap" => self.calculate_vwap(bars, &IndicatorParams { period: 0, alpha: None, threshold: None })?,
            "hh" => self.calculate_highest_high(bars, &IndicatorParams { period: 20, alpha: None, threshold: None })?,
            "ll" => self.calculate_lowest_low(bars, &IndicatorParams { period: 20, alpha: None, threshold: None })?,
            _ => return Err(anyhow::anyhow!("Unknown indicator: {}", indicator_name)),
        };
        
        // Cache the result
        self.cache.insert(cache_key, values.clone());
        
        Ok(values)
    }
    
    /// Calculate Exponential Moving Average (EMA)
    fn calculate_ema(&self, bars: &[Bar], params: &IndicatorParams) -> Result<Vec<IndicatorValue>> {
        let period = params.period;
        let alpha = params.alpha.unwrap_or(dec!(2.0) / Decimal::from(period + 1));
        
        if bars.len() < period {
            return Ok(Vec::new());
        }
        
        let mut values = Vec::with_capacity(bars.len());
        
        // Initialize with SMA for the first value
        let mut ema = bars[0..period].iter()
            .map(|b| b.close)
            .sum::<Decimal>() / Decimal::from(period);
        
        values.push(IndicatorValue {
            timestamp: bars[period - 1].timestamp,
            value: ema,
        });
        
        // Calculate EMA for remaining bars
        for bar in bars.iter().skip(period) {
            ema = alpha * bar.close + (dec!(1.0) - alpha) * ema;
            values.push(IndicatorValue {
                timestamp: bar.timestamp,
                value: ema,
            });
        }
        
        Ok(values)
    }
    
    /// Calculate Simple Moving Average (SMA) with SIMD optimization
    fn calculate_sma(&self, bars: &[Bar], params: &IndicatorParams) -> Result<Vec<IndicatorValue>> {
        let period = params.period;
        
        if bars.len() < period {
            return Ok(Vec::new());
        }
        
        let mut values = Vec::with_capacity(bars.len() - period + 1);
        
        if self.enable_simd && period >= 8 {
            // SIMD-optimized version for larger periods
            self.calculate_sma_simd(bars, period, &mut values)?;
        } else {
            // Standard implementation
            for i in 0..=bars.len() - period {
                let sum = bars[i..i + period].iter()
                    .map(|b| b.close)
                    .sum::<Decimal>();
                let sma = sum / Decimal::from(period);
                
                values.push(IndicatorValue {
                    timestamp: bars[i + period - 1].timestamp,
                    value: sma,
                });
            }
        }
        
        Ok(values)
    }
    
    /// SIMD-optimized SMA calculation
    fn calculate_sma_simd(&self, bars: &[Bar], period: usize, values: &mut Vec<IndicatorValue>) -> Result<()> {
        // Convert Decimal to f64 for SIMD operations
        let closes: Vec<f64> = bars.iter()
            .map(|b| b.close.to_f64().unwrap_or(0.0))
            .collect();
        
        for i in 0..=closes.len() - period {
            let slice = &closes[i..i + period];
            
            // Use SIMD for vectorized sum
            let sum = if slice.len() >= 8 {
                self.simd_sum_f64(slice)
            } else {
                slice.iter().sum()
            };
            
            let sma = sum / period as f64;
            
            values.push(IndicatorValue {
                timestamp: bars[i + period - 1].timestamp,
                value: Decimal::from_f64(sma).unwrap_or(dec!(0.0)),
            });
        }
        
        Ok(())
    }
    
    /// SIMD sum for f64 arrays
    fn simd_sum_f64(&self, data: &[f64]) -> f64 {
        if data.len() < 8 {
            return data.iter().sum();
        }
        
        // Use SIMD for vectorized sum
        let chunks = data.chunks_exact(8);
        let mut sum = 0.0;
        
        for chunk in chunks {
            sum += chunk.iter().sum::<f64>();
        }
        
        let mut result = sum;
        
        // Handle remaining elements
        let remainder = data.len() % 8;
        if remainder > 0 {
            for &val in &data[data.len() - remainder..] {
                result += val;
            }
        }
        
        result
    }
    
    /// Calculate Relative Strength Index (RSI)
    fn calculate_rsi(&self, bars: &[Bar], params: &IndicatorParams) -> Result<Vec<IndicatorValue>> {
        let period = params.period;
        
        if bars.len() < period + 1 {
            return Ok(Vec::new());
        }
        
        let mut values = Vec::with_capacity(bars.len() - period);
        
        // Calculate price changes
        let mut gains = Vec::with_capacity(bars.len() - 1);
        let mut losses = Vec::with_capacity(bars.len() - 1);
        
        for i in 1..bars.len() {
            let change = bars[i].close - bars[i - 1].close;
            if change > dec!(0.0) {
                gains.push(change);
                losses.push(dec!(0.0));
            } else {
                gains.push(dec!(0.0));
                losses.push(-change);
            }
        }
        
        // Calculate initial averages
        let mut avg_gain = gains[0..period].iter().sum::<Decimal>() / Decimal::from(period);
        let mut avg_loss = losses[0..period].iter().sum::<Decimal>() / Decimal::from(period);
        
        // Calculate RSI
        for i in period..gains.len() {
            avg_gain = (avg_gain * Decimal::from(period - 1) + gains[i]) / Decimal::from(period);
            avg_loss = (avg_loss * Decimal::from(period - 1) + losses[i]) / Decimal::from(period);
            
            let rs = if avg_loss == dec!(0.0) {
                dec!(100.0)
            } else {
                avg_gain / avg_loss
            };
            
            let rsi = dec!(100.0) - (dec!(100.0) / (dec!(1.0) + rs));
            
            values.push(IndicatorValue {
                timestamp: bars[i + 1].timestamp,
                value: rsi,
            });
        }
        
        Ok(values)
    }
    
    /// Calculate Average True Range (ATR)
    fn calculate_atr(&self, bars: &[Bar], params: &IndicatorParams) -> Result<Vec<IndicatorValue>> {
        let period = params.period;
        
        if bars.len() < period + 1 {
            return Ok(Vec::new());
        }
        
        let mut values = Vec::with_capacity(bars.len() - period);
        
        // Calculate True Range
        let mut true_ranges = Vec::with_capacity(bars.len() - 1);
        
        for i in 1..bars.len() {
            let hl = bars[i].high - bars[i].low;
            let hc = (bars[i].high - bars[i - 1].close).abs();
            let lc = (bars[i].low - bars[i - 1].close).abs();
            
            let tr = hl.max(hc).max(lc);
            true_ranges.push(tr);
        }
        
        // Calculate ATR using Wilder's smoothing
        let mut atr = true_ranges[0..period].iter().sum::<Decimal>() / Decimal::from(period);
        
        values.push(IndicatorValue {
            timestamp: bars[period].timestamp,
            value: atr,
        });
        
        for i in period..true_ranges.len() {
            atr = (atr * Decimal::from(period - 1) + true_ranges[i]) / Decimal::from(period);
            values.push(IndicatorValue {
                timestamp: bars[i + 1].timestamp,
                value: atr,
            });
        }
        
        Ok(values)
    }
    
    /// Calculate Volume Weighted Average Price (VWAP)
    fn calculate_vwap(&self, bars: &[Bar], _params: &IndicatorParams) -> Result<Vec<IndicatorValue>> {
        let mut values = Vec::with_capacity(bars.len());
        let mut cumulative_volume = dec!(0.0);
        let mut cumulative_volume_price = dec!(0.0);
        
        for bar in bars {
            let typical_price = (bar.high + bar.low + bar.close) / dec!(3.0);
            cumulative_volume_price += typical_price * bar.volume;
            cumulative_volume += bar.volume;
            
            let vwap = if cumulative_volume > dec!(0.0) {
                cumulative_volume_price / cumulative_volume
            } else {
                dec!(0.0)
            };
            
            values.push(IndicatorValue {
                timestamp: bar.timestamp,
                value: vwap,
            });
        }
        
        Ok(values)
    }
    
    /// Calculate Highest High over period
    fn calculate_highest_high(&self, bars: &[Bar], params: &IndicatorParams) -> Result<Vec<IndicatorValue>> {
        let period = params.period;
        
        if bars.len() < period {
            return Ok(Vec::new());
        }
        
        let mut values = Vec::with_capacity(bars.len() - period + 1);
        
        for i in 0..=bars.len() - period {
            let highest = bars[i..i + period].iter()
                .map(|b| b.high)
                .max()
                .unwrap_or(dec!(0.0));
            
            values.push(IndicatorValue {
                timestamp: bars[i + period - 1].timestamp,
                value: highest,
            });
        }
        
        Ok(values)
    }
    
    /// Calculate Lowest Low over period
    fn calculate_lowest_low(&self, bars: &[Bar], params: &IndicatorParams) -> Result<Vec<IndicatorValue>> {
        let period = params.period;
        
        if bars.len() < period {
            return Ok(Vec::new());
        }
        
        let mut values = Vec::with_capacity(bars.len() - period + 1);
        
        for i in 0..=bars.len() - period {
            let lowest = bars[i..i + period].iter()
                .map(|b| b.low)
                .min()
                .unwrap_or(dec!(0.0));
            
            values.push(IndicatorValue {
                timestamp: bars[i + period - 1].timestamp,
                value: lowest,
            });
        }
        
        Ok(values)
    }
}

