//! Precision handling utilities for deterministic financial calculations
//! 
//! Ensures deterministic floating-point arithmetic and proper decimal precision
//! for all financial calculations including prices, quantities, and fees.

use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use std::ops::{Add, Sub, Mul, Div};
use anyhow::Result;

/// Precision configuration for financial calculations
#[derive(Debug, Clone)]
pub struct PrecisionConfig {
    pub price_precision: u8,
    pub quantity_precision: u8,
    pub fee_precision: u8,
    pub rounding_mode: RoundingMode,
}

impl Default for PrecisionConfig {
    fn default() -> Self {
        Self {
            price_precision: 8,
            quantity_precision: 8,
            fee_precision: 8,
            rounding_mode: RoundingMode::NearestEven,
        }
    }
}

/// Rounding modes for deterministic calculations
#[derive(Debug, Clone, Copy)]
pub enum RoundingMode {
    NearestEven,    // IEEE 754 default
    NearestAway,    // Round half away from zero
    TowardZero,     // Truncate toward zero
    TowardPositive, // Round toward positive infinity
    TowardNegative, // Round toward negative infinity
}

/// Precision-aware decimal operations
pub struct PreciseDecimal {
    value: Decimal,
    precision: u8,
    rounding_mode: RoundingMode,
}

impl PreciseDecimal {
    pub fn new(value: Decimal, precision: u8, rounding_mode: RoundingMode) -> Self {
        Self {
            value: Self::round_to_precision(value, precision, rounding_mode),
            precision,
            rounding_mode,
        }
    }
    
    pub fn from_f64(value: f64, precision: u8, rounding_mode: RoundingMode) -> Result<Self> {
        let decimal = Decimal::from_f64(value)
            .ok_or_else(|| anyhow::anyhow!("Invalid f64 value: {}", value))?;
        Ok(Self::new(decimal, precision, rounding_mode))
    }
    
    pub fn value(&self) -> Decimal {
        self.value
    }
    
    pub fn precision(&self) -> u8 {
        self.precision
    }
    
    /// Round to specified precision using configured rounding mode
    fn round_to_precision(value: Decimal, precision: u8, mode: RoundingMode) -> Decimal {
        let scale = 10_u64.pow(precision as u32);
        let scaled = value * Decimal::from(scale);
        
        let rounded = match mode {
            RoundingMode::NearestEven => scaled.round(),
            RoundingMode::NearestAway => scaled.round(),
            RoundingMode::TowardZero => scaled.trunc(),
            RoundingMode::TowardPositive => scaled.ceil(),
            RoundingMode::TowardNegative => scaled.floor(),
        };
        
        rounded / Decimal::from(scale)
    }
    
    /// Quantize to tick size
    pub fn quantize_to_tick(&self, tick_size: Decimal) -> Result<Self> {
        let quantized = (self.value / tick_size).round() * tick_size;
        Ok(Self::new(quantized, self.precision, self.rounding_mode))
    }
    
    /// Ensure minimum notional value
    pub fn ensure_min_notional(&self, min_notional: Decimal, quantity: Decimal) -> Result<Self> {
        let notional = self.value * quantity;
        if notional < min_notional {
            return Err(anyhow::anyhow!(
                "Notional value {} below minimum {}", 
                notional, 
                min_notional
            ));
        }
        Ok(self.clone())
    }
}

impl Clone for PreciseDecimal {
    fn clone(&self) -> Self {
        Self {
            value: self.value,
            precision: self.precision,
            rounding_mode: self.rounding_mode,
        }
    }
}

impl Add for PreciseDecimal {
    type Output = Self;
    
    fn add(self, rhs: Self) -> Self::Output {
        let result = self.value + rhs.value;
        Self::new(result, self.precision.max(rhs.precision), self.rounding_mode)
    }
}

impl Sub for PreciseDecimal {
    type Output = Self;
    
    fn sub(self, rhs: Self) -> Self::Output {
        let result = self.value - rhs.value;
        Self::new(result, self.precision.max(rhs.precision), self.rounding_mode)
    }
}

impl Mul for PreciseDecimal {
    type Output = Self;
    
    fn mul(self, rhs: Self) -> Self::Output {
        let result = self.value * rhs.value;
        Self::new(result, self.precision.max(rhs.precision), self.rounding_mode)
    }
}

impl Div for PreciseDecimal {
    type Output = Self;
    
    fn div(self, rhs: Self) -> Self::Output {
        if rhs.value == Decimal::ZERO {
            panic!("Division by zero");
        }
        let result = self.value / rhs.value;
        Self::new(result, self.precision.max(rhs.precision), self.rounding_mode)
    }
}

/// Floating-point configuration for deterministic calculations
pub struct FloatConfig {
    pub rounding_mode: RoundingMode,
    pub enable_fma: bool,
    pub enable_fast_math: bool,
}

impl Default for FloatConfig {
    fn default() -> Self {
        Self {
            rounding_mode: RoundingMode::NearestEven,
            enable_fma: false,      // Disable FMA for determinism
            enable_fast_math: false, // Disable fast-math for determinism
        }
    }
}

/// Set floating-point rounding mode (platform-specific)
pub fn set_fp_rounding_mode(mode: RoundingMode) -> Result<()> {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::*;
        
        let control_word = unsafe { _mm_getcsr() };
        let new_control_word = match mode {
            RoundingMode::NearestEven => control_word & !0x6000, // Clear rounding bits
            RoundingMode::TowardZero => control_word | 0x2000,    // Set to 01
            RoundingMode::TowardNegative => control_word | 0x4000, // Set to 10
            RoundingMode::TowardPositive => control_word | 0x6000, // Set to 11
            RoundingMode::NearestAway => control_word & !0x6000,   // Same as nearest even
        };
        
        unsafe { _mm_setcsr(new_control_word) };
    }
    
    #[cfg(not(target_arch = "x86_64"))]
    {
        // For non-x86_64 platforms, we rely on Decimal for precision
        tracing::warn!("FP rounding mode setting not supported on this platform");
    }
    
    Ok(())
}

/// Validate floating-point determinism
pub fn validate_fp_determinism() -> Result<()> {
    // Test that floating-point operations are deterministic
    let test_values = vec![0.1, 0.2, 0.3, 0.4, 0.5];
    
    for _ in 0..10 {
        let mut sum = 0.0;
        for &val in &test_values {
            sum += val;
        }
        
        // The sum should be exactly the same every time
        let expected = 1.5;
        if (sum - expected).abs() > 1e-15f64 {
            return Err(anyhow::anyhow!(
                "Floating-point determinism check failed: {} != {}", 
                sum, 
                expected
            ));
        }
    }
    
    Ok(())
}

/// Convert f64 to Decimal with proper precision handling
pub fn f64_to_decimal(value: f64, precision: u8) -> Result<Decimal> {
    let decimal = Decimal::from_f64(value)
        .ok_or_else(|| anyhow::anyhow!("Invalid f64 value: {}", value))?;
    
    // Round to specified precision
    let scale = 10_u64.pow(precision as u32);
    let scaled = decimal * Decimal::from(scale);
    let rounded = scaled.round();
    let result = rounded / Decimal::from(scale);
    
    Ok(result)
}

/// Convert Decimal to f64 with precision loss warning
pub fn decimal_to_f64(value: Decimal) -> Result<f64> {
    value.to_f64()
        .ok_or_else(|| anyhow::anyhow!("Decimal too large for f64: {}", value))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_precision_rounding() {
        let config = PrecisionConfig::default();
        let precise = PreciseDecimal::from_f64(1.23456789, 4, RoundingMode::NearestEven).unwrap();
        assert_eq!(precise.value(), dec!(1.2346));
    }
    
    #[test]
    fn test_tick_quantization() {
        let price = PreciseDecimal::from_f64(100.123456, 8, RoundingMode::NearestEven).unwrap();
        let tick_size = dec!(0.01);
        let quantized = price.quantize_to_tick(tick_size).unwrap();
        assert_eq!(quantized.value(), dec!(100.12));
    }
    
    #[test]
    fn test_min_notional_check() {
        let price = PreciseDecimal::from_f64(100.0, 8, RoundingMode::NearestEven).unwrap();
        let quantity = dec!(0.05); // $5 notional
        let min_notional = dec!(10.0);
        
        let result = price.ensure_min_notional(min_notional, quantity);
        assert!(result.is_err());
    }
}

