package engine

// Performance benchmarks and SLOs

import (
	"time"
)

type BenchmarkResult struct {
	Name       string
	Duration   time.Duration
	BarsPerSec float64
	MemoryMB   float64
}

type SLOConfig struct {
	MaxLatencyP50 time.Duration
	MaxLatencyP95 time.Duration
	MaxLatencyP99 time.Duration
	MinBarsPerSec float64
	MaxMemoryMB   float64
}

type PerformanceMonitor struct {
	config  SLOConfig
	results []BenchmarkResult
}

func NewPerformanceMonitor(config SLOConfig) *PerformanceMonitor {
	return &PerformanceMonitor{
		config:  config,
		results: make([]BenchmarkResult, 0),
	}
}

func (pm *PerformanceMonitor) RecordBenchmark(name string, duration time.Duration, bars int, memoryMB float64) {
	barsPerSec := float64(bars) / duration.Seconds()

	result := BenchmarkResult{
		Name:       name,
		Duration:   duration,
		BarsPerSec: barsPerSec,
		MemoryMB:   memoryMB,
	}

	pm.results = append(pm.results, result)
}

func (pm *PerformanceMonitor) CheckSLOs() []string {
	var violations []string

	for _, result := range pm.results {
		if result.Duration > pm.config.MaxLatencyP50 {
			violations = append(violations, result.Name+" exceeded P50 latency")
		}
		if result.BarsPerSec < pm.config.MinBarsPerSec {
			violations = append(violations, result.Name+" below minimum bars/sec")
		}
		if result.MemoryMB > pm.config.MaxMemoryMB {
			violations = append(violations, result.Name+" exceeded memory limit")
		}
	}

	return violations
}

// Micro-benchmark for indicators
func BenchmarkIndicators(values []float64, period int) BenchmarkResult {
	start := time.Now()

	calc := &SIMDCalculator{}
	calc.CalculateSMA(values, period)
	calc.CalculateEMA(values, period)
	calc.CalculateRSI(values, period)

	duration := time.Since(start)

	return BenchmarkResult{
		Name:       "indicators",
		Duration:   duration,
		BarsPerSec: float64(len(values)) / duration.Seconds(),
		MemoryMB:   0, // Would measure actual memory usage
	}
}
