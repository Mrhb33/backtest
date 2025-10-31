// TypeScript WASM Strategy Example
// This demonstrates how to write a trading strategy in TypeScript that compiles to WASM

export interface StrategyConfig {
  emaPeriod: number;
  rsiPeriod: number;
  rsiOversold: number;
  rsiOverbought: number;
  positionSize: number;
  stopLossPct: number;
  takeProfitPct: number;
}

export interface MarketBar {
  timestamp: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface IndicatorValue {
  timestamp: number;
  value: number;
}

export interface TradingSignal {
  side: 'buy' | 'sell';
  size: number;
  entryPrice?: number;
  stopLoss?: number;
  takeProfit?: number;
  timeToLive?: number;
}

export interface Position {
  symbol: string;
  quantity: number;
  avgPrice: number;
  unrealizedPnl: number;
  realizedPnl: number;
}

export interface StrategyMetadata {
  name: string;
  version: string;
  description: string;
  author: string;
  requiredIndicators: string[];
  parameters: Record<string, string>;
}

export class BollingerBandsStrategy {
  private config: StrategyConfig;
  private position: Position | null = null;
  private bbValues: IndicatorValue[] = [];
  private lastSignalTime: number = 0;

  constructor(config: StrategyConfig) {
    this.config = config;
  }

  /**
   * Process a new bar and generate trading signals
   */
  processBar(
    bar: MarketBar,
    bbValues: IndicatorValue[]
  ): TradingSignal[] {
    const signals: TradingSignal[] = [];
    
    // Update indicator values
    this.bbValues = bbValues;
    
    // Need at least 2 values for analysis
    if (this.bbValues.length < 2) {
      return signals;
    }

    const currentBB = this.bbValues[this.bbValues.length - 1];
    const previousBB = this.bbValues[this.bbValues.length - 2];
    
    // Calculate Bollinger Band position
    const bbPosition = this.calculateBBPosition(bar.close, currentBB.value);
    const bbTrend = currentBB.value > previousBB.value ? 'up' : 'down';
    
    // Generate signals based on strategy logic
    if (!this.position) {
      // No position - look for entry signals
      if (this.shouldEnterLong(bar, bbPosition, bbTrend)) {
        signals.push(this.createLongSignal(bar));
      } else if (this.shouldEnterShort(bar, bbPosition, bbTrend)) {
        signals.push(this.createShortSignal(bar));
      }
    } else {
      // Have position - check for exit signals
      if (this.shouldExitPosition(bar, this.position)) {
        signals.push(this.createExitSignal(bar, this.position));
      }
    }

    this.lastSignalTime = bar.timestamp;
    return signals;
  }

  /**
   * Calculate Bollinger Band position (0 = lower band, 1 = upper band)
   */
  private calculateBBPosition(price: number, bbValue: number): number {
    // This is a simplified calculation - in practice you'd have upper/lower bands
    // For this example, we'll use a mock calculation
    const lowerBand = bbValue * 0.95;
    const upperBand = bbValue * 1.05;
    
    if (price <= lowerBand) return 0;
    if (price >= upperBand) return 1;
    
    return (price - lowerBand) / (upperBand - lowerBand);
  }

  /**
   * Determine if we should enter a long position
   */
  private shouldEnterLong(
    bar: MarketBar,
    bbPosition: number,
    bbTrend: string
  ): boolean {
    const timeSinceLastSignal = bar.timestamp - this.lastSignalTime;
    const minTimeBetweenSignals = 300000; // 5 minutes

    // Long entry conditions:
    // 1. Price near lower Bollinger Band (oversold)
    // 2. Bollinger Band trend is up (momentum)
    // 3. Haven't signaled recently
    return (
      bbPosition < 0.2 && // Near lower band
      bbTrend === 'up' &&
      timeSinceLastSignal > minTimeBetweenSignals
    );
  }

  /**
   * Determine if we should enter a short position
   */
  private shouldEnterShort(
    bar: MarketBar,
    bbPosition: number,
    bbTrend: string
  ): boolean {
    const timeSinceLastSignal = bar.timestamp - this.lastSignalTime;
    const minTimeBetweenSignals = 300000; // 5 minutes

    // Short entry conditions:
    // 1. Price near upper Bollinger Band (overbought)
    // 2. Bollinger Band trend is down (momentum)
    // 3. Haven't signaled recently
    return (
      bbPosition > 0.8 && // Near upper band
      bbTrend === 'down' &&
      timeSinceLastSignal > minTimeBetweenSignals
    );
  }

  /**
   * Determine if we should exit current position
   */
  private shouldExitPosition(bar: MarketBar, position: Position): boolean {
    const pnlPct = (bar.close - position.avgPrice) / position.avgPrice;

    // Stop loss
    if (pnlPct <= -this.config.stopLossPct) {
      return true;
    }

    // Take profit
    if (pnlPct >= this.config.takeProfitPct) {
      return true;
    }

    // Bollinger Band reversal
    const currentBB = this.bbValues[this.bbValues.length - 1];
    const bbPosition = this.calculateBBPosition(bar.close, currentBB.value);
    
    if (position.quantity > 0 && bbPosition > 0.8) {
      return true; // Long position, price at upper band
    }
    if (position.quantity < 0 && bbPosition < 0.2) {
      return true; // Short position, price at lower band
    }

    return false;
  }

  /**
   * Create a long entry signal
   */
  private createLongSignal(bar: MarketBar): TradingSignal {
    const stopLoss = bar.close * (1 - this.config.stopLossPct);
    const takeProfit = bar.close * (1 + this.config.takeProfitPct);

    return {
      side: 'buy',
      size: this.config.positionSize,
      entryPrice: bar.close,
      stopLoss,
      takeProfit,
      timeToLive: 3600000, // 1 hour
    };
  }

  /**
   * Create a short entry signal
   */
  private createShortSignal(bar: MarketBar): TradingSignal {
    const stopLoss = bar.close * (1 + this.config.stopLossPct);
    const takeProfit = bar.close * (1 - this.config.takeProfitPct);

    return {
      side: 'sell',
      size: this.config.positionSize,
      entryPrice: bar.close,
      stopLoss,
      takeProfit,
      timeToLive: 3600000, // 1 hour
    };
  }

  /**
   * Create an exit signal
   */
  private createExitSignal(bar: MarketBar, position: Position): TradingSignal {
    return {
      side: position.quantity > 0 ? 'sell' : 'buy',
      size: Math.abs(position.quantity),
      entryPrice: bar.close,
      timeToLive: 60000, // 1 minute
    };
  }

  /**
   * Update position after trade execution
   */
  updatePosition(position: Position | null): void {
    this.position = position;
  }

  /**
   * Get strategy metadata
   */
  getMetadata(): StrategyMetadata {
    return {
      name: 'BollingerBandsStrategy',
      version: '1.0.0',
      description: 'Bollinger Bands mean reversion strategy',
      author: 'backtest-engine',
      requiredIndicators: ['bb'],
      parameters: {
        emaPeriod: this.config.emaPeriod.toString(),
        rsiPeriod: this.config.rsiPeriod.toString(),
        rsiOversold: this.config.rsiOversold.toString(),
        rsiOverbought: this.config.rsiOverbought.toString(),
        positionSize: this.config.positionSize.toString(),
        stopLossPct: this.config.stopLossPct.toString(),
        takeProfitPct: this.config.takeProfitPct.toString(),
      },
    };
  }
}

// Default configuration
export const defaultConfig: StrategyConfig = {
  emaPeriod: 20,
  rsiPeriod: 14,
  rsiOversold: 30,
  rsiOverbought: 70,
  positionSize: 0.1, // 10% of equity
  stopLossPct: 0.02, // 2% stop loss
  takeProfitPct: 0.04, // 4% take profit
};

// WASM ABI functions (these would be generated by the TypeScript to WASM compiler)
export function strategyInit(configJson: string): BollingerBandsStrategy {
  const config = JSON.parse(configJson) as StrategyConfig;
  return new BollingerBandsStrategy(config);
}

export function strategyProcessBar(
  strategy: BollingerBandsStrategy,
  barJson: string,
  bbValuesJson: string
): string {
  const bar = JSON.parse(barJson) as MarketBar;
  const bbValues = JSON.parse(bbValuesJson) as IndicatorValue[];
  
  const signals = strategy.processBar(bar, bbValues);
  return JSON.stringify(signals);
}

export function strategyGetMetadata(strategy: BollingerBandsStrategy): string {
  const metadata = strategy.getMetadata();
  return JSON.stringify(metadata);
}

export function strategyUpdatePosition(
  strategy: BollingerBandsStrategy,
  positionJson: string
): void {
  const position = positionJson ? JSON.parse(positionJson) as Position : null;
  strategy.updatePosition(position);
}

