package engine

type EngineClient struct{}

func NewClient(cfg any) (*EngineClient, error) { return &EngineClient{}, nil }

type BacktestJob struct {
	JobID            string
	Symbols          []string
	Timeframe        string
	StartTime        uint64
	EndTime          uint64
	IntrabarPolicy   IntrabarPolicy
	FeeVersion       string
	SlippageMode     SlippageMode
	StrategyWasmHash string
	SnapshotID       string
}

type IntrabarPolicy string

const (
	IntrabarPolicyExactTrades         IntrabarPolicy = "EXACT_TRADES"
	IntrabarPolicyOneSecondBars       IntrabarPolicy = "ONE_SECOND_BARS"
	IntrabarPolicyLinearInterpolation IntrabarPolicy = "LINEAR_INTERPOLATION"
)

type SlippageMode string

const (
	SlippageModeNone          SlippageMode = "NONE"
	SlippageModeTradeSweep    SlippageMode = "TRADE_SWEEP"
	SlippageModeSyntheticBook SlippageMode = "SYNTHETIC_BOOK"
)

type SymbolBacktestRequest struct {
	JobID      string
	Symbol     string
	ArrowData  []byte
	Strategy   string
	Parameters map[string]string
}

type TradeSide string

const (
	TradeSideBuy  TradeSide = "BUY"
	TradeSideSell TradeSide = "SELL"
)

type ExecutedTrade struct {
	Timestamp  uint64
	Symbol     string
	Side       TradeSide
	Quantity   decimalString
	Price      decimalString
	Fee        decimalString
	Slippage   decimalString
	ReasonCode string
}

type Position struct {
	Timestamp     uint64
	Symbol        string
	Quantity      decimalString
	AvgPrice      decimalString
	UnrealizedPnl decimalString
	RealizedPnl   decimalString
}

type EquityPoint struct {
	Timestamp int64
	Equity    decimalString
	Drawdown  decimalString
	Exposure  decimalString
}

type PerformanceMetrics struct{}

type RunManifest struct {
	JobID          string
	SnapshotID     string
	EngineVersion  string
	StrategyHash   string
	IntrabarPolicy string
	FeeVersion     string
	SlippageMode   string
	CreatedAt      uint64
	CpuFeatures    []string
	FpFlags        string
}

type SymbolResult struct {
	Symbol      string
	Trades      []*ExecutedTrade
	Positions   []*Position
	EquityCurve []*EquityPoint
	Drawdown    decimalString
	Exposure    decimalString
}

type BacktestResult struct {
	JobID              string
	ExecutionTimeMs    uint64
	SymbolResults      []*SymbolResult
	PerformanceMetrics *PerformanceMetrics
	Manifest           *RunManifest
}

type decimalString string

func (c *EngineClient) ExecuteSymbolBacktest(_ any, _ *SymbolBacktestRequest) (*SymbolResult, error) {
	return &SymbolResult{
		Symbol:      "TEST",
		Trades:      []*ExecutedTrade{},
		Positions:   []*Position{},
		EquityCurve: []*EquityPoint{},
		Drawdown:    "0",
		Exposure:    "0",
	}, nil
}
