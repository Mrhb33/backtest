package proto

type BacktestRequest struct {
	Symbols          []string `json:"symbols"`
	Timeframe        string   `json:"timeframe"`
	StartTime        int64    `json:"start_time"`
	EndTime          int64    `json:"end_time"`
	IntrabarPolicy   IntrabarPolicy
	SlippageMode     SlippageMode
	FeeVersion       string `json:"fee_version"`
	StrategyWasmHash string `json:"strategy_wasm_hash"`
	SnapshotId       string `json:"snapshot_id"`
}

type IntrabarPolicy int32

const (
	IntrabarPolicy_EXACT_TRADES         IntrabarPolicy = 0
	IntrabarPolicy_ONE_SECOND_BARS      IntrabarPolicy = 1
	IntrabarPolicy_LINEAR_INTERPOLATION IntrabarPolicy = 2
)

type SlippageMode int32

const (
	SlippageMode_NONE           SlippageMode = 0
	SlippageMode_TRADE_SWEEP    SlippageMode = 1
	SlippageMode_SYNTHETIC_BOOK SlippageMode = 2
)

type ExecutedTrade struct{}

type Position struct{}

type EquityPoint struct{}

type RunManifest struct{}

type BacktestResponse struct {
	JobId         string
	ExecutionTime int64
	SymbolResults []*SymbolResult
	Manifest      *RunManifest
}

type SymbolResult struct {
	Symbol      string
	Trades      []*ExecutedTrade
	Positions   []*Position
	EquityCurve []*EquityPoint
	Drawdown    string
	Exposure    string
}

// gRPC server interface stub

type UnimplementedBacktestServiceServer struct{}

func RegisterBacktestServiceServer(_ any, _ BacktestServiceServer) {}

type BacktestServiceServer interface {
	ExecuteBacktest(any, *BacktestRequest) (*BacktestResponse, error)
}
