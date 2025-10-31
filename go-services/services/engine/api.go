package engine

// End-to-end API with error taxonomy

type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

var (
	ErrInvalidStrategy = APIError{Code: "INVALID_STRATEGY", Message: "Strategy compilation failed"}
	ErrInvalidParams   = APIError{Code: "INVALID_PARAMS", Message: "Invalid parameters provided"}
	ErrDataNotFound    = APIError{Code: "DATA_NOT_FOUND", Message: "Required data not available"}
	ErrExecutionFailed = APIError{Code: "EXECUTION_FAILED", Message: "Strategy execution failed"}
	ErrTimeout         = APIError{Code: "TIMEOUT", Message: "Operation timed out"}
)

type StrategySubmitRequest struct {
	Source     string            `json:"source"`
	Language   string            `json:"language"`
	Parameters map[string]string `json:"parameters"`
}

type StrategySubmitResponse struct {
	StrategyID string    `json:"strategy_id"`
	Status     string    `json:"status"`
	Error      *APIError `json:"error,omitempty"`
}

type BacktestRunRequest struct {
	StrategyID string   `json:"strategy_id"`
	Symbols    []string `json:"symbols"`
	StartTime  uint64   `json:"start_time"`
	EndTime    uint64   `json:"end_time"`
	Timeframe  string   `json:"timeframe"`
}

type BacktestRunResponse struct {
	JobID  string    `json:"job_id"`
	Status string    `json:"status"`
	Error  *APIError `json:"error,omitempty"`
}

type BacktestResultRequest struct {
	JobID string `json:"job_id"`
}

type BacktestResultResponse struct {
	JobID   string          `json:"job_id"`
	Status  string          `json:"status"`
	Results *BacktestResult `json:"results,omitempty"`
	Error   *APIError       `json:"error,omitempty"`
}

type APIService struct {
	engine  *EngineClient
	planner *Planner
}

func NewAPIService() *APIService {
	return &APIService{
		engine:  &EngineClient{},
		planner: NewPlanner(10, 4),
	}
}

func (api *APIService) SubmitStrategy(req StrategySubmitRequest) StrategySubmitResponse {
	// Stub implementation
	return StrategySubmitResponse{
		StrategyID: "strategy_123",
		Status:     "compiled",
	}
}

func (api *APIService) RunBacktest(req BacktestRunRequest) BacktestRunResponse {
	// Stub implementation
	return BacktestRunResponse{
		JobID:  "job_456",
		Status: "running",
	}
}

func (api *APIService) GetResults(req BacktestResultRequest) BacktestResultResponse {
	// Stub implementation
	return BacktestResultResponse{
		JobID:  req.JobID,
		Status: "completed",
		Results: &BacktestResult{
			JobID: req.JobID,
		},
	}
}
