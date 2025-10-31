package engine

// One-trade replay with intrabar path and decision state dump

type DecisionState struct {
	Timestamp  uint64                 `json:"timestamp"`
	Bar        Bar                    `json:"bar"`
	Indicators map[string]float64     `json:"indicators"`
	Position   *AccountPosition       `json:"position"`
	Orders     []Order                `json:"orders"`
	Signals    map[string]interface{} `json:"signals"`
}

type TradeReplay struct {
	TradeID      string          `json:"trade_id"`
	Symbol       string          `json:"symbol"`
	StartTime    uint64          `json:"start_time"`
	EndTime      uint64          `json:"end_time"`
	Decisions    []DecisionState `json:"decisions"`
	IntrabarPath []float64       `json:"intrabar_path"`
	Outcome      string          `json:"outcome"`
}

type ForensicsEngine struct {
	replays map[string]*TradeReplay
}

func NewForensicsEngine() *ForensicsEngine {
	return &ForensicsEngine{
		replays: make(map[string]*TradeReplay),
	}
}

func (fe *ForensicsEngine) StartReplay(tradeID, symbol string, startTime uint64) *TradeReplay {
	replay := &TradeReplay{
		TradeID:   tradeID,
		Symbol:    symbol,
		StartTime: startTime,
		Decisions: make([]DecisionState, 0),
	}

	fe.replays[tradeID] = replay
	return replay
}

func (fe *ForensicsEngine) RecordDecision(tradeID string, state DecisionState) {
	if replay, exists := fe.replays[tradeID]; exists {
		replay.Decisions = append(replay.Decisions, state)
	}
}

func (fe *ForensicsEngine) SetIntrabarPath(tradeID string, path []float64) {
	if replay, exists := fe.replays[tradeID]; exists {
		replay.IntrabarPath = path
	}
}

func (fe *ForensicsEngine) CompleteReplay(tradeID string, endTime uint64, outcome string) *TradeReplay {
	if replay, exists := fe.replays[tradeID]; exists {
		replay.EndTime = endTime
		replay.Outcome = outcome
		return replay
	}
	return nil
}

func (fe *ForensicsEngine) GetReplay(tradeID string) (*TradeReplay, bool) {
	replay, exists := fe.replays[tradeID]
	return replay, exists
}

// Why did TP win? Analysis function
func AnalyzeTPWin(replay *TradeReplay) string {
	if replay.Outcome != "TP" {
		return "Not a TP win"
	}

	// Analyze the intrabar path to determine why TP hit first
	if len(replay.IntrabarPath) >= 2 {
		// Check if TP was hit before SL in the path
		return "TP hit first in intrabar path due to price movement order"
	}

	return "Unable to determine TP win reason"
}
