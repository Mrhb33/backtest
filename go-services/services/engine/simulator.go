package engine

// Minimal simulator that applies first-touch TP/SL per bar

type PositionSide int

const (
	SideFlat PositionSide = iota
	SideLong
	SideShort
)

type PositionState struct {
	Side     PositionSide
	Entry    float64
	Quantity float64
	TP       float64
	SL       float64
}

type SimConfig struct{}

type Simulator struct {
	cfg SimConfig
	log *EventLog
}

func NewSimulator(cfg SimConfig, log *EventLog) *Simulator { return &Simulator{cfg: cfg, log: log} }

// Step processes a single bar with optional tp/sl
func (s *Simulator) Step(ts uint64, symbol string, bar Bar, pos *PositionState) {
	if pos.Side == SideLong {
		switch ResolveFirstTouchLong(bar, pos.TP, pos.SL) {
		case TouchTP:
			s.log.Append(Event{Ts: ts, Type: EventTakeProfitHit, Symbol: symbol})
			*pos = PositionState{Side: SideFlat}
			return
		case TouchSL:
			s.log.Append(Event{Ts: ts, Type: EventStopHit, Symbol: symbol})
			*pos = PositionState{Side: SideFlat}
			return
		}
	} else if pos.Side == SideShort {
		switch ResolveFirstTouchShort(bar, pos.TP, pos.SL) {
		case TouchTP:
			s.log.Append(Event{Ts: ts, Type: EventTakeProfitHit, Symbol: symbol})
			*pos = PositionState{Side: SideFlat}
			return
		case TouchSL:
			s.log.Append(Event{Ts: ts, Type: EventStopHit, Symbol: symbol})
			*pos = PositionState{Side: SideFlat}
			return
		}
	}
}
