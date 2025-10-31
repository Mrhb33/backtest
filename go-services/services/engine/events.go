package engine

type EventType int

const (
	EventOrderSubmit EventType = iota
	EventOrderFill
	EventStopHit
	EventTakeProfitHit
	EventPositionUpdate
	EventEquityPoint
)

type Event struct {
	Ts      uint64
	Type    EventType
	Symbol  string
	Details map[string]string
}

type EventLog struct {
	Events []Event
}

func (l *EventLog) Append(e Event) { l.Events = append(l.Events, e) }
