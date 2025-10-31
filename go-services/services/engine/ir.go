package engine

// ValueType enumerates IR value types
type ValueType int

const (
	ValueVoid ValueType = iota
	ValueBool
	ValueI64
	ValueF64
	ValueDecimal128
	ValueTimestampMs
	ValueSeriesF64
	ValueSeriesBool
)

// OpCode represents an IR operation code
type OpCode int

const (
	OpConst OpCode = iota
	OpLoadParam
	OpLoadSeries
	OpMath
	OpCompare
	OpLogical
	OpIndicator
	OpEnterLong
	OpEnterShort
	OpExit
	OpUpdateOrders
	OpAdjustSLTP
	OpTrailStop
	OpIf
	OpSequence
)

// Node is a single IR node
type Node struct {
	Op       OpCode
	Type     ValueType
	Inputs   []int // references to nodes in the same Program.Nodes
	IntArg   int
	FloatArg float64
	StrArg   string
}

// Program is a full IR program
type Program struct {
	Nodes          []Node
	WarmupBars     int
	RequiresOnTick bool
}
