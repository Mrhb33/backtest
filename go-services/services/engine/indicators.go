package engine

// Minimal indicator DAG scaffold

type IndicatorNode struct {
	Name   string
	Inputs []int
	Warmup int
}

type IndicatorDAG struct {
	Nodes []IndicatorNode
}

func (d *IndicatorDAG) WarmupBars() int {
	max := 0
	for _, n := range d.Nodes {
		if n.Warmup > max {
			max = n.Warmup
		}
	}
	return max
}
