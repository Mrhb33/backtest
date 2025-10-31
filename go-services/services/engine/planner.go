package engine

// Run planner and orchestrator

type Chunk struct {
	Symbols []string
	StartTs uint64
	EndTs   uint64
}

type Planner struct {
	MaxChunkSize int
	MaxWorkers   int
}

func NewPlanner(maxChunkSize, maxWorkers int) *Planner {
	return &Planner{
		MaxChunkSize: maxChunkSize,
		MaxWorkers:   maxWorkers,
	}
}

func (p *Planner) PlanChunks(symbols []string, startTs, endTs uint64) []Chunk {
	var chunks []Chunk

	// Simple chunking by symbol count
	for i := 0; i < len(symbols); i += p.MaxChunkSize {
		end := i + p.MaxChunkSize
		if end > len(symbols) {
			end = len(symbols)
		}

		chunks = append(chunks, Chunk{
			Symbols: symbols[i:end],
			StartTs: startTs,
			EndTs:   endTs,
		})
	}

	return chunks
}

type Backpressure struct {
	MaxQueueSize int
	QueueLen     int
}

func (bp *Backpressure) CanAccept() bool {
	return bp.QueueLen < bp.MaxQueueSize
}

func (bp *Backpressure) Accept() {
	bp.QueueLen++
}

func (bp *Backpressure) Release() {
	if bp.QueueLen > 0 {
		bp.QueueLen--
	}
}
