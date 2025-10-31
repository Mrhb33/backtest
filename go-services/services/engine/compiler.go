package engine

// Strategy source kinds accepted by the system
type StrategySourceKind int

const (
	SourceDSL StrategySourceKind = iota
	SourceTypeScript
	SourceRust
)

// StrategySource is the user-provided strategy artifact
type StrategySource struct {
	Kind    StrategySourceKind
	Content []byte
}

// CompiledArtifact is the result of compiling a strategy
type CompiledArtifact struct {
	IR       Program
	Wasm     []byte
	Manifest *RunManifest
}

// CompilePipeline performs parse->typecheck->IR->WASM emission and validation (stub)
func CompilePipeline(src StrategySource) (*CompiledArtifact, error) {
	// Parse + typecheck would go here (omitted)

	// Build minimal IR program placeholder
	ir := Program{Nodes: []Node{}, WarmupBars: 0, RequiresOnTick: false}

	// Emit WASM (stub)
	wasm := []byte{} // Placeholder

	// Build manifest (partial)
	manifest := &RunManifest{
		EngineVersion:  "1.0.0",
		IntrabarPolicy: string(IntrabarPolicyExactTrades),
		FpFlags:        "nearest-even",
	}

	return &CompiledArtifact{IR: ir, Wasm: wasm, Manifest: manifest}, nil
}
