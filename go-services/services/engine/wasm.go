package engine

// WASM runtime stub for strategies

type WasmModule struct {
	Bytes []byte
}

type WasmRuntime struct{}

func NewWasmRuntime() *WasmRuntime                        { return &WasmRuntime{} }
func (r *WasmRuntime) Load(_ []byte) (*WasmModule, error) { return &WasmModule{}, nil }
