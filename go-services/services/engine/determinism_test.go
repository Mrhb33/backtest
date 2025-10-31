package engine

import "testing"

func TestResolveFirstTouchLong(t *testing.T) {
	bar := Bar{Open: 100, High: 110, Low: 90, Close: 105}
	if ResolveFirstTouchLong(bar, 108, 95) != TouchTP {
		t.Fatal("expected TP first")
	}
}

func TestResolveFirstTouchShort(t *testing.T) {
	bar := Bar{Open: 100, High: 110, Low: 90, Close: 95}
	if ResolveFirstTouchShort(bar, 92, 105) != TouchTP { // tp below, sl above
		t.Fatal("expected TP first for short")
	}
}
