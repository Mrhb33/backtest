package engine

// Golden parity suite scaffolding

type ParityTestCase struct {
	Name     string
	Bars     []Bar
	TP       float64
	SL       float64
	Expected FirstTouchResult
}

var GoldenCases = []ParityTestCase{
	{
		Name:     "TP_first_long",
		Bars:     []Bar{{Open: 100, High: 110, Low: 90, Close: 105}},
		TP:       108,
		SL:       95,
		Expected: TouchTP,
	},
	{
		Name:     "SL_first_short",
		Bars:     []Bar{{Open: 100, High: 110, Low: 90, Close: 95}},
		TP:       92,
		SL:       105,
		Expected: TouchSL,
	},
}

func RunParitySuite() []string {
	var failures []string
	for _, tc := range GoldenCases {
		result := ResolveFirstTouchLong(tc.Bars[0], tc.TP, tc.SL)
		if result != tc.Expected {
			failures = append(failures, tc.Name)
		}
	}
	return failures
}
