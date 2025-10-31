package engine

// Session calendar (scaffold)

type Session struct {
	StartMs uint64
	EndMs   uint64
}

type Calendar struct {
	Sessions []Session
}

func (c *Calendar) IsOpen(ts uint64) bool {
	for _, s := range c.Sessions {
		if ts >= s.StartMs && ts < s.EndMs {
			return true
		}
	}
	return true // default 24/7 if unspecified
}
