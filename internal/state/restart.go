package state

import (
	"math"
	"time"
)

type RestartBudget struct {
	Count       int       `json:"count"`
	WindowStart time.Time `json:"windowStart,omitzero"`
	NextAfter   time.Time `json:"nextAfter,omitzero"`
}

func (b RestartBudget) Allows(now time.Time, limit int, window time.Duration) bool {
	b = b.rolled(now, window)
	if b.Count >= limit {
		return false
	}
	return b.NextAfter.IsZero() || !now.Before(b.NextAfter)
}

func (b RestartBudget) Consume(now time.Time, window, base time.Duration, multiplier float64) RestartBudget {
	b = b.rolled(now, window)
	start := b.WindowStart
	if start.IsZero() {
		start = now
	}
	backoff := time.Duration(float64(base) * math.Pow(multiplier, float64(b.Count)))
	return RestartBudget{Count: b.Count + 1, WindowStart: start, NextAfter: now.Add(backoff)}
}

// rolled: a budget older than the window is a fresh budget.
func (b RestartBudget) rolled(now time.Time, window time.Duration) RestartBudget {
	if b.WindowStart.IsZero() || now.After(b.WindowStart.Add(window)) {
		return RestartBudget{}
	}
	return b
}
