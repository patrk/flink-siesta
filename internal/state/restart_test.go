package state

import (
	"testing"
	"time"
)

func TestRestartBudget(t *testing.T) {
	t0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	window, base := 30*time.Minute, time.Minute
	var b RestartBudget
	now := t0
	for i := 1; i <= 3; i++ {
		if !b.Allows(now, 3, window) {
			t.Fatalf("restart %d should be allowed", i)
		}
		b = b.Consume(now, window, base, 2)
		now = b.NextAfter
	}
	if b.Allows(now.Add(time.Second), 3, window) {
		t.Fatal("4th restart inside the window must be refused")
	}
	if !b.Allows(t0.Add(window+time.Second), 3, window) {
		t.Fatal("budget must reset after the window")
	}
	// backoff doubles: 1m, 2m, 4m
	if got := b.NextAfter.Sub(t0); got != 1*time.Minute+2*time.Minute+4*time.Minute {
		t.Fatalf("unexpected cumulative backoff %v", got)
	}
}
