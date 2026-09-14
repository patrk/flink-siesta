package decide

import (
	"fmt"
	"time"
)

// jobGate turns the job's reading into its answer (ADR 13). Pending is exact: the broker's end
// offset from this tick against the last offset the reader emitted, per declared partition. A
// reader still at its initial offset is caught up only if it also reports idle, which is a
// reader that had nothing to read from where it started. Quiet means no record emitted for at
// least one poll interval, so a job still working through a fetched backlog objects.
func jobGate(sources []string, ends map[string][]int64, reading JobReading, quiet time.Duration) (gate JobGate, note string) {
	var pending int64
	for _, topic := range sources {
		cur := reading.Offsets[topic]
		if len(ends[topic]) == 0 || cur == nil {
			return JobUnknown, "job exposes no offsets for " + topic
		}
		for p, end := range ends[topic] {
			if end < 0 {
				return JobUnknown, fmt.Sprintf("no end offset for %s partition %d", topic, p)
			}
			emitted, has := cur[p]
			switch {
			case !has:
				return JobUnknown, fmt.Sprintf("job exposes no offset for %s partition %d", topic, p)
			case emitted == InitialOffset && reading.IdleFor == 0:
				pending += end // nothing emitted yet and not idle: the reader is about to read
			case emitted == InitialOffset:
				// idle without ever emitting: nothing to read from where it started
			default:
				pending += max(0, end-(emitted+1))
			}
		}
	}
	switch {
	case pending > 0:
		return JobBusy, fmt.Sprintf("%d records pending in the job", pending)
	case reading.IdleFor < quiet:
		return JobBusy, "job emitted a record " + reading.IdleFor.Round(time.Second).String() + " ago"
	default:
		return JobIdle, ""
	}
}
