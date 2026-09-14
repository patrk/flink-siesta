// Package probe answers one question per source: has anything arrived since last time?
package probe

import (
	"context"
	"strconv"
	"strings"
)

// Offsets is one observation of the sources. Snapshot is opaque to the decider: compared for
// equality and persisted, never interpreted (ADR 3). Ends is the typed side of the same read,
// the end offset per partition, for the job gate (ADR 13). A missing partition is -1.
type Offsets struct {
	Snapshot map[string]string
	Ends     map[string][]int64
}

// ActivityProbe reads the sources. An error means they could not be asked; the caller treats
// that as unknown, which never counts as idle and never as input.
type ActivityProbe interface {
	Observe(ctx context.Context, sources []string) (Offsets, error)
}

// Func adapts a function that returns only the snapshot, in the Kafka probe's per-topic
// "o0,o1,..." form, to the interface. Handy in tests; the typed side is derived.
type Func func(ctx context.Context, sources []string) (map[string]string, error)

func (f Func) Observe(ctx context.Context, s []string) (Offsets, error) {
	snap, err := f(ctx, s)
	if err != nil {
		return Offsets{}, err
	}
	return Offsets{Snapshot: snap, Ends: Ends(snap)}, nil
}

// Ends reads the typed offsets back out of the Kafka probe's snapshot form. Unreadable
// values become -1, which the job gate treats as unknown.
func Ends(snapshot map[string]string) map[string][]int64 {
	out := make(map[string][]int64, len(snapshot))
	for topic, joined := range snapshot {
		for _, raw := range strings.Split(joined, ",") {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				n = -1
			}
			out[topic] = append(out[topic], n)
		}
	}
	return out
}

// LagProbe is optional: a source that has a notion of "consumed up to" can report how much
// is still pending for a consumer. An error means it could not be measured, which, like an
// unknown snapshot, must never be read as "nothing pending".
type LagProbe interface {
	Lag(ctx context.Context, group string, sources []string) (pending int64, err error)
}
