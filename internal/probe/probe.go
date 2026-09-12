// Package probe answers one question per source: has anything arrived since last time?
package probe

import "context"

type ActivityProbe interface {
	Observe(ctx context.Context, sources []string) (snapshot map[string]string, ok bool)
}

// Func adapts a function to the interface. Handy in tests.
type Func func(ctx context.Context, sources []string) (map[string]string, bool)

func (f Func) Observe(ctx context.Context, s []string) (map[string]string, bool) { return f(ctx, s) }

// LagProbe is optional: a source that has a notion of "consumed up to" can report how much
// is still pending for a consumer. ok=false means it could not be measured, which, like an
// unknown snapshot, must never be read as "nothing pending".
type LagProbe interface {
	Lag(ctx context.Context, group string, sources []string) (pending int64, ok bool)
}
