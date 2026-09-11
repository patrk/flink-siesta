// Package probe answers one question per source: has anything arrived since last time?
package probe

import "context"

type ActivityProbe interface {
	Observe(ctx context.Context, sources []string) (snapshot map[string]string, ok bool)
}

// Func adapts a function to the interface. Handy in tests.
type Func func(ctx context.Context, sources []string) (map[string]string, bool)

func (f Func) Observe(ctx context.Context, s []string) (map[string]string, bool) { return f(ctx, s) }
