package controller

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/patrk/flink-siesta/internal/probe"
	"github.com/patrk/flink-siesta/internal/store"
)

// countingClient counts API requests by verb. Timing benchmarks are noisy; request counts are
// exact, and one extra read per tick is what a scaling regression looks like.
type countingClient struct {
	client.Client
	gets, writes int
}

func (c *countingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.gets++
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *countingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	c.writes++
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *countingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	c.writes++
	return c.Client.Create(ctx, obj, opts...)
}

func (c *countingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	c.writes++
	return c.Client.Update(ctx, obj, opts...)
}

// A tick on which nothing changed must cost exactly: one read of the deployment, one read of
// its ConfigMap on load, one probe call, and no writes at all. The save compares against what
// the load read.
func TestQuietTickCostsTwoReadsOneProbeAndNoWrites(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	createDeployment(t, c, "quiet", "savepoint")
	cc := &countingClient{Client: c}
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	probes := 0
	r := newReconciler(cc, &now, probe.Func(func(context.Context, []string) (map[string]string, error) {
		probes++
		return map[string]string{"in-0": "5"}, nil
	}))
	r.Store = store.Store{Client: cc, Reader: cc, Prefix: "siesta.flink.io", Seen: &store.Seen{}}
	req := reconcileRequest(types.NamespacedName{Name: "quiet", Namespace: "default"})

	if _, err := r.Reconcile(ctx, req); err != nil { // first observation: writes are expected
		t.Fatal(err)
	}
	cc.gets, cc.writes, probes = 0, 0, 0
	now = now.Add(time.Minute)
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if cc.writes != 0 {
		t.Fatalf("a quiet tick must not write, got %d writes", cc.writes)
	}
	if cc.gets != 2 {
		t.Fatalf("a quiet tick should cost 2 reads (deployment, ConfigMap load), got %d", cc.gets)
	}
	if probes != 1 {
		t.Fatalf("a quiet tick should ask the source once, got %d", probes)
	}
}

// Several workers reconcile different deployments at once (--max-concurrent-reconciles). They
// share the reconciler's memo, the store's Seen and the probes; run under -race this is what
// shows a shared map written without its lock.
func TestConcurrentReconcilesOfDifferentDeployments(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	r := newReconciler(c, &now, fixedOffsets(map[string]string{"in": "5"}))
	r.Store.Seen = &store.Seen{}
	keys := make([]types.NamespacedName, 0, 8)
	for i := range 8 {
		name := fmt.Sprintf("parallel-%d", i)
		createDeployment(t, c, name, "savepoint")
		keys = append(keys, types.NamespacedName{Name: name, Namespace: "default"})
	}
	tick := func() {
		var wg sync.WaitGroup
		errs := make(chan error, len(keys))
		for _, k := range keys {
			wg.Go(func() {
				if _, err := r.Reconcile(ctx, reconcileRequest(k)); err != nil {
					errs <- err
				}
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
	}
	tick()
	now = now.Add(2 * time.Hour) // past idle-after
	tick()
	for _, k := range keys {
		if s := specJobState(t, c, k); s != "suspended" {
			t.Fatalf("%s: every idle deployment must be suspended, got %q", k.Name, s)
		}
	}
}
