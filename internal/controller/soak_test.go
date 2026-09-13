//go:build soak

package controller

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/patrk/flink-siesta/internal/probe"
	"github.com/patrk/flink-siesta/internal/state"
	"github.com/patrk/flink-siesta/internal/store"
)

// A simulated soak: many deployments, sources that come and go, a source that sometimes cannot
// be asked, an API server that drops some writes, an operator that reacts late and sometimes
// fails a job, and a controller that is crashed and recreated. Days of simulated time in
// minutes. Run: make soak
const (
	crashEvery = 400
	idleAfter  = 30 * time.Minute
)

// flakyClient drops a fraction of writes with a transient error.
type flakyClient struct {
	client.Client
	rng  *rand.Rand
	rate float64
}

func (f *flakyClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if f.rng.Float64() < f.rate {
		return errors.New("injected: transient API error")
	}
	return f.Client.Patch(ctx, obj, patch, opts...)
}

// world is the simulated environment per deployment.
type world struct {
	active    bool  // producers writing
	offset    int64 // end offset
	failAt    int   // tick at which the fake operator fails the job, 0 = never
	seenNonce int64 // last restartNonce the fake operator acted on
}

// Defaults suit a laptop (minutes); the nightly sets SOAK_DEPLOYMENTS=100 SOAK_TICKS=3000.
var (
	soakDeployments = envInt("SOAK_DEPLOYMENTS", 30)
	soakTicks       = envInt("SOAK_TICKS", 1200) // one simulated minute each
)

func envInt(name string, dflt int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return dflt
}

func TestSoak(t *testing.T) {
	ctrl.SetLogger(logr.Discard())
	c := startEnv(t)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(42)) // deterministic: a failure reproduces
	worlds := make([]*world, soakDeployments)
	keys := make([]types.NamespacedName, soakDeployments)
	for i := range soakDeployments {
		name := fmt.Sprintf("soak-%03d", i)
		fd := createDeployment(t, c, name, "savepoint")
		fd.SetAnnotations(map[string]string{"siesta.flink.io/mode": "auto", "siesta.flink.io/sources": name, "siesta.flink.io/idle-after": idleAfter.String(), "siesta.flink.io/min-awake": "5m"})
		if err := c.Update(ctx, fd); err != nil {
			t.Fatal(err)
		}
		worlds[i] = &world{active: rng.Float64() < 0.5, offset: rng.Int63n(1000)}
		keys[i] = types.NamespacedName{Namespace: "default", Name: name}
	}

	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	byName := map[string]*world{}
	for i, k := range keys {
		byName[k.Name] = worlds[i]
	}
	// The probe answers from the simulated world; 3% of calls are "could not ask".
	obs := probe.Func(func(_ context.Context, sources []string) (map[string]string, bool) {
		if rng.Float64() < 0.03 {
			return nil, false
		}
		w := byName[sources[0]]
		return map[string]string{sources[0] + "-0": fmt.Sprint(w.offset)}, true
	})
	newController := func() *Reconciler {
		fc := &flakyClient{Client: c, rng: rng, rate: 0.02}
		r := newReconciler(fc, &now, obs)
		r.Store = store.Store{Client: fc, Reader: c, Prefix: "siesta.flink.io"}
		return r
	}
	r := newController()

	var heapAt20, goroutinesAt20 uint64
	transitions := map[string]int{}
	window := soakTicks / 5
	perWindow := make([]int, 5) // suspends per fifth of the run; a falling rate means deployments are getting stranded
	for tick := 1; tick <= soakTicks; tick++ {
		now = now.Add(time.Minute)
		if tick%crashEvery == 0 {
			r = newController() // crash: the only memory is in the cluster
		}
		for i, key := range keys {
			w := worlds[i]
			// the world moves
			if rng.Float64() < 0.02 {
				w.active = !w.active
			}
			if w.active {
				w.offset += 1 + rng.Int63n(50)
			}
			if w.failAt == 0 && rng.Float64() < 0.001 {
				w.failAt = tick
			}
			fakeOperator(t, c, key, w, tick, rng)

			before := specJobState(t, c, key)
			if _, err := r.Reconcile(ctx, reconcileRequest(key)); err != nil && !isInjected(err) {
				t.Fatalf("tick %d %s: %v", tick, key.Name, err)
			}
			if after := specJobState(t, c, key); after != before {
				transitions[after]++
				if after == "suspended" {
					perWindow[min((tick-1)/window, 4)]++
				}
			}
		}
		if tick == soakTicks/5 {
			heapAt20, goroutinesAt20 = memory()
		}
		if tick%500 == 0 {
			t.Logf("tick %d: transitions so far %v", tick, transitions)
		}
	}

	// Invariant: memory and object agree, or are repairable on the next tick, for every deployment.
	for _, key := range keys {
		fd := mustGet(t, c, key)
		st, ok, err := r.Store.Load(ctx, fd)
		if err != nil || !ok {
			t.Fatalf("%s: no memory after the soak: ok=%v err=%v", key.Name, ok, err)
		}
		spec, _, _ := unstructured.NestedString(fd.Object, "spec", "job", "state")
		ann := fd.GetAnnotations()["siesta.flink.io/state"]
		if (st.Phase == state.Suspended) != (spec == "suspended") && ann != string(st.Phase) {
			t.Fatalf("%s: memory=%s spec=%s annotation=%s: inconsistent and not repairable", key.Name, st.Phase, spec, ann)
		}
	}
	heapEnd, goroutinesEnd := memory()
	t.Logf("heap %d KiB -> %d KiB, goroutines %d -> %d, transitions %v", heapAt20/1024, heapEnd/1024, goroutinesAt20, goroutinesEnd, transitions)
	if heapEnd > heapAt20+heapAt20/4 {
		t.Fatalf("heap grew more than 25%% between 20%% and the end: %d -> %d bytes", heapAt20, heapEnd)
	}
	if goroutinesEnd > goroutinesAt20+10 {
		t.Fatalf("goroutines leaked: %d -> %d", goroutinesAt20, goroutinesEnd)
	}
	if transitions["suspended"] == 0 || transitions["running"] == 0 {
		t.Fatalf("the soak must exercise both directions, got %v", transitions)
	}
	t.Logf("suspends per fifth of the run: %v", perWindow)
	if perWindow[4]*3 < perWindow[0] {
		t.Fatalf("the transition rate collapsed over the run (%v): deployments are being stranded", perWindow)
	}
}

// fakeOperator plays the Flink operator: reacts to spec changes with a random delay, fails a
// job when the world says so, recovers it when restarted.
func fakeOperator(t *testing.T, c client.Client, key types.NamespacedName, w *world, tick int, rng *rand.Rand) {
	t.Helper()
	fd := mustGet(t, c, key)
	spec, _, _ := unstructured.NestedString(fd.Object, "spec", "job", "state")
	lifecycle, _, _ := unstructured.NestedString(fd.Object, "status", "lifecycleState")
	job, _, _ := unstructured.NestedString(fd.Object, "status", "jobStatus", "state")
	nonce, _, _ := unstructured.NestedInt64(fd.Object, "spec", "restartNonce")
	set := func(l, j string) {
		_ = unstructured.SetNestedField(fd.Object, l, "status", "lifecycleState")
		_ = unstructured.SetNestedField(fd.Object, j, "status", "jobStatus", "state")
		if l == "SUSPENDED" {
			_ = unstructured.SetNestedField(fd.Object, fmt.Sprintf("file:///sp/%s-%d", key.Name, tick), "status", "jobStatus", "upgradeSavepointPath")
		}
		if err := c.Status().Update(context.Background(), fd); err != nil {
			t.Fatal(err)
		}
	}
	switch {
	case spec == "suspended" && lifecycle != "SUSPENDED":
		if rng.Float64() < 0.5 { // reacts within a couple of ticks
			set("SUSPENDED", "FINISHED")
		}
	case spec == "running" && lifecycle == "SUSPENDED":
		set("UPGRADING", "RECONCILING")
	case nonce != w.seenNonce && nonce != 0: // restart requested
		w.seenNonce = nonce
		w.failAt = 0
		set("UPGRADING", "RECONCILING")
	case lifecycle == "UPGRADING":
		if rng.Float64() < 0.6 {
			set("STABLE", "RUNNING")
		}
	case w.failAt != 0 && tick >= w.failAt && job == "RUNNING":
		set("FAILED", "FAILED")
	}
}

func isInjected(err error) bool { return err != nil && (contains(err.Error(), "injected")) }

func contains(s, sub string) bool { return len(sub) <= len(s) && (s == sub || index(s, sub) >= 0) }

func index(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func memory() (heap uint64, goroutines uint64) {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc, uint64(runtime.NumGoroutine())
}
