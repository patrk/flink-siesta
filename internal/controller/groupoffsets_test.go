package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/patrk/flink-siesta/internal/probe"
)

// lagFunc adapts a function to probe.LagProbe.
type lagFunc func() (int64, error)

func (f lagFunc) Lag(context.Context, string, []string) (int64, error) { return f() }

// ADR 15 end to end on the API server: a stateless job resuming from group offsets sleeps when
// caught up, and a resume whose group lost its offsets is held with one ResumeHeld event until
// the offsets are back, then happens on the input that asked for it.
func TestStatelessResumeFromGroupOffsets(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	fd := createDeployment(t, c, "grouped", "stateless")
	ann := fd.GetAnnotations()
	ann["siesta.flink.io/consumer-group"] = "g"
	ann["siesta.flink.io/resume-from"] = "group-offsets"
	fd.SetAnnotations(ann)
	if err := c.Update(ctx, fd); err != nil {
		t.Fatal(err)
	}
	now := t0
	offsets := map[string]string{"in": "5"}
	r := newReconciler(c, &now, fixedOffsets(offsets))
	var lagErr error
	r.Lag = lagFunc(func() (int64, error) { return 0, lagErr })
	rec := &fakeRecorder{}
	r.Recorder = rec
	key := types.NamespacedName{Name: "grouped", Namespace: "default"}
	tick := func(n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if _, err := r.Reconcile(ctx, reconcileRequest(key)); err != nil {
				t.Fatal(err)
			}
		}
	}

	tick(1)
	now = now.Add(2 * time.Hour)
	tick(1)
	if s := specJobState(t, c, key); s != "suspended" {
		t.Fatalf("a caught-up stateless job resuming from group offsets must sleep, events %v", rec.reasons)
	}
	setStatus(t, c, fd, map[string]any{"lifecycleState": "SUSPENDED"})
	tick(1)
	if rec.count("SuspendedWithoutSavepoint") != 0 {
		t.Fatalf("a job that resumes from the group needs no savepoint, events %v", rec.reasons)
	}

	// Input arrives, but the group's offsets are gone.
	offsets["in"] = "6"
	r.Probe = fixedOffsets(offsets)
	lagErr = fmt.Errorf("group g has %w for in partition 0", probe.ErrNoCommit)
	now = now.Add(time.Minute)
	tick(3)
	if s := specJobState(t, c, key); s != "suspended" {
		t.Fatal("a resume without committed offsets must be held")
	}
	if rec.count("ResumeHeld") != 1 {
		t.Fatalf("ResumeHeld once, got %v", rec.reasons)
	}

	// The offsets are restored: the held input resumes the job without a new record.
	lagErr = nil
	now = now.Add(time.Minute)
	tick(1)
	if s := specJobState(t, c, key); s != "running" {
		t.Fatalf("restored offsets must resume on the held input, events %v", rec.reasons)
	}
}
