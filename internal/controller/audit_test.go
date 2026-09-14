package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/patrk/flink-siesta/internal/flink"
	"github.com/patrk/flink-siesta/internal/metrics"
	"github.com/patrk/flink-siesta/internal/probe"
	"github.com/patrk/flink-siesta/internal/state"
)

// The tests in this file were written from a traceability audit of the README against the
// suite: each one pins a promise that had no test at any level.

var t0 = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

func TestRepairAdoptsAnUnrecoverableMarkTheObjectCarries(t *testing.T) {
	prev := state.Initial(t0)
	got := repair(prev, "unrecoverable", "restart budget exhausted", t0)
	if got.Phase != state.Unrecoverable || got.Reason != "restart budget exhausted" {
		t.Fatalf("got %+v", got)
	}
}

func fixedOffsets(m map[string]string) probe.ActivityProbe {
	return probe.Func(func(context.Context, []string) (map[string]string, error) { return m, nil })
}

func setStatus(t *testing.T, c client.Client, fd *unstructured.Unstructured, fields map[string]any) {
	t.Helper()
	ctx := context.Background()
	if err := c.Get(ctx, client.ObjectKeyFromObject(fd), fd); err != nil {
		t.Fatal(err)
	}
	for k, v := range fields {
		if err := unstructured.SetNestedField(fd.Object, v, "status", k); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Status().Update(ctx, fd); err != nil {
		t.Fatal(err)
	}
}

// A stateless job that is idle is refused once, not once a minute: one event, one transition.
func TestARefusalIsReportedOnceNotPerTick(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	createDeployment(t, c, "refused", "stateless")
	now := t0
	r := newReconciler(c, &now, fixedOffsets(map[string]string{"in": "5"}))
	rec := &fakeRecorder{}
	r.Recorder = rec
	key := types.NamespacedName{Name: "refused", Namespace: "default"}
	before := testutil.ToFloat64(metrics.Transitions.WithLabelValues("default", "refused", "refuse"))
	for i := 0; i < 4; i++ {
		if _, err := r.Reconcile(ctx, reconcileRequest(key)); err != nil {
			t.Fatal(err)
		}
		now = now.Add(2 * time.Hour)
	}
	if rec.count("Refused") != 1 {
		t.Fatalf("Refused must be raised once, got %v", rec.reasons)
	}
	if got := testutil.ToFloat64(metrics.Transitions.WithLabelValues("default", "refused", "refuse")) - before; got != 1 {
		t.Fatalf("one refusal is one transition, got %v", got)
	}
	if s := specJobState(t, c, key); s != "running" {
		t.Fatal("a refused job is never suspended")
	}
}

// Removing the policy releases the deployment: our annotations and the ConfigMap go with it.
func TestRemovingThePolicyClearsStateAndMemory(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	fd := createDeployment(t, c, "released", "savepoint")
	now := t0
	r := newReconciler(c, &now, fixedOffsets(map[string]string{"in": "5"}))
	rec := &fakeRecorder{}
	r.Recorder = rec
	key := types.NamespacedName{Name: "released", Namespace: "default"}
	if _, err := r.Reconcile(ctx, reconcileRequest(key)); err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, types.NamespacedName{Name: "siesta-released", Namespace: "default"}, cm); err != nil {
		t.Fatalf("memory must exist after the first tick: %v", err)
	}
	if err := c.Get(ctx, key, fd); err != nil {
		t.Fatal(err)
	}
	ann := fd.GetAnnotations()
	delete(ann, "siesta.flink.io/mode")
	fd.SetAnnotations(ann)
	if err := c.Update(ctx, fd); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, reconcileRequest(key)); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, key, fd); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"siesta.flink.io/state", "siesta.flink.io/reason"} {
		if _, has := fd.GetAnnotations()[k]; has {
			t.Fatalf("%s must be removed when the policy goes", k)
		}
	}
	if err := c.Get(ctx, types.NamespacedName{Name: "siesta-released", Namespace: "default"}, cm); !apierrors.IsNotFound(err) {
		t.Fatalf("memory must be deleted when the policy goes, got %v", err)
	}
	if rec.count("Released") != 1 {
		t.Fatalf("events = %v", rec.reasons)
	}
}

// The four once-only events: raised on the first tick their situation holds, never repeated.
func TestOnceOnlyEvents(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	fd := createDeployment(t, c, "once", "savepoint")
	now := t0
	offsets := map[string]string{"in": "5"}
	r := newReconciler(c, &now, fixedOffsets(offsets))
	r.ResumeStallAfter = 10 * time.Minute
	rec := &fakeRecorder{}
	r.Recorder = rec
	key := types.NamespacedName{Name: "once", Namespace: "default"}
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
	tick(1) // suspends
	if s := specJobState(t, c, key); s != "suspended" {
		t.Fatal("setup: expected a suspend")
	}
	// The operator never completes it: SuspendStalled once.
	now = now.Add(11 * time.Minute)
	tick(3)
	if rec.count("SuspendStalled") != 1 {
		t.Fatalf("SuspendStalled once, got %v", rec.reasons)
	}
	// Then it completes without a savepoint: SuspendedWithoutSavepoint once.
	setStatus(t, c, fd, map[string]any{"lifecycleState": "SUSPENDED"})
	tick(3)
	if rec.count("SuspendedWithoutSavepoint") != 1 {
		t.Fatalf("SuspendedWithoutSavepoint once, got %v", rec.reasons)
	}
	// Input resumes it, but the job never reports RUNNING again: ResumeStalled once.
	offsets["in"] = "6"
	r.Probe = fixedOffsets(offsets)
	now = now.Add(time.Minute)
	tick(1)
	if s := specJobState(t, c, key); s != "running" {
		t.Fatal("setup: expected a resume")
	}
	setStatus(t, c, fd, map[string]any{"lifecycleState": "DEPLOYED"})
	now = now.Add(11 * time.Minute)
	tick(3)
	if rec.count("ResumeStalled") != 1 {
		t.Fatalf("ResumeStalled once, got %v", rec.reasons)
	}
}

// An invalid policy is reported once and the job is left untouched until it is fixed.
func TestInvalidPolicyIsReportedOnceAndTouchesNothing(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	fd := createDeployment(t, c, "invalid", "savepoint")
	ann := fd.GetAnnotations()
	ann["siesta.flink.io/idle-after"] = "two weeks"
	fd.SetAnnotations(ann)
	if err := c.Update(ctx, fd); err != nil {
		t.Fatal(err)
	}
	now := t0
	r := newReconciler(c, &now, fixedOffsets(map[string]string{"in": "5"}))
	rec := &fakeRecorder{}
	r.Recorder = rec
	key := types.NamespacedName{Name: "invalid", Namespace: "default"}
	for i := 0; i < 3; i++ {
		if _, err := r.Reconcile(ctx, reconcileRequest(key)); err != nil {
			t.Fatal(err)
		}
		now = now.Add(48 * time.Hour)
	}
	if rec.count("InvalidPolicy") != 1 {
		t.Fatalf("InvalidPolicy once, got %v", rec.reasons)
	}
	if s := specJobState(t, c, key); s != "running" {
		t.Fatal("an invalid policy must never lead to a suspend")
	}
	if err := c.Get(ctx, key, fd); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(fd.GetAnnotations()["siesta.flink.io/reason"], "invalid policy:") {
		t.Fatalf("the reason on the object must say why: %q", fd.GetAnnotations()["siesta.flink.io/reason"])
	}
}

// A suspend changes job.state and our two annotations, under the siesta field manager, and
// nothing else: the upgrade mode in particular (ADR 5, ADR 6).
func TestASuspendChangesOnlyWhatWeOwnUnderOurManager(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	fd := createDeployment(t, c, "owned", "last-state")
	now := t0
	r := newReconciler(c, &now, fixedOffsets(map[string]string{"in": "5"}))
	key := types.NamespacedName{Name: "owned", Namespace: "default"}
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, reconcileRequest(key)); err != nil {
			t.Fatal(err)
		}
		now = now.Add(2 * time.Hour)
	}
	if err := c.Get(ctx, key, fd); err != nil {
		t.Fatal(err)
	}
	if s, _, _ := unstructured.NestedString(fd.Object, "spec", "job", "state"); s != "suspended" {
		t.Fatal("setup: expected a suspend")
	}
	if um, _, _ := unstructured.NestedString(fd.Object, "spec", "job", "upgradeMode"); um != "last-state" {
		t.Fatalf("upgradeMode must never change, got %q", um)
	}
	managers := map[string]bool{}
	for _, m := range fd.GetManagedFields() {
		managers[m.Manager] = true
	}
	if !managers["siesta"] {
		t.Fatalf("our writes must carry the siesta field manager, got %v", managers)
	}
}

// conflictingClient changes the object behind the controller's back right before the first
// patch that touches the spec, which is what an edit between our read and our write looks like.
// edit says what changes.
type conflictingClient struct {
	client.Client
	base  client.Client
	edit  func(*unstructured.Unstructured)
	fired bool
}

func (c *conflictingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	data, _ := patch.Data(obj)
	if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "FlinkDeployment" && !c.fired && strings.Contains(string(data), `"spec"`) {
		c.fired = true
		fresh := flink.New()
		if err := c.base.Get(ctx, client.ObjectKeyFromObject(u), fresh); err != nil {
			return err
		}
		c.edit(fresh)
		if err := c.base.Update(ctx, fresh); err != nil {
			return err
		}
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

// Every patch is conditional on the resourceVersion that was read. An unrelated concurrent
// change, the operator's status writes being the common one, is retried once from a fresh
// read. A concurrent change to spec.job.state itself is never overwritten.
func TestAPatchIsConditionalOnWhatWasRead(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	race := func(name string, edit func(*unstructured.Unstructured)) (*Reconciler, types.NamespacedName, error) {
		createDeployment(t, c, name, "savepoint")
		now := t0.Add(2 * time.Hour)
		cc := &conflictingClient{Client: c, base: c, edit: edit}
		r := newReconciler(cc, &now, fixedOffsets(map[string]string{"in": "5"}))
		r.Store.Reader = c
		key := types.NamespacedName{Name: name, Namespace: "default"}
		if _, err := r.Reconcile(ctx, reconcileRequest(key)); err != nil { // first observation, no write of ours races
			t.Fatal(err)
		}
		now = now.Add(2 * time.Hour)
		_, err := r.Reconcile(ctx, reconcileRequest(key)) // the suspend, raced by edit
		return r, key, err
	}

	_, key, err := race("raced-annotation", func(u *unstructured.Unstructured) {
		ann := u.GetAnnotations()
		ann["human"] = "note"
		u.SetAnnotations(ann)
	})
	if err != nil {
		t.Fatalf("an unrelated edit must be retried through, got %v", err)
	}
	if s := specJobState(t, c, key); s != "suspended" {
		t.Fatal("the suspend must land after the retry")
	}

	_, _, err = race("raced-owned", func(u *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(u.Object, "suspended", "spec", "job", "state")
	})
	if err == nil || !strings.Contains(err.Error(), "spec.job.state changed") {
		t.Fatalf("a concurrent change of the field we own must not be overwritten, got %v", err)
	}
}

// The savings and inventory gauges follow the phase.
func TestSavingsMetricsFollowThePhase(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	fd := createDeployment(t, c, "saving", "savepoint")
	if err := unstructured.SetNestedMap(fd.Object, map[string]any{"resource": map[string]any{"cpu": 0.5, "memory": "1024m"}}, "spec", "jobManager"); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedMap(fd.Object, map[string]any{"resource": map[string]any{"cpu": 1.5, "memory": "2g"}}, "spec", "taskManager"); err != nil {
		t.Fatal(err)
	}
	if err := c.Update(ctx, fd); err != nil {
		t.Fatal(err)
	}
	now := t0
	r := newReconciler(c, &now, fixedOffsets(map[string]string{"in": "5"}))
	key := types.NamespacedName{Name: "saving", Namespace: "default"}
	released := metrics.ReleasedCPU.WithLabelValues("default", "saving")
	slept := metrics.SuspendedSeconds.WithLabelValues("default", "saving")
	if _, err := r.Reconcile(ctx, reconcileRequest(key)); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(released); got != 0 {
		t.Fatalf("nothing is released while awake, got %v", got)
	}
	now = now.Add(2 * time.Hour)
	if _, err := r.Reconcile(ctx, reconcileRequest(key)); err != nil { // suspends
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(released); got != 2 { // one JobManager at 0.5, one TaskManager at 1.5
		t.Fatalf("released cores = %v, want 2", got)
	}
	now = now.Add(90 * time.Second)
	if _, err := r.Reconcile(ctx, reconcileRequest(key)); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(slept); got != 90 {
		t.Fatalf("suspended seconds = %v, want 90", got)
	}
	if got := testutil.ToFloat64(metrics.State.WithLabelValues("default", "saving", "suspended")); got != 1 {
		t.Fatalf("state gauge = %v", got)
	}
}

// sources: auto learns the topics from the running job, remembers them, and uses the memory as
// the wake signal while the job sleeps (ADR 14). Without a job to learn from, nothing acts.
func TestSourcesAutoLearnsFromTheJobAndWakesFromMemory(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	fd := createDeployment(t, c, "auto", "savepoint")
	ann := fd.GetAnnotations()
	ann["siesta.flink.io/sources"] = "auto"
	fd.SetAnnotations(ann)
	if err := c.Update(ctx, fd); err != nil {
		t.Fatal(err)
	}
	now := t0
	var asked []string
	offsets := map[string]string{"orders": "5"}
	r := newReconciler(c, &now, probe.Func(func(_ context.Context, sources []string) (map[string]string, error) {
		asked = append(asked, sources...)
		return offsets, nil
	}))
	job := &fakeJob{topics: []string{"orders"}}
	rec := &fakeRecorder{}
	r.Flink, r.Recorder = job, rec
	key := types.NamespacedName{Name: "auto", Namespace: "default"}
	tick := func() {
		t.Helper()
		if _, err := r.Reconcile(ctx, reconcileRequest(key)); err != nil {
			t.Fatal(err)
		}
	}

	// No job id yet: nothing to learn from, nothing asked, and the reason says so.
	tick()
	if len(asked) != 0 {
		t.Fatalf("nothing may be asked before the sources are known, asked %v", asked)
	}
	st, _, _ := r.Store.Load(ctx, fd)
	if !strings.Contains(st.Reason, "waiting for the job to run once") {
		t.Fatalf("reason = %q", st.Reason)
	}

	setJobID(t, r, fd, "j1")
	tick()
	if rec.count("SourcesLearned") != 1 {
		t.Fatalf("events = %v", rec.reasons)
	}
	if st, _, _ = r.Store.Load(ctx, fd); strings.Join(st.Sources, ",") != "orders" {
		t.Fatalf("memory must hold the learned sources, got %v", st.Sources)
	}
	if len(asked) == 0 || asked[len(asked)-1] != "orders" {
		t.Fatalf("the broker must be asked about the learned topics, asked %v", asked)
	}

	// Idle on the learned topic: suspend. New input on it: resume, from memory, no job to ask.
	now = now.Add(2 * time.Hour)
	tick()
	if s := specJobState(t, c, key); s != "suspended" {
		t.Fatal("expected a suspend on the learned sources")
	}
	setStatus(t, c, fd, map[string]any{"lifecycleState": "SUSPENDED"})
	job.err = errors.New("no JobManager while suspended")
	offsets["orders"] = "6"
	now = now.Add(time.Minute)
	tick()
	if s := specJobState(t, c, key); s != "running" {
		t.Fatal("input on a remembered source must resume the job")
	}
}

// sources: auto needs the job's REST API; with it switched off the policy is invalid.
func TestSourcesAutoWithoutTheRESTAPIIsInvalid(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	fd := createDeployment(t, c, "auto-norest", "savepoint")
	ann := fd.GetAnnotations()
	ann["siesta.flink.io/sources"] = "auto"
	fd.SetAnnotations(ann)
	if err := c.Update(ctx, fd); err != nil {
		t.Fatal(err)
	}
	now := t0
	r := newReconciler(c, &now, fixedOffsets(map[string]string{"orders": "5"}))
	rec := &fakeRecorder{}
	r.Recorder = rec
	key := types.NamespacedName{Name: "auto-norest", Namespace: "default"}
	if _, err := r.Reconcile(ctx, reconcileRequest(key)); err != nil {
		t.Fatal(err)
	}
	if rec.count("InvalidPolicy") != 1 {
		t.Fatalf("events = %v", rec.reasons)
	}
}
