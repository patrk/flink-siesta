package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/patrk/flink-siesta/internal/flink"
	"github.com/patrk/flink-siesta/internal/probe"
	"github.com/patrk/flink-siesta/internal/state"
)

// fakeJob plays the JobManager REST API: fixed topics and a pendingRecords value, with call counts.
type fakeJob struct {
	topics   []string
	pending  int64
	err      error
	sources  int
	pendings int
}

func (f *fakeJob) Sources(context.Context, string, string, string) (flink.Sources, error) {
	f.sources++
	if f.err != nil {
		return flink.Sources{}, f.err
	}
	return flink.Sources{Topics: f.topics, Pending: map[string][]string{"v": {"pendingRecords"}}}, nil
}

func (f *fakeJob) Pending(context.Context, string, string, string, flink.Sources) (int64, error) {
	f.pendings++
	return f.pending, f.err
}

// fakeRecorder keeps event reasons in order.
type fakeRecorder struct{ reasons []string }

func (r *fakeRecorder) Eventf(_, _ runtime.Object, _, reason, _, _ string, _ ...any) {
	r.reasons = append(r.reasons, reason)
}

func (r *fakeRecorder) AnnotatedEventf(_, _ runtime.Object, _ map[string]string, _, reason, _, _ string, _ ...any) {
	r.reasons = append(r.reasons, reason)
}

func (r *fakeRecorder) count(reason string) int {
	n := 0
	for _, s := range r.reasons {
		if s == reason {
			n++
		}
	}
	return n
}

func setJobID(t *testing.T, r *Reconciler, fd *unstructured.Unstructured, id string) {
	t.Helper()
	ctx := context.Background()
	if err := r.Get(ctx, types.NamespacedName{Name: fd.GetName(), Namespace: fd.GetNamespace()}, fd); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(fd.Object, id, "status", "jobStatus", "jobId"); err != nil {
		t.Fatal(err)
	}
	if err := r.Status().Update(ctx, fd); err != nil {
		t.Fatal(err)
	}
}

func TestSourcesAreVerifiedOncePerJobInstanceAndLagFromTheJobGatesSuspend(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	fd := createDeployment(t, c, "job", "savepoint")
	ann := fd.GetAnnotations()
	ann["siesta.flink.io/lag"] = "job"
	fd.SetAnnotations(ann)
	if err := c.Update(ctx, fd); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	offsets := map[string]string{"in-0": "5"}
	r := newReconciler(c, &now, probe.Func(func(context.Context, []string) (map[string]string, bool) { return offsets, true }))
	job := &fakeJob{topics: []string{"other"}, pending: 3}
	rec := &fakeRecorder{}
	r.Flink, r.Recorder = job, rec
	setJobID(t, r, fd, "j1")
	key := types.NamespacedName{Name: "job", Namespace: "default"}
	reconcile := func() {
		t.Helper()
		if _, err := r.Reconcile(ctx, reconcileRequest(key)); err != nil {
			t.Fatal(err)
		}
	}

	reconcile()
	reconcile()
	if rec.count("SourcesDrift") != 1 || job.sources != 1 {
		t.Fatalf("drift must be reported once and the graph read once per job: events=%v reads=%d", rec.reasons, job.sources)
	}
	st, _, err := r.Store.Load(ctx, fd)
	if err != nil || st.SourcesChecked != "j1" {
		t.Fatalf("memory must record the checked job id: %+v %v", st, err)
	}

	now = now.Add(2 * time.Hour) // idle and awake long enough
	reconcile()
	if s := specJobState(t, c, key); s != "running" {
		t.Fatal("pending records in the job must block the suspend")
	}
	if st, _, _ = r.Store.Load(ctx, fd); !strings.Contains(st.Reason, "records pending in the job") {
		t.Fatalf("reason = %q", st.Reason)
	}

	job.err = errors.New("connection refused")
	reconcile()
	if s := specJobState(t, c, key); s != "running" {
		t.Fatal("an unreachable REST API is unknown lag, which never suspends")
	}
	if st, _, _ = r.Store.Load(ctx, fd); !strings.Contains(st.Reason, "lag unknown in the job") {
		t.Fatalf("reason = %q", st.Reason)
	}

	job.err, job.pending = nil, 0
	reconcile()
	if s := specJobState(t, c, key); s != "suspended" {
		t.Fatal("caught up in the job must allow the suspend")
	}

	// The job is restarted by someone: a new job id with matching sources is verified afresh.
	job.topics = []string{"in"}
	setJobID(t, r, fd, "j2")
	reconcile()
	if rec.count("SourcesVerified") != 1 || job.sources != 2 {
		t.Fatalf("a new job instance must be checked again: events=%v reads=%d", rec.reasons, job.sources)
	}
	if st, _, _ = r.Store.Load(ctx, fd); st.Phase != state.Suspended {
		t.Fatalf("verification must not touch the phase, got %s", st.Phase)
	}
}
