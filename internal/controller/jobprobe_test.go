package controller

import (
	"context"
	"errors"
	"fmt"
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

// fakeJob plays the JobManager REST API: fixed topics and one reading, with call counts.
type fakeJob struct {
	topics  []string
	reading flink.Reading
	err     error
	sources int
	reads   int
}

func (f *fakeJob) Sources(context.Context, string, string, string) (flink.Sources, error) {
	f.sources++
	if f.err != nil {
		return flink.Sources{}, f.err
	}
	return flink.Sources{Topics: f.topics, Vertices: []string{"v"}}, nil
}

func (f *fakeJob) Read(context.Context, string, string, string, flink.Sources) (flink.Reading, error) {
	f.reads++
	return f.reading, f.err
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

func TestSourcesAreVerifiedOncePerJobInstanceAndTheJobGateHoldsTheSuspend(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	fd := createDeployment(t, c, "job", "savepoint")
	ann := fd.GetAnnotations()
	ann["siesta.flink.io/idle"] = "job"
	fd.SetAnnotations(ann)
	if err := c.Update(ctx, fd); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	offsets := map[string]string{"in": "5"} // the broker: partition 0 ends at 5
	r := newReconciler(c, &now, probe.Func(func(context.Context, []string) (map[string]string, error) { return offsets, nil }))
	// The job emitted up to offset 1, so three records are still pending in it.
	job := &fakeJob{topics: []string{"other"}, reading: flink.Reading{Offsets: map[string]map[int]int64{"in": {0: 1}}, IdleFor: time.Hour}}
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
	now = now.Add(time.Minute) // the same topics, read sourceSettle apart, are the job's answer
	reconcile()
	reconcile()
	if rec.count("SourcesDrift") != 1 || job.sources != 2 {
		t.Fatalf("drift must be reported once and the graph read twice per job: events=%v reads=%d", rec.reasons, job.sources)
	}
	st, _, err := r.Store.Load(ctx, fd)
	if err != nil || st.Reported.SourcesChecked != "j1" {
		t.Fatalf("memory must record the checked job id: %+v %v", st, err)
	}

	now = now.Add(2 * time.Hour) // idle and awake long enough
	reconcile()
	if s := specJobState(t, c, key); s != "running" {
		t.Fatal("pending records in the job must block the suspend")
	}
	if st, _, _ = r.Store.Load(ctx, fd); st.Reason != "job busy: 3 records pending in the job" {
		t.Fatalf("reason = %q", st.Reason)
	}

	job.err = errors.New("connection refused")
	reconcile()
	if s := specJobState(t, c, key); s != "running" {
		t.Fatal("an unreachable REST API is an unknown gate, which never suspends")
	}
	if st, _, _ = r.Store.Load(ctx, fd); !strings.HasPrefix(st.Reason, "job unknown: ") {
		t.Fatalf("reason = %q", st.Reason)
	}

	job.err = nil
	job.reading = flink.Reading{Offsets: map[string]map[int]int64{"in": {0: 4}}, IdleFor: time.Second}
	reconcile()
	if s := specJobState(t, c, key); s != "running" {
		t.Fatal("a record emitted within the poll interval must hold the suspend")
	}
	if st, _, _ = r.Store.Load(ctx, fd); !strings.HasPrefix(st.Reason, "job busy: job emitted a record") {
		t.Fatalf("reason = %q", st.Reason)
	}

	job.reading = flink.Reading{Offsets: map[string]map[int]int64{"in": {0: 4}}, IdleFor: time.Hour}
	reconcile()
	if s := specJobState(t, c, key); s != "suspended" {
		t.Fatal("caught up and quiet in the job must allow the suspend")
	}

	// The job is restarted by someone: a new job id with matching sources is verified afresh.
	job.topics = []string{"in"}
	setJobID(t, r, fd, "j2")
	reconcile()
	now = now.Add(time.Minute)
	reconcile()
	if rec.count("SourcesVerified") != 1 || job.sources != 4 {
		t.Fatalf("a new job instance must be checked again: events=%v reads=%d", rec.reasons, job.sources)
	}
	if st, _, _ = r.Store.Load(ctx, fd); st.Phase != state.Suspended {
		t.Fatalf("verification must not touch the phase, got %s", st.Phase)
	}
}

func TestAJobWithoutKafkaSourceMetricsIsReportedUnverified(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	fd := createDeployment(t, c, "job", "savepoint")
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	r := newReconciler(c, &now, probe.Func(func(context.Context, []string) (map[string]string, error) { return map[string]string{"in-0": "1"}, nil }))
	job, rec := &fakeJob{}, &fakeRecorder{}
	r.Flink, r.Recorder = job, rec
	setJobID(t, r, fd, "j1")
	key := types.NamespacedName{Name: "job", Namespace: "default"}
	// The reader registers its topic metrics a few seconds after RUNNING, so a job without
	// them gets three ticks before the absence is reported, and the graph is not read again after.
	for i := range 5 {
		if _, err := r.Reconcile(ctx, reconcileRequest(key)); err != nil {
			t.Fatal(err)
		}
		if i < 2 && len(rec.reasons) != 0 {
			t.Fatalf("tick %d: too early to conclude, events = %v", i+1, rec.reasons)
		}
	}
	if rec.count("SourcesUnverified") != 1 || rec.count("SourcesDrift") != 0 || job.sources != 3 {
		t.Fatalf("events = %v, graph reads = %d", rec.reasons, job.sources)
	}
}

// A job restored from a savepoint gets its old splits back from state at once, and a topic it
// newly reads only when the enumerator discovers it, seconds later. A tick in between sees part
// of the graph: it must not be taken as the job's answer (the job-graph e2e flake).
func TestSourcesAreJudgedOnlyOnceTheGraphHoldsStill(t *testing.T) {
	c := startEnv(t)
	fd := createDeployment(t, c, "job", "savepoint")
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	r := newReconciler(c, &now, probe.Func(func(context.Context, []string) (map[string]string, error) { return map[string]string{"in": "1"}, nil }))
	job, rec := &fakeJob{topics: []string{"in"}}, &fakeRecorder{}
	r.Flink, r.Recorder = job, rec
	setJobID(t, r, fd, "j1")
	key := types.NamespacedName{Name: "job", Namespace: "default"}
	reconcile := func() {
		t.Helper()
		if _, err := r.Reconcile(context.Background(), reconcileRequest(key)); err != nil {
			t.Fatal(err)
		}
	}

	reconcile() // the restored split only
	now = now.Add(5 * time.Second)
	job.topics = []string{"in", "other"}
	reconcile() // the discovered topic has shown up
	now = now.Add(time.Minute)
	reconcile()
	if rec.count("SourcesVerified") != 0 || rec.count("SourcesDrift") != 1 {
		t.Fatalf("the job reads in and other, the annotation lists in: events = %v", rec.reasons)
	}
	now = now.Add(time.Minute)
	reconcile()
	if rec.count("SourcesDrift") != 1 || job.sources != 3 {
		t.Fatalf("once settled, the graph is not read again: events = %v, reads = %d", rec.reasons, job.sources)
	}
}

// hangingJob is a JobManager whose REST port swallows connections: every call waits out its context.
type hangingJob struct{}

func (hangingJob) Sources(ctx context.Context, _, _, _ string) (flink.Sources, error) {
	<-ctx.Done()
	return flink.Sources{}, ctx.Err()
}

func (hangingJob) Read(ctx context.Context, _, _, _ string, _ flink.Sources) (flink.Reading, error) {
	<-ctx.Done()
	return flink.Reading{}, ctx.Err()
}

// An unreachable REST API is the job gate's unknown. It must not use up the time the Kafka
// reads have, or every job would also report its source unreachable and never sleep.
func TestAHangingRESTAPILeavesKafkaItsTime(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	fd := createDeployment(t, c, "hang", "savepoint")
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	r := newReconciler(c, &now, probe.Func(func(ctx context.Context, _ []string) (map[string]string, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return map[string]string{"in": "5"}, nil
	}))
	rec := &fakeRecorder{}
	r.Flink, r.Recorder, r.ProbeTimeout = hangingJob{}, rec, 200*time.Millisecond
	setJobID(t, r, fd, "j1")
	if _, err := r.Reconcile(ctx, reconcileRequest(types.NamespacedName{Name: "hang", Namespace: "default"})); err != nil {
		t.Fatal(err)
	}
	if rec.count("SourceUnreachable") != 0 {
		t.Fatalf("a hanging REST API must not make Kafka unreachable, events %v", rec.reasons)
	}
}

// gapFunc adapts a function to probe.GapProbe and records what was asked.
type gapFunc func(topic string, partition int32, from, to int64) (bool, error)

func (f gapFunc) Records(_ context.Context, topic string, partition int32, from, to int64) (bool, error) {
	return f(topic, partition, from, to)
}

// A transactional producer leaves a commit marker after its last record: the job has emitted
// offset 9 of a partition that ends at 11. The broker is asked what lies in between, and only
// a gap it confirms holds nothing for a consumer lets the job sleep.
func TestTheJobGateAsksTheBrokerAboutAGapAfterTheLastRecord(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	fd := createDeployment(t, c, "txn", "savepoint")
	ann := fd.GetAnnotations()
	ann["siesta.flink.io/idle"] = "job"
	fd.SetAnnotations(ann)
	if err := c.Update(ctx, fd); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	r := newReconciler(c, &now, fixedOffsets(map[string]string{"in": "11"}))
	job := &fakeJob{topics: []string{"in"}, reading: flink.Reading{Offsets: map[string]map[int]int64{"in": {0: 9}}, IdleFor: time.Hour}}
	r.Flink = job
	var asked []string
	records := true
	r.Gaps = gapFunc(func(topic string, partition int32, from, to int64) (bool, error) {
		asked = append(asked, fmt.Sprintf("%s/%d [%d,%d)", topic, partition, from, to))
		return records, nil
	})
	setJobID(t, r, fd, "j1")
	key := types.NamespacedName{Name: "txn", Namespace: "default"}
	reconcile := func() {
		t.Helper()
		if _, err := r.Reconcile(ctx, reconcileRequest(key)); err != nil {
			t.Fatal(err)
		}
	}
	reconcile()
	now = now.Add(2 * time.Hour)
	reconcile()
	if s := specJobState(t, c, key); s != "running" {
		t.Fatal("a record the broker says is there must hold the suspend")
	}
	if len(asked) == 0 || asked[len(asked)-1] != "in/0 [10,11)" {
		t.Fatalf("the broker must be asked about the offsets after the last emitted record, asked %v", asked)
	}
	records = false
	now = now.Add(time.Minute)
	reconcile()
	if s := specJobState(t, c, key); s != "suspended" {
		t.Fatal("a gap of markers only must let the job sleep")
	}
}
