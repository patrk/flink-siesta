package controller

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/recorder"

	"github.com/patrk/flink-siesta/internal/decide"
	"github.com/patrk/flink-siesta/internal/flink"
	"github.com/patrk/flink-siesta/internal/metrics"
	"github.com/patrk/flink-siesta/internal/policy"
	"github.com/patrk/flink-siesta/internal/probe"
	"github.com/patrk/flink-siesta/internal/state"
	"github.com/patrk/flink-siesta/internal/store"
)

const (
	defaultPollInterval = time.Minute
	defaultProbeTimeout = 10 * time.Second
)

// JobProbe reads from the running job's own REST API: the topics its Kafka sources consume,
// the offsets they have emitted and how long they have been idle. nil switches both uses off.
type JobProbe interface {
	Sources(ctx context.Context, namespace, name, jobID string) (flink.Sources, error)
	Read(ctx context.Context, namespace, name, jobID string, src flink.Sources) (flink.Reading, error)
}

type Reconciler struct {
	client.Client
	Prefix   string
	DryRun   bool
	Probe    probe.ActivityProbe
	Lag      probe.LagProbe // optional; nil when the source cannot measure consumer lag
	Flink    JobProbe       // optional; nil when the JobManager REST API is not to be used
	memo     memo           // per-process memory that need not survive a restart
	Decider  *decide.Decider
	Recorder recorder.EventRecorder
	Now      func() time.Time // injectable clock; tests freeze it
	Store    store.Store
	// PollInterval is how often each deployment is revisited; ProbeTimeout bounds one source call
	// so a hung broker degrades to "unknown" for one deployment instead of freezing the worker.
	PollInterval     time.Duration
	ProbeTimeout     time.Duration
	ResumeStallAfter time.Duration // warn once if a resumed job is not RUNNING after this
}

func (r *Reconciler) requeue() time.Duration {
	if r.PollInterval > 0 {
		return r.PollInterval
	}
	return defaultPollInterval
}

func (r *Reconciler) stallAfter() time.Duration {
	if r.ResumeStallAfter > 0 {
		return r.ResumeStallAfter
	}
	return 10 * time.Minute
}

func (r *Reconciler) probeTimeout() time.Duration {
	if r.ProbeTimeout > 0 {
		return r.ProbeTimeout
	}
	return defaultProbeTimeout
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Only objects that carry <prefix>/mode reach Reconcile, plus the one update that removes
	// it, so the state annotation and the ConfigMap can be cleared instead of going stale.
	ours := func(o client.Object) bool {
		_, ok := o.GetAnnotations()[r.Prefix+"/mode"]
		return ok
	}
	hasPolicy := predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return ours(e.Object) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return ours(e.Object) },
		GenericFunc: func(e event.GenericEvent) bool { return ours(e.Object) },
		UpdateFunc:  func(e event.UpdateEvent) bool { return ours(e.ObjectNew) || ours(e.ObjectOld) },
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("siesta").
		For(flink.New(), builder.WithPredicates(hasPolicy, relevantChange(r.Prefix))).
		Complete(r)
}

// Reconcile is one tick for one deployment, in the order the ADRs describe it: load what we
// remember, observe the broker and the job, decide, report what changed, then act. Every step
// is a function so that the order and the reasons can be read here without the details.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	fd := flink.New()
	if err := r.Get(ctx, req.NamespacedName, fd); err != nil {
		if client.IgnoreNotFound(err) == nil {
			r.forget(req)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	pol, ours := policy.Read(r.Prefix, fd.GetAnnotations())
	if !ours {
		return ctrl.Result{}, r.release(ctx, req, fd)
	}
	now := r.Now()
	live := flink.Live(fd)
	prev, seen, err := r.load(ctx, fd, live, now)
	if err != nil {
		return ctrl.Result{}, err
	}
	if pol.SourcesAuto && r.Flink == nil {
		pol.Problems = append(pol.Problems, r.Prefix+"/sources: auto needs the job's REST API, which --flink-rest=false switched off")
	}
	if len(pol.Problems) > 0 {
		return r.refuseInvalidPolicy(ctx, fd, pol, prev)
	}
	obs := r.observe(ctx, req, fd, &pol, &prev, live)
	d := r.Decider.Decide(pol, prev, live, obs, now)
	ctrl.LoggerFrom(ctx).Info("decided", gateTrace(pol, prev, live, obs, d, now)...)
	d = r.report(fd, pol, prev, live, obs, d, now)
	if r.DryRun {
		would := ""
		if d.Action != decide.None {
			would = d.Action.String()
			d = shadow(prev, d)
		}
		metrics.SetWouldAct(req.Namespace, req.Name, would)
	}
	r.record(req, fd, prev, d, now)
	return r.act(ctx, fd, prev, seen, d)
}

// forget drops everything this process holds for a deleted deployment.
func (r *Reconciler) forget(req ctrl.Request) {
	metrics.Forget(req.Namespace, req.Name)
	r.memo.forget(req.String())
}

// memo is what the process remembers between ticks and may lose on a restart: the job graph
// read once per job instance, and the time of the last tick for suspended-time accounting.
type memo struct {
	mu   sync.Mutex
	jobs map[string]cachedJob
	seen map[string]time.Time
}

func (m *memo) job(key string) (cachedJob, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.jobs[key]
	return c, ok
}

func (m *memo) setJob(key string, c cachedJob) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.jobs == nil {
		m.jobs = map[string]cachedJob{}
	}
	m.jobs[key] = c
}

// tick records now as the last tick for key and returns the previous one, if any.
func (m *memo) tick(key string, now time.Time) (time.Time, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.seen == nil {
		m.seen = map[string]time.Time{}
	}
	last, ok := m.seen[key]
	m.seen[key] = now
	return last, ok
}

func (m *memo) forget(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.jobs, key)
	delete(m.seen, key)
}

// load reads memory and reconciles it with the object (ADR 10). A deployment never seen before
// starts active, now, and takes the object's current restartNonce as its baseline: a nonce
// that was there before us is not a restart to count.
func (r *Reconciler) load(ctx context.Context, fd *unstructured.Unstructured, live decide.Live, now time.Time) (prev state.State, seen bool, err error) {
	prev, seen, err = r.Store.Load(ctx, fd)
	if err != nil {
		return state.State{}, false, err
	}
	if !seen {
		prev = state.Initial(now)
		prev.Restarts.LastNonce = live.RestartNonce
	}
	ann := fd.GetAnnotations()
	return repair(prev, ann[r.Prefix+"/state"], ann[r.Prefix+"/reason"], now), seen, nil
}

// release is the exit for a deployment that stopped being ours: someone removed the policy.
// Our two annotations and the ConfigMap go with it, so nothing stale is left to mislead.
func (r *Reconciler) release(ctx context.Context, req ctrl.Request, fd *unstructured.Unstructured) error {
	if _, marked := fd.GetAnnotations()[r.Prefix+"/state"]; !marked {
		return nil
	}
	if err := r.patcher().clear(ctx, fd); err != nil {
		return err
	}
	if err := r.Store.Delete(ctx, fd); err != nil {
		return err
	}
	r.forget(req)
	r.event(fd, corev1.EventTypeNormal, "Released", "Release", "policy removed; state and memory cleared")
	return nil
}

func (r *Reconciler) patcher() patcher {
	return patcher{Client: r.Client, Reader: r.Store.Reader, Prefix: r.Prefix, Recorder: r.Recorder}
}

// refuseInvalidPolicy handles a deployment that is ours but not usable: say why, once, and do
// nothing until the annotations are fixed.
func (r *Reconciler) refuseInvalidPolicy(ctx context.Context, fd *unstructured.Unstructured, pol policy.Policy, prev state.State) (ctrl.Result, error) {
	reason := "invalid policy: " + strings.Join(pol.Problems, "; ")
	if prev.Reason != reason {
		r.event(fd, corev1.EventTypeWarning, "InvalidPolicy", "Validate", reason)
		next := prev
		next.Reason = reason
		// Object first, memory second, like every other write: if the second fails, the next
		// tick sees the object's reason and does not repeat the event.
		if err := r.patcher().annotate(ctx, fd, next); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Store.Save(ctx, fd, next); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: r.requeue()}, nil
}

// observe asks the running job (ADR 12, ADR 13, ADR 14), then the broker and the consumer group
// when there is one. Everything that could not be asked stays unknown, which the decider never
// acts on. It advances two pieces of memory: which job instance the sources were checked
// against, and, with sources: auto, the topics learned from it. With auto it also fills in the
// policy's sources, so the decider and the rest of the tick see the effective list.
func (r *Reconciler) observe(ctx context.Context, req ctrl.Request, fd *unstructured.Unstructured, pol *policy.Policy, prev *state.State, live decide.Live) decide.Observation {
	log := ctrl.LoggerFrom(ctx)
	obs := decide.Observation{}
	pctx, cancel := context.WithTimeout(ctx, r.probeTimeout())
	defer cancel()

	// The running job is asked, once per job instance, what it reads: to verify the annotation,
	// or with sources: auto to learn the list. Both are events, never decisions.
	job, jobKnown, settled := r.jobSources(pctx, req, live)
	if settled && prev.Reported.SourcesChecked != live.JobID {
		if pol.SourcesAuto {
			r.learnSources(fd, prev, job)
		} else {
			r.verifySources(fd, pol.Sources, job)
		}
		prev.Reported.SourcesChecked = live.JobID
	}
	if pol.SourcesAuto {
		pol.Sources = prev.Sources
		if len(pol.Sources) == 0 {
			obs.Why = "sources: auto, waiting for the job to run once so its topics can be learned"
			return obs
		}
	}

	offsets, err := timed("offsets", func() (probe.Offsets, error) { return r.Probe.Observe(pctx, pol.Sources) })
	obs.Snapshot, obs.Ends = offsets.Snapshot, offsets.Ends
	obs.Known = known(log, "offsets", err)
	if pol.ConsumerGroup != "" && r.Lag != nil {
		obs.Pending, err = timed("lag", func() (int64, error) { return r.Lag.Lag(pctx, pol.ConsumerGroup, pol.Sources) })
		obs.LagKnown = known(log, "lag", err)
	}
	// With idle: job, the job is also asked whether it objects to sleeping.
	if pol.IdleFromJob && obs.Known {
		obs.ReadingNote = "job not running or its REST API not reachable"
		if jobKnown {
			reading, err := timed("flink-rest", func() (flink.Reading, error) { return r.Flink.Read(pctx, req.Namespace, req.Name, live.JobID, job) })
			if obs.ReadingKnown = known(log, "flink-rest", err); obs.ReadingKnown {
				obs.Reading = reading
			} else {
				obs.ReadingNote = err.Error()
			}
		}
	}
	return obs
}

// report raises the events that describe an edge, never a tick, and records in the next state
// that they were raised: reachability, a suspend completed without a savepoint, a stalled resume.
func (r *Reconciler) report(fd *unstructured.Unstructured, pol policy.Policy, prev state.State, live decide.Live, obs decide.Observation, d decide.Decision, now time.Time) decide.Decision {
	// Each outage is named by when it began: the events API folds identical events into a
	// series that surfaces late, and two outages deserve two lines in kubectl describe.
	switch {
	case !obs.Known && !prev.Outage.Down:
		d.Next.Outage.Since = now
		r.event(fd, corev1.EventTypeWarning, "SourceUnreachable", "Probe",
			"could not read offsets for "+strings.Join(pol.Sources, ",")+"; down since "+now.UTC().Format(time.RFC3339))
	case obs.Known && prev.Outage.Down:
		r.event(fd, corev1.EventTypeNormal, "SourceReachable", "Probe",
			"offsets readable again after "+now.Sub(prev.Outage.Since).Round(time.Second).String()+"; down since "+prev.Outage.Since.UTC().Format(time.RFC3339))
	}
	d.Next.Outage.Down = !obs.Known

	// The first time the operator reports our suspend as complete, check it took a savepoint.
	// It falls back to last-state when a savepoint fails; the job still resumes, from an older
	// checkpoint, and an operator on call should know.
	if d.Next.Phase == state.Suspended && !prev.Reported.SuspendChecked && live.LifecycleState == "SUSPENDED" {
		d.Next.Reported.SuspendChecked = true
		if live.SavepointPath == "" {
			r.event(fd, corev1.EventTypeWarning, "SuspendedWithoutSavepoint", "Suspend", "operator completed the suspend without a savepoint; resume will use the last checkpoint")
		}
	}
	if d.Next.Phase != state.Suspended {
		d.Next.Reported.SuspendChecked = false
	}

	// A resume that has not produced a RUNNING job within the stall window is worth a warning:
	// a full cluster, an image that no longer pulls, a savepoint that no longer restores.
	if !d.Next.ResumedAt.IsZero() && !d.Next.Reported.ResumeStalled && now.Sub(d.Next.ResumedAt) > r.stallAfter() {
		d.Next.Reported.ResumeStalled = true
		r.event(fd, corev1.EventTypeWarning, "ResumeStalled", "Resume", "job not RUNNING "+r.stallAfter().String()+" after resume; check pods, image and savepoint")
	}
	if d.Next.ResumedAt.IsZero() {
		d.Next.Reported.ResumeStalled = false
	}

	// The mirror image: a suspend the operator has not completed within the stall window. A
	// savepoint that cannot be written, or an operator that is not running, looks like this.
	suspending := d.Next.Phase == state.Suspended && live.LifecycleState != "SUSPENDED"
	if suspending && !d.Next.Reported.SuspendStalled && now.Sub(d.Next.SuspendedAt) > r.stallAfter() {
		d.Next.Reported.SuspendStalled = true
		r.event(fd, corev1.EventTypeWarning, "SuspendStalled", "Suspend", "operator has not completed the suspend "+r.stallAfter().String()+" after it was requested; check the operator and the savepoint")
	}
	if !suspending {
		d.Next.Reported.SuspendStalled = false
	}
	return d
}

// record updates the metrics for this tick (ADR 9: they observe, never decide).
func (r *Reconciler) record(req ctrl.Request, fd *unstructured.Unstructured, prev state.State, d decide.Decision, now time.Time) {
	metrics.SetState(req.Namespace, req.Name, string(d.Next.Phase))
	metrics.SetHeld(req.Namespace, req.Name, d.Held)
	r.account(req, fd, prev, d.Next, now)
	if acted(prev, d) {
		metrics.Transitions.WithLabelValues(req.Namespace, req.Name, d.Action.String()).Inc()
	}
	if d.ResumedAfter > 0 {
		metrics.ResumeLatency.Observe(d.ResumedAfter.Seconds())
	}
}

// act writes the decision: the object first, spec and state annotation in one patch, memory
// second. If the second write fails, the next tick repairs memory from the object's annotation
// (ADR 5, ADR 10). Nothing is written when nothing a human could see has changed.
func (r *Reconciler) act(ctx context.Context, fd *unstructured.Unstructured, prev state.State, seen bool, d decide.Decision) (ctrl.Result, error) {
	humanVisible := !seen || acted(prev, d) || prev.Phase != d.Next.Phase || prev.Reason != d.Next.Reason
	p := r.patcher()
	var err error
	switch {
	case !humanVisible:
		// nothing to write on the object
	case d.Action == decide.Suspend:
		err = p.suspend(ctx, fd, d.Next, d.Reason)
	case d.Action == decide.Resume:
		err = p.resume(ctx, fd, d.Next, d.Reason)
	case d.Action == decide.Restart:
		err = p.restart(ctx, fd, d.Next, d.Reason)
	case d.Action == decide.Refuse:
		err = p.refuse(ctx, fd, d.Next, d.Reason)
	case d.Action == decide.MarkUnrecoverable:
		err = p.markUnrecoverable(ctx, fd, d.Next, d.Reason)
	default:
		err = p.annotate(ctx, fd, d.Next)
	}
	if err != nil {
		return ctrl.Result{}, err // controller-runtime retries with backoff
	}
	if err := r.Store.Save(ctx, fd, d.Next); err != nil {
		r.event(fd, corev1.EventTypeWarning, "SaveFailed", "Save", "could not write the controller's memory: "+err.Error())
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.requeue()}, nil
}

// acted says whether this decision does something to the object. A refusal is an action the
// first time and a standing fact afterwards: repeating it every tick would be an event and a
// transition per minute for a job that simply cannot sleep.
func acted(prev state.State, d decide.Decision) bool {
	if d.Action == decide.None {
		return false
	}
	if d.Action == decide.Refuse && prev.Reason == d.Next.Reason {
		return false
	}
	return true
}

type cachedJob struct {
	id       string
	src      flink.Sources
	attempts int
}

// sourceAttempts is how many ticks a running job gets to show its Kafka source metrics before
// the absence counts as an answer. The reader registers them when it receives its splits, a
// few seconds after the job reports RUNNING, so the first tick is often too early.
const sourceAttempts = 3

// jobSources returns what the running job says about its sources. ok=false when there is no
// REST probe, no running job, or the call failed. settled=true once topics were found, or the
// job had its chances: only then is the answer worth an event, and it is cached per job id.
func (r *Reconciler) jobSources(ctx context.Context, req ctrl.Request, live decide.Live) (src flink.Sources, ok, settled bool) {
	if r.Flink == nil || live.JobID == "" || live.JobState != "RUNNING" {
		return flink.Sources{}, false, false
	}
	cached, _ := r.memo.job(req.String())
	if cached.id != live.JobID {
		cached = cachedJob{id: live.JobID}
	}
	if len(cached.src.Topics) > 0 || cached.attempts >= sourceAttempts {
		return cached.src, true, true
	}
	src, err := timed("flink-rest", func() (flink.Sources, error) { return r.Flink.Sources(ctx, req.Namespace, req.Name, live.JobID) })
	if !known(ctrl.LoggerFrom(ctx), "flink-rest", err) {
		return flink.Sources{}, false, false
	}
	cached.src, cached.attempts = src, cached.attempts+1
	r.memo.setJob(req.String(), cached)
	return src, true, len(src.Topics) > 0 || cached.attempts >= sourceAttempts
}

// learnSources is verifySources for sources: auto: the job graph is the source of truth, and
// what it says is remembered so that the wake signal exists while the job sleeps and has no
// graph to ask. A job that exposes no Kafka source teaches nothing and is reported once.
func (r *Reconciler) learnSources(fd *unstructured.Unstructured, prev *state.State, job flink.Sources) {
	switch {
	case len(job.Topics) == 0:
		r.event(fd, corev1.EventTypeWarning, "SourcesUnverified", "Learn", "job exposes no Kafka source metrics; sources: auto has nothing to learn from it")
	case !sameSet(prev.Sources, job.Topics):
		prev.Sources = job.Topics
		r.event(fd, corev1.EventTypeNormal, "SourcesLearned", "Learn", "job reads "+strings.Join(job.Topics, ",")+"; remembered as the sources to watch")
	}
}

// verifySources compares the annotation with the job graph and says what it found, once per
// job instance. It never changes the annotation: that is the user's contract, and a job can
// legitimately read a topic it should not be woken by.
func (r *Reconciler) verifySources(fd *unstructured.Unstructured, declared []string, job flink.Sources) {
	switch {
	case len(job.Topics) == 0:
		r.event(fd, corev1.EventTypeWarning, "SourcesUnverified", "Verify",
			"job exposes no Kafka source metrics; sources "+strings.Join(declared, ",")+" could not be checked against it")
	case sameSet(declared, job.Topics):
		r.event(fd, corev1.EventTypeNormal, "SourcesVerified", "Verify", "job reads exactly the annotated sources: "+strings.Join(job.Topics, ","))
	default:
		r.event(fd, corev1.EventTypeWarning, "SourcesDrift", "Verify",
			"job reads "+strings.Join(job.Topics, ",")+" but the annotation lists "+strings.Join(declared, ","))
	}
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[string]bool{}
	for _, s := range a {
		seen[s] = true
	}
	for _, s := range b {
		if !seen[s] {
			return false
		}
	}
	return true
}

// gateTrace is the "decided" log line: the action, and every gate's input, so one line explains
// why a job slept or did not. Gates that are not configured are left out.
func gateTrace(pol policy.Policy, prev state.State, live decide.Live, obs decide.Observation, d decide.Decision, now time.Time) []any {
	kv := []any{
		"action", d.Action.String(), "reason", d.Reason, "held", d.Held,
		"lifecycle", live.LifecycleState, "job", live.JobState, "specState", live.SpecJobState, "upgradeMode", live.UpgradeMode,
		"offsetsKnown", obs.Known, "idleFor", now.Sub(prev.LastActivityAt).Round(time.Second).String(), "idleAfter", pol.IdleAfter.String(),
		"awakeFor", now.Sub(prev.AwakeSince).Round(time.Second).String(), "minAwake", pol.MinAwake.String(),
	}
	if pol.ConsumerGroup != "" {
		kv = append(kv, "group", pol.ConsumerGroup, "lagKnown", obs.LagKnown, "pending", obs.Pending)
	}
	if pol.IdleFromJob {
		kv = append(kv, "jobGate", d.Gate.String(), "jobNote", d.GateNote)
	}
	return kv
}

// timed wraps a probe call with the duration histogram.
func timed[T any](kind string, call func() (T, error)) (T, error) {
	defer metrics.Timed(kind)()
	return call()
}

// known turns a probe error into the one answer the decider understands, unknown, and keeps
// the cause where an operator looks for it: the log line and the probe error counter.
func known(log logr.Logger, kind string, err error) bool {
	if err == nil {
		return true
	}
	log.Info("could not ask "+kind, "err", err.Error())
	metrics.ProbeErrors.WithLabelValues(kind).Inc()
	return false
}

// account keeps the savings and inventory gauges. Suspended time is counted between this
// tick and the previous one seen by this process, so a controller restart loses at most one
// interval and never double counts; Prometheus handles the counter reset.
func (r *Reconciler) account(req ctrl.Request, fd *unstructured.Unstructured, prev, next state.State, now time.Time) {
	ns, name := req.Namespace, req.Name
	if last, ok := r.memo.tick(req.String(), now); ok && prev.Phase == state.Suspended && next.Phase == state.Suspended {
		if gap := now.Sub(last); gap > 0 {
			metrics.SuspendedSeconds.WithLabelValues(ns, name).Add(gap.Seconds())
		}
	}
	fp := flink.Footprint{}
	since := 0.0
	if next.Phase == state.Suspended {
		fp = flink.FootprintOf(fd)
		since = float64(next.SuspendedAt.Unix())
	}
	metrics.ReleasedCPU.WithLabelValues(ns, name).Set(fp.CPU)
	metrics.ReleasedMemory.WithLabelValues(ns, name).Set(float64(fp.Memory))
	metrics.SuspendedSince.WithLabelValues(ns, name).Set(since)
	metrics.IdleSeconds.WithLabelValues(ns, name).Set(now.Sub(next.LastActivityAt).Seconds())
}

// repair reconciles memory with what the object itself says. The state annotation is written
// in the same patch as spec.job.state, so if the two stores disagree, the object is the truth:
// a suspend or resume whose memory write failed is recovered instead of being mistaken for a
// manual change.
func repair(prev state.State, objectState, objectReason string, now time.Time) state.State {
	switch {
	case objectState == string(state.Suspended) && prev.Phase == state.Active:
		prev.Phase, prev.SuspendedAt, prev.Reason = state.Suspended, now, "memory repaired from object"
	case objectState == string(state.Active) && prev.Phase == state.Suspended:
		prev.Phase, prev.SuspendedAt, prev.AwakeSince, prev.ResumedAt, prev.Reason = state.Active, time.Time{}, now, now, "memory repaired from object"
	case objectState == string(state.Unrecoverable) && prev.Phase != state.Unrecoverable:
		// The mark reached the object but not memory. The object's reason is the one a human read.
		prev.Phase, prev.Reason = state.Unrecoverable, objectReason
	}
	return prev
}

func (r *Reconciler) event(fd *unstructured.Unstructured, kind, reason, action, note string) {
	if r.Recorder != nil {
		r.Recorder.Eventf(fd, nil, kind, reason, action, "%s", note)
	}
}

// shadow turns a decision into a record of what would have happened: the observation is kept
// (so idle detection keeps working), the phase and clocks are not, and the reason says so.
// Shadow mode must never write a state the cluster is not actually in.
func shadow(prev state.State, d decide.Decision) decide.Decision {
	next := d.Next
	next.Phase, next.SuspendedAt, next.AwakeSince, next.Restarts, next.ResumedAt = prev.Phase, prev.SuspendedAt, prev.AwakeSince, prev.Restarts, prev.ResumedAt
	next.Reason = "dry-run: would " + d.Action.String() + ": " + d.Reason
	return decide.Decision{Action: decide.None, Next: next, Reason: next.Reason}
}
