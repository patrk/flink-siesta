package controller

import (
	"context"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

type Reconciler struct {
	client.Client
	Prefix   string
	DryRun   bool
	Probe    probe.ActivityProbe
	Lag      probe.LagProbe // optional; nil when the source cannot measure consumer lag
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
	// Only objects that carry <prefix>/mode reach Reconcile
	hasPolicy := predicate.NewPredicateFuncs(func(o client.Object) bool {
		_, ok := o.GetAnnotations()[r.Prefix+"/mode"]
		return ok
	})
	return ctrl.NewControllerManagedBy(mgr).
		Named("siesta").
		For(flink.New(), builder.WithPredicates(hasPolicy, relevantChange(r.Prefix))).
		Complete(r)
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	fd := flink.New()
	if err := r.Get(ctx, req.NamespacedName, fd); err != nil {
		if client.IgnoreNotFound(err) == nil {
			metrics.Forget(req.Namespace, req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	ann := fd.GetAnnotations()
	pol, ok := policy.Read(r.Prefix, ann)
	if !ok {
		return ctrl.Result{}, nil
	}
	now := r.Now()
	prev, seen, err := r.Store.Load(ctx, fd)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !seen {
		prev = state.Initial(now)
	}
	prev = repair(prev, ann[r.Prefix+"/state"], now)
	if len(pol.Problems) > 0 {
		// Ours, but not usable. Say why, once, and do nothing until the annotations are fixed.
		reason := "invalid policy: " + strings.Join(pol.Problems, "; ")
		if prev.Reason != reason {
			r.event(fd, corev1.EventTypeWarning, "InvalidPolicy", "Validate", reason)
			next := prev
			next.Reason = reason
			if err := r.Store.Save(ctx, fd, next); err != nil {
				return ctrl.Result{}, err
			}
			p := patcher{Client: r.Client, Prefix: r.Prefix, Recorder: r.Recorder}
			if err := p.annotate(ctx, fd, next); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: r.requeue()}, nil
	}
	obs := decide.Observation{}
	pctx, cancel := context.WithTimeout(ctx, r.probeTimeout())
	defer cancel()
	obs.Snapshot, obs.Known = r.Probe.Observe(pctx, pol.Sources)
	if !obs.Known {
		metrics.ProbeErrors.WithLabelValues("offsets").Inc()
	}
	if pol.ConsumerGroup != "" && r.Lag != nil {
		obs.Pending, obs.LagKnown = r.Lag.Lag(pctx, pol.ConsumerGroup, pol.Sources)
		if !obs.LagKnown {
			metrics.ProbeErrors.WithLabelValues("lag").Inc()
		}
	}
	d := r.Decider.Decide(pol, prev, flink.Live(fd), obs, now)
	log.Info("decided", "action", d.Action.String(), "reason", d.Reason)

	// Reachability is reported once per edge, never per tick: an event when the source stops
	// answering and one when it answers again. The flag lives in the store, not on the object.
	switch {
	case !obs.Known && !prev.SourceDown:
		r.event(fd, corev1.EventTypeWarning, "SourceUnreachable", "Probe", "could not read offsets for "+strings.Join(pol.Sources, ","))
	case obs.Known && prev.SourceDown:
		r.event(fd, corev1.EventTypeNormal, "SourceReachable", "Probe", "offsets readable again")
	}
	d.Next.SourceDown = !obs.Known

	// The first time the operator reports our suspend as complete, check it took a savepoint.
	// It falls back to last-state when a savepoint fails; the job still resumes, from an older
	// checkpoint, and an operator on call should know.
	live := flink.Live(fd)
	if d.Next.Phase == state.Suspended && !prev.SuspendChecked && live.LifecycleState == "SUSPENDED" {
		d.Next.SuspendChecked = true
		if live.SavepointPath == "" {
			r.event(fd, corev1.EventTypeWarning, "SuspendedWithoutSavepoint", "Suspend", "operator completed the suspend without a savepoint; resume will use the last checkpoint")
		}
	}
	if d.Next.Phase != state.Suspended {
		d.Next.SuspendChecked = false
	}

	// A resume that has not produced a RUNNING job within the stall window is worth a warning:
	// a full cluster, an image that no longer pulls, a savepoint that no longer restores.
	if !d.Next.ResumedAt.IsZero() && !d.Next.ResumeStalledReported && now.Sub(d.Next.ResumedAt) > r.stallAfter() {
		d.Next.ResumeStalledReported = true
		r.event(fd, corev1.EventTypeWarning, "ResumeStalled", "Resume", "job not RUNNING "+r.stallAfter().String()+" after resume; check pods, image and savepoint")
	}
	if d.Next.ResumedAt.IsZero() {
		d.Next.ResumeStalledReported = false
	}

	if r.DryRun && d.Action != decide.None {
		d = shadow(prev, d)
	}
	metrics.SetState(req.Namespace, req.Name, string(d.Next.Phase))
	if d.Action != decide.None {
		metrics.Transitions.WithLabelValues(req.Namespace, req.Name, d.Action.String()).Inc()
	}
	if d.ResumedAfter > 0 {
		metrics.ResumeLatency.Observe(d.ResumedAfter.Seconds())
	}

	// Order matters: the object first (spec and state annotation in one patch), memory second.
	// If the second write fails, the next tick repairs memory from the object's annotation.
	humanVisible := !seen || d.Action != decide.None || prev.Phase != d.Next.Phase || prev.Reason != d.Next.Reason
	p := patcher{Client: r.Client, Prefix: r.Prefix, Recorder: r.Recorder}
	switch {
	case !humanVisible:
		// nothing to write on the object
	case d.Action == decide.Suspend:
		err = p.suspend(ctx, fd, d.Next, d.Reason)
	case d.Action == decide.Resume:
		err = p.resume(ctx, fd, d.Next, d.Reason)
	case d.Action == decide.Restart:
		err = p.restart(ctx, fd, d.Next, d.Reason, now)
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
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.requeue()}, nil
}

// repair reconciles memory with what the object itself says. The state annotation is written
// in the same patch as spec.job.state, so if the two stores disagree, the object is the truth:
// a suspend or resume whose memory write failed is recovered instead of being mistaken for a
// manual change.
func repair(prev state.State, objectState string, now time.Time) state.State {
	switch {
	case objectState == string(state.Suspended) && prev.Phase == state.Active:
		prev.Phase, prev.SuspendedAt, prev.Reason = state.Suspended, now, "memory repaired from object"
	case objectState == string(state.Active) && prev.Phase == state.Suspended:
		prev.Phase, prev.SuspendedAt, prev.AwakeSince, prev.ResumedAt, prev.Reason = state.Active, time.Time{}, now, now, "memory repaired from object"
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
