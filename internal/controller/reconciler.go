package controller

import (
	"context"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/recorder"

	"github.com/patrk/flink-siesta/internal/decide"
	"github.com/patrk/flink-siesta/internal/flink"
	"github.com/patrk/flink-siesta/internal/policy"
	"github.com/patrk/flink-siesta/internal/probe"
	"github.com/patrk/flink-siesta/internal/state"
)

const (
	requeue      = time.Minute
	persistEvery = 10 * time.Minute // how often an active, busy job's activity is written back
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
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	ann := fd.GetAnnotations()
	pol, ok := policy.Read(r.Prefix, ann)
	if !ok {
		return ctrl.Result{}, nil
	}
	now := r.Now()
	prev, seen := state.Read(r.Prefix, ann)
	if !seen {
		prev = state.Initial(now)
	}
	obs := decide.Observation{}
	obs.Snapshot, obs.Known = r.Probe.Observe(ctx, pol.Sources)
	if pol.ConsumerGroup != "" && r.Lag != nil {
		obs.Pending, obs.LagKnown = r.Lag.Lag(ctx, pol.ConsumerGroup, pol.Sources)
	}
	d := r.Decider.Decide(pol, prev, flink.Live(fd), obs, now)
	log.Info("decided", "action", d.Action.String(), "reason", d.Reason)

	// A busy topic moves every tick. Persisting that every minute is churn for no information:
	// write activity for an active job at most every persistEvery. The persisted timestamp can
	// lag reality by that much after a controller restart, which is nothing against idle-after.
	if d.Action == decide.None && seen && onlyActivityChanged(prev, d.Next) && now.Sub(prev.LastActivityAt) < persistEvery {
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	p := patcher{Client: r.Client, Prefix: r.Prefix, Recorder: r.Recorder}
	var err error
	switch {
	case r.DryRun:
		err = p.annotate(ctx, fd, d.Next)
	case d.Action == decide.Suspend:
		err = p.suspend(ctx, fd, d.Next, d.Reason)
	case d.Action == decide.Resume:
		err = p.resume(ctx, fd, d.Next, d.Reason)
	case d.Action == decide.Restart:
		err = p.restart(ctx, fd, d.Next, d.Reason, now)
	case d.Action == decide.Refuse:
		err = p.refuse(ctx, fd, d.Next, d.Reason)
	default:
		err = p.annotate(ctx, fd, d.Next)
	}
	if err != nil {
		return ctrl.Result{}, err // controller-runtime retries with backoff
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// onlyActivityChanged: the two states differ at most in the snapshot and the activity timestamp.
func onlyActivityChanged(prev, next state.State) bool {
	a, b := prev, next
	a.Snapshot, b.Snapshot = nil, nil
	a.LastActivityAt, b.LastActivityAt = time.Time{}, time.Time{}
	a.Reason, b.Reason = "", ""
	return a.Phase == b.Phase && a.SuspendedAt.Equal(b.SuspendedAt) && a.AwakeSince.Equal(b.AwakeSince) &&
		a.Restarts == b.Restarts && a.Generation == b.Generation
}
