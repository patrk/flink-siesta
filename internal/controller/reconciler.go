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

const requeue = time.Minute

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
	obs := decide.Observation{}
	obs.Snapshot, obs.Known = r.Probe.Observe(ctx, pol.Sources)
	if !obs.Known {
		metrics.ProbeErrors.WithLabelValues("offsets").Inc()
	}
	if pol.ConsumerGroup != "" && r.Lag != nil {
		obs.Pending, obs.LagKnown = r.Lag.Lag(ctx, pol.ConsumerGroup, pol.Sources)
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
	metrics.SetState(req.Namespace, req.Name, string(d.Next.Phase))
	if d.Action != decide.None {
		metrics.Transitions.WithLabelValues(req.Namespace, req.Name, d.Action.String()).Inc()
	}
	if d.ResumedAfter > 0 {
		metrics.ResumeLatency.Observe(d.ResumedAfter.Seconds())
	}

	// The controller's memory goes to its own ConfigMap every tick: cheap, and nobody watches it.
	// The deployment itself is touched only when something a human would want to see changed.
	if err := r.Store.Save(ctx, fd, d.Next); err != nil {
		return ctrl.Result{}, err
	}
	humanVisible := !seen || d.Action != decide.None || prev.Phase != d.Next.Phase || prev.Reason != d.Next.Reason
	p := patcher{Client: r.Client, Prefix: r.Prefix, Recorder: r.Recorder}
	switch {
	case !humanVisible:
		// nothing to write on the object
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
	case d.Action == decide.MarkUnrecoverable:
		err = p.markUnrecoverable(ctx, fd, d.Next, d.Reason)
	default:
		err = p.annotate(ctx, fd, d.Next)
	}
	if err != nil {
		return ctrl.Result{}, err // controller-runtime retries with backoff
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

func (r *Reconciler) event(fd *unstructured.Unstructured, kind, reason, action, note string) {
	if r.Recorder != nil {
		r.Recorder.Eventf(fd, nil, kind, reason, action, "%s", note)
	}
}
