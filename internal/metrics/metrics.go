// Package metrics exposes what the controller did. Nothing in the controller reads these back:
// metrics observe, they never decide. They are registered on controller-runtime's registry, so
// they appear on the same /metrics endpoint as the reconcile and work-queue metrics.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// State is 1 for the deployment's current state and 0 for the other two, so
	// sum(siesta_deployment_state{state="suspended"}) is "how many are suspended right now".
	State = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "siesta_deployment_state",
		Help: "1 if the FlinkDeployment is in this state (active, suspended, unrecoverable), else 0.",
	}, []string{"namespace", "name", "state"})

	Transitions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "siesta_transitions_total",
		Help: "Actions the controller took, by action (suspend, resume, restart, mark-unrecoverable, refuse).",
	}, []string{"namespace", "name", "action"})

	ResumeLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "siesta_resume_latency_seconds",
		Help:    "Seconds from the input that triggered a resume until the job reported RUNNING again.",
		Buckets: []float64{15, 30, 60, 90, 120, 180, 300, 600},
	})

	ProbeErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "siesta_probe_errors_total",
		Help: "Times a source could not be asked, by what was asked (offsets, lag, flink-rest).",
	}, []string{"kind"})

	// The savings view. SuspendedSeconds accumulates wall time spent suspended; Released* hold
	// the deployment's footprint while it is suspended and 0 otherwise, so avg_over_time of a
	// released gauge times the window is what the cluster got back.
	SuspendedSeconds = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "siesta_suspended_seconds_total",
		Help: "Seconds the deployment has spent suspended by the controller.",
	}, []string{"namespace", "name"})

	SuspendedSince = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "siesta_suspended_since_timestamp_seconds",
		Help: "Unix time the current suspension began, 0 while awake.",
	}, []string{"namespace", "name"})

	ReleasedCPU = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "siesta_released_cpu_cores",
		Help: "CPU cores the deployment's JobManager and TaskManagers would occupy, while it is suspended; 0 otherwise.",
	}, []string{"namespace", "name"})

	ReleasedMemory = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "siesta_released_memory_bytes",
		Help: "Memory the deployment's JobManager and TaskManagers would occupy, while it is suspended; 0 otherwise.",
	}, []string{"namespace", "name"})

	// The inventory view, for a dry run on an existing namespace: how idle is each job, and
	// what would the controller do right now if it were allowed to.
	IdleSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "siesta_idle_seconds",
		Help: "Seconds since the last observed input on the deployment's sources.",
	}, []string{"namespace", "name"})

	WouldAct = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "siesta_dry_run_would_act",
		Help: "1 for the action the controller would take now in dry-run (suspend, resume, restart, refuse), 0 otherwise.",
	}, []string{"namespace", "name", "action"})

	// HeldAwake is 1 for the gate that keeps an idle job awake this tick, so
	// sum by (gate) (siesta_held_awake) is "why are idle jobs not sleeping" for the namespace.
	HeldAwake = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "siesta_held_awake",
		Help: "1 for the gate holding an idle deployment awake (min-awake, lag-unknown, lag-pending, job-unknown, job-busy), 0 otherwise.",
	}, []string{"namespace", "name", "gate"})

	ProbeDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "siesta_probe_duration_seconds",
		Help:    "Round trip of one probe call, by what was asked (offsets, lag, flink-rest).",
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, []string{"kind"})
)

var (
	dryRunActions = []string{"suspend", "resume", "restart", "refuse", "mark-unrecoverable"}
	gates         = []string{"min-awake", "lag-unknown", "lag-pending", "job-unknown", "job-busy"}
)

var states = []string{"active", "suspended", "unrecoverable"}

func init() {
	metrics.Registry.MustRegister(State, Transitions, ResumeLatency, ProbeErrors,
		SuspendedSeconds, SuspendedSince, ReleasedCPU, ReleasedMemory, IdleSeconds, WouldAct, HeldAwake, ProbeDuration)
}

// SetState records the current state and clears the other two for this deployment.
func SetState(namespace, name, current string) {
	for _, s := range states {
		v := 0.0
		if s == current {
			v = 1
		}
		State.WithLabelValues(namespace, name, s).Set(v)
	}
}

// SetHeld records which gate holds the deployment awake, clearing the others.
func SetHeld(namespace, name, gate string) {
	for _, g := range gates {
		v := 0.0
		if g == gate {
			v = 1
		}
		HeldAwake.WithLabelValues(namespace, name, g).Set(v)
	}
}

// Timed observes one probe round trip: defer metrics.Timed("offsets")() around the call.
func Timed(kind string) func() {
	t := prometheus.NewTimer(ProbeDuration.WithLabelValues(kind))
	return func() { t.ObserveDuration() }
}

// SetWouldAct records the action a dry run would take, clearing the others.
func SetWouldAct(namespace, name, action string) {
	for _, a := range dryRunActions {
		v := 0.0
		if a == action {
			v = 1
		}
		WouldAct.WithLabelValues(namespace, name, a).Set(v)
	}
}

// Forget drops a deleted deployment's series so dashboards do not show ghosts.
func Forget(namespace, name string) {
	labels := prometheus.Labels{"namespace": namespace, "name": name}
	for _, v := range []*prometheus.GaugeVec{State, SuspendedSince, ReleasedCPU, ReleasedMemory, IdleSeconds, WouldAct, HeldAwake} {
		v.DeletePartialMatch(labels)
	}
	Transitions.DeletePartialMatch(labels)
	SuspendedSeconds.DeletePartialMatch(labels)
}
