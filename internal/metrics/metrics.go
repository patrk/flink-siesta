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
		Help: "Times a source could not be asked, by what was asked (offsets, lag).",
	}, []string{"kind"})
)

var states = []string{"active", "suspended", "unrecoverable"}

func init() {
	metrics.Registry.MustRegister(State, Transitions, ResumeLatency, ProbeErrors)
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

// Forget drops a deleted deployment's series so dashboards do not show ghosts.
func Forget(namespace, name string) {
	State.DeletePartialMatch(prometheus.Labels{"namespace": namespace, "name": name})
	Transitions.DeletePartialMatch(prometheus.Labels{"namespace": namespace, "name": name})
}
