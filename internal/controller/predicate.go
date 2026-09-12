package controller

import (
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/patrk/flink-siesta/internal/flink"
)

// relevantChange lets an update through only if something we act on changed: the spec
// generation, the status fields the decider reads, or annotations that are not ours.
// Without it every state annotation we write would wake our own reconciler, doubling the
// Kafka calls for nothing; the periodic requeue already drives the poll.
func relevantChange(prefix string) predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldU, ok1 := e.ObjectOld.(*unstructured.Unstructured)
			newU, ok2 := e.ObjectNew.(*unstructured.Unstructured)
			if !ok1 || !ok2 {
				return true
			}
			return changedOutside(prefix, oldU, newU)
		},
	}
}

func changedOutside(prefix string, oldU, newU *unstructured.Unstructured) bool {
	if oldU.GetGeneration() != newU.GetGeneration() {
		return true
	}
	if flink.Live(oldU) != flink.Live(newU) {
		return true
	}
	return !equalForeignAnnotations(prefix+"/", oldU.GetAnnotations(), newU.GetAnnotations())
}

// equalForeignAnnotations compares annotations while ignoring keys under our prefix.
func equalForeignAnnotations(prefix string, a, b map[string]string) bool {
	foreign := func(m map[string]string) map[string]string {
		out := map[string]string{}
		for k, v := range m {
			if !strings.HasPrefix(k, prefix) {
				out[k] = v
			}
		}
		return out
	}
	fa, fb := foreign(a), foreign(b)
	if len(fa) != len(fb) {
		return false
	}
	for k, v := range fa {
		if fb[k] != v {
			return false
		}
	}
	return true
}
