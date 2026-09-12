package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/patrk/flink-siesta/internal/probe"
)

// BenchmarkReconcile measures one worker reconciling many deployments against a real API
// server, with a fake probe so Kafka is not in the picture. It answers "how many
// deployments per minute does one instance handle" for the README's scale section.
// Run: make bench   (needs make envtest once for the binaries and the CRD)
func BenchmarkReconcile(b *testing.B) {
	const deployments = 200
	c := startEnv(b)
	ctx := context.Background()
	for i := range deployments {
		createDeployment(b, c, fmt.Sprintf("bench-%03d", i), "savepoint")
	}
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	offsets := map[string]string{"in-0": "5"}
	r := newReconciler(c, &now, probe.Func(func(context.Context, []string) (map[string]string, bool) { return offsets, true }))

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		key := types.NamespacedName{Namespace: "default", Name: fmt.Sprintf("bench-%03d", i%deployments)}
		if _, err := r.Reconcile(ctx, reconcileRequest(key)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(time.Minute)/float64(b.Elapsed()/time.Duration(b.N)), "reconciles/min")
}
