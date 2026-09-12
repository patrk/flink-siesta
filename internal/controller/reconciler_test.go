package controller

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/patrk/flink-siesta/internal/decide"
	"github.com/patrk/flink-siesta/internal/flink"
	"github.com/patrk/flink-siesta/internal/probe"
	"github.com/patrk/flink-siesta/internal/state"
)

// envtest runs a real kube-apiserver + etcd locally with the FlinkDeployment CRD applied
// (make envtest downloads both). No cluster, no operator: we assert the patches we send.

func TestReconcileSuspendsIdleAndResumesOnInput(t *testing.T) {
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "test", "crds")}}
	cfg, err := env.Start()
	if err != nil {
		t.Skipf("envtest not available: %v (run make envtest)", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Logf("stop envtest: %v", err)
		}
	})
	c, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	fd := flink.New()
	fd.SetName("job")
	fd.SetNamespace("default")
	fd.SetAnnotations(map[string]string{"siesta.flink.io/mode": "auto", "siesta.flink.io/sources": "in", "siesta.flink.io/idle-after": "1h", "siesta.flink.io/min-awake": "1m"})
	if err := unstructured.SetNestedMap(fd.Object, map[string]any{"image": "flink:1.20", "flinkVersion": "v1_20", "serviceAccount": "flink",
		"job": map[string]any{"jarURI": "local:///x.jar", "state": "running", "upgradeMode": "savepoint"}}, "spec"); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, fd); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedMap(fd.Object, map[string]any{"jobStatus": map[string]any{"state": "RUNNING"}, "lifecycleState": "STABLE"}, "status"); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(ctx, fd); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	offsets := map[string]string{"in-0": "5"}
	r := &Reconciler{Client: c, Prefix: "siesta.flink.io", Now: func() time.Time { return now },
		Probe:   probe.Func(func(context.Context, []string) (map[string]string, bool) { return offsets, true }),
		Decider: decide.New(decide.RestartPolicy{MaxRestarts: 3, Window: time.Hour, BaseBackoff: time.Minute, Multiplier: 2, FailingAfter: time.Hour})}
	key := types.NamespacedName{Name: "job", Namespace: "default"}
	reconcile := func() {
		t.Helper()
		if _, err := r.Reconcile(ctx, reconcileRequest(key)); err != nil {
			t.Fatal(err)
		}
	}

	reconcile() // first observation
	now = now.Add(2 * time.Hour)
	reconcile() // idle > 1h and awake > 1m
	if err := c.Get(ctx, key, fd); err != nil {
		t.Fatal(err)
	}
	if s, _, _ := unstructured.NestedString(fd.Object, "spec", "job", "state"); s != "suspended" {
		t.Fatalf("want spec.job.state=suspended, got %q", s)
	}
	if st, _ := state.Read("siesta.flink.io", fd.GetAnnotations()); st.Phase != state.Suspended {
		t.Fatalf("want state annotation suspended, got %q", st.Phase)
	}

	// envtest has no Flink operator; play its part and report the suspend as complete.
	if err := unstructured.SetNestedField(fd.Object, "SUSPENDED", "status", "lifecycleState"); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(ctx, fd); err != nil {
		t.Fatal(err)
	}

	offsets = map[string]string{"in-0": "6"}
	now = now.Add(time.Minute)
	reconcile()
	if err := c.Get(ctx, key, fd); err != nil {
		t.Fatal(err)
	}
	if s, _, _ := unstructured.NestedString(fd.Object, "spec", "job", "state"); s != "running" {
		t.Fatalf("want spec.job.state=running after input, got %q", s)
	}
}
