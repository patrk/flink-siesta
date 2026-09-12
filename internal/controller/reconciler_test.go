package controller

import (
	"context"
	"path/filepath"
	"strings"
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
	"github.com/patrk/flink-siesta/internal/store"
)

// envtest runs a real kube-apiserver + etcd locally with the FlinkDeployment CRD applied
// (make envtest downloads both). No cluster, no operator: we assert the patches we send.

// startEnv boots a kube-apiserver with the FlinkDeployment CRD (make envtest). Skips without it.
func startEnv(t *testing.T) client.Client {
	t.Helper()
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
	return c
}

// createDeployment makes an annotated, RUNNING+STABLE FlinkDeployment as the operator would report it.
func createDeployment(t *testing.T, c client.Client, name, upgradeMode string) *unstructured.Unstructured {
	t.Helper()
	ctx := context.Background()
	fd := flink.New()
	fd.SetName(name)
	fd.SetNamespace("default")
	fd.SetAnnotations(map[string]string{"siesta.flink.io/mode": "auto", "siesta.flink.io/sources": "in", "siesta.flink.io/idle-after": "1h", "siesta.flink.io/min-awake": "1m"})
	if err := unstructured.SetNestedMap(fd.Object, map[string]any{"image": "flink:1.20", "flinkVersion": "v1_20", "serviceAccount": "flink",
		"job": map[string]any{"jarURI": "local:///x.jar", "state": "running", "upgradeMode": upgradeMode}}, "spec"); err != nil {
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
	return fd
}

// newReconciler wires a reconciler whose clock and probe are closures over the test's variables.
func newReconciler(c client.Client, now *time.Time, observe probe.ActivityProbe) *Reconciler {
	return &Reconciler{Client: c, Prefix: "siesta.flink.io", Now: func() time.Time { return *now },
		Store:   store.Store{Client: c, Reader: c, Prefix: "siesta.flink.io"},
		Probe:   observe,
		Decider: decide.New(decide.RestartPolicy{MaxRestarts: 3, Window: time.Hour, BaseBackoff: time.Minute, Multiplier: 2, FailingAfter: time.Hour})}
}

func specJobState(t *testing.T, c client.Client, key types.NamespacedName) string {
	t.Helper()
	fd := flink.New()
	if err := c.Get(context.Background(), key, fd); err != nil {
		t.Fatal(err)
	}
	s, _, _ := unstructured.NestedString(fd.Object, "spec", "job", "state")
	return s
}

func TestReconcileSuspendsIdleAndResumesOnInput(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	fd := createDeployment(t, c, "job", "savepoint")

	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	offsets := map[string]string{"in-0": "5"}
	r := newReconciler(c, &now, probe.Func(func(context.Context, []string) (map[string]string, bool) { return offsets, true }))
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

func TestDryRunRecordsButNeverPatches(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	fd := createDeployment(t, c, "shadow", "savepoint")
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	offsets := map[string]string{"in-0": "5"}
	r := newReconciler(c, &now, probe.Func(func(context.Context, []string) (map[string]string, bool) { return offsets, true }))
	r.DryRun = true
	key := types.NamespacedName{Name: "shadow", Namespace: "default"}
	req := reconcileRequest(key)

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if got := specJobState(t, c, key); got != "running" {
		t.Fatalf("dry-run must never patch spec, got %q", got)
	}
	if err := c.Get(ctx, key, fd); err != nil {
		t.Fatal(err)
	}
	ann := fd.GetAnnotations()
	if ann["siesta.flink.io/state"] != "active" || !strings.Contains(ann["siesta.flink.io/reason"], "dry-run: would suspend") {
		t.Fatalf("dry-run must keep the real state and say what it would do, got %v", ann)
	}
}

func TestStatelessUpgradeModeIsRefusedOnTheServer(t *testing.T) {
	c := startEnv(t)
	ctx := context.Background()
	fd := createDeployment(t, c, "stateless", "stateless")
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	offsets := map[string]string{"in-0": "5"}
	r := newReconciler(c, &now, probe.Func(func(context.Context, []string) (map[string]string, bool) { return offsets, true }))
	key := types.NamespacedName{Name: "stateless", Namespace: "default"}
	req := reconcileRequest(key)

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if got := specJobState(t, c, key); got != "running" {
		t.Fatalf("stateless must not be suspended, got %q", got)
	}
	if err := c.Get(ctx, key, fd); err != nil {
		t.Fatal(err)
	}
	if reason := fd.GetAnnotations()["siesta.flink.io/reason"]; !strings.Contains(reason, "refused") {
		t.Fatalf("the object must say why, got %q", reason)
	}
}
