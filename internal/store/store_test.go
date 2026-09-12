package store

import (
	"context"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/patrk/flink-siesta/internal/flink"
	"github.com/patrk/flink-siesta/internal/state"
)

// The fake client is enough here: Store only does Get, Create and Patch on ConfigMaps.
func TestStoreRoundTripAndOwnership(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
	s := Store{Client: c, Reader: c, Prefix: "siesta.flink.io"}
	ctx := context.Background()

	fd := flink.New()
	fd.SetNamespace("ns")
	fd.SetName("job")
	fd.SetUID("uid-1")

	if _, ok, err := s.Load(ctx, fd); err != nil || ok {
		t.Fatalf("fresh deployment must have no state, got ok=%v err=%v", ok, err)
	}
	t0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	want := state.Initial(t0)
	want.Snapshot = map[string]string{"t-0": "5"}
	want.Restarts = state.RestartBudget{Count: 1, WindowStart: t0, NextAfter: t0.Add(time.Minute)}
	if err := s.Save(ctx, fd, want); err != nil {
		t.Fatal(err)
	}
	var cm corev1.ConfigMap
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "siesta-job"}, &cm); err != nil {
		t.Fatal(err)
	}
	if len(cm.OwnerReferences) != 1 || cm.OwnerReferences[0].UID != "uid-1" || cm.OwnerReferences[0].Kind != "FlinkDeployment" {
		t.Fatalf("ConfigMap must be owned by the deployment, got %+v", cm.OwnerReferences)
	}
	got, ok, err := s.Load(ctx, fd)
	if err != nil || !ok {
		t.Fatalf("load after save: ok=%v err=%v", ok, err)
	}
	if got.Snapshot["t-0"] != "5" || got.Restarts.Count != 1 || !got.Restarts.NextAfter.Equal(t0.Add(time.Minute)) || !got.AwakeSince.Equal(t0) {
		t.Fatalf("round trip lost data: %+v", got)
	}
}

func TestStoreMigratesFromAnnotations(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
	s := Store{Client: c, Reader: c, Prefix: "siesta.flink.io"}
	fd := flink.New()
	fd.SetNamespace("ns")
	fd.SetName("old")
	fd.SetAnnotations(map[string]string{"siesta.flink.io/state": "suspended", "siesta.flink.io/offsets": `{"t-0":"9"}`})
	got, ok, err := s.Load(context.Background(), fd)
	if err != nil || !ok || got.Phase != state.Suspended || got.Snapshot["t-0"] != "9" {
		t.Fatalf("state written by an older version must still load: ok=%v err=%v %+v", ok, err, got)
	}
}

// countingClient counts writes so a test can prove an unchanged state is not written.
type countingClient struct {
	client.Client
	patches, creates int
}

func (c *countingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	c.patches++
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *countingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	c.creates++
	return c.Client.Create(ctx, obj, opts...)
}

func TestStoreDoesNotWriteUnchangedStateAndRecreatesAfterDeletion(t *testing.T) {
	base := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
	cc := &countingClient{Client: base}
	s := Store{Client: cc, Reader: base, Prefix: "siesta.flink.io"}
	ctx := context.Background()
	fd := flink.New()
	fd.SetNamespace("ns")
	fd.SetName("job")
	fd.SetUID("uid-2")
	st := state.Initial(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))

	for range 3 {
		if err := s.Save(ctx, fd, st); err != nil {
			t.Fatal(err)
		}
	}
	if cc.creates != 1 || cc.patches != 0 {
		t.Fatalf("three saves of the same state: want 1 create, 0 patches; got %d, %d", cc.creates, cc.patches)
	}
	st.Reason = "changed"
	if err := s.Save(ctx, fd, st); err != nil {
		t.Fatal(err)
	}
	if cc.patches != 1 {
		t.Fatalf("a changed state must be patched once, got %d", cc.patches)
	}

	var cm corev1.ConfigMap
	if err := base.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "siesta-job"}, &cm); err != nil {
		t.Fatal(err)
	}
	if err := base.Delete(ctx, &cm); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, fd, st); err != nil {
		t.Fatal(err)
	}
	if cc.creates != 2 {
		t.Fatalf("a deleted ConfigMap must be recreated, creates=%d", cc.creates)
	}
}

func TestStoreIgnoresAndReplacesStateOfAPreviousDeploymentWithTheSameName(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
	s := Store{Client: c, Reader: c, Prefix: "siesta.flink.io"}
	ctx := context.Background()
	old := flink.New()
	old.SetNamespace("ns")
	old.SetName("job")
	old.SetUID("uid-old")
	suspended := state.Initial(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	suspended.Phase = state.Suspended
	if err := s.Save(ctx, old, suspended); err != nil {
		t.Fatal(err)
	}

	fresh := flink.New() // same name, recreated: new UID, old ConfigMap not yet garbage-collected
	fresh.SetNamespace("ns")
	fresh.SetName("job")
	fresh.SetUID("uid-new")
	if _, ok, err := s.Load(ctx, fresh); err != nil || ok {
		t.Fatalf("a recreated deployment must not inherit the old one's state, ok=%v err=%v", ok, err)
	}
	if err := s.Save(ctx, fresh, state.Initial(time.Now())); err != nil {
		t.Fatal(err)
	}
	var cm corev1.ConfigMap
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "siesta-job"}, &cm); err != nil {
		t.Fatal(err)
	}
	if len(cm.OwnerReferences) != 1 || cm.OwnerReferences[0].UID != "uid-new" || cm.Data["state"] != "active" {
		t.Fatalf("the ConfigMap must now belong to the new deployment with fresh state, got owner=%v state=%q", cm.OwnerReferences, cm.Data["state"])
	}
}
