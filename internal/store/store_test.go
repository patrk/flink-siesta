package store

import (
	"context"
	"testing"
	"time"

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
