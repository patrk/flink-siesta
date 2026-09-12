package controller

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/patrk/flink-siesta/internal/flink"
)

func obj(gen int64, ann map[string]string, lifecycle string) *unstructured.Unstructured {
	u := flink.New()
	u.SetGeneration(gen)
	u.SetAnnotations(ann)
	_ = unstructured.SetNestedField(u.Object, lifecycle, "status", "lifecycleState")
	return u
}

func TestChangedOutside(t *testing.T) {
	const p = "siesta.flink.io"
	base := map[string]string{p + "/mode": "auto", "team": "a"}
	ours := map[string]string{p + "/mode": "auto", "team": "a", p + "/state": "active", p + "/offsets": "{}"}
	cases := []struct {
		name     string
		old, new *unstructured.Unstructured
		want     bool
	}{
		{"our own annotations only", obj(1, base, "STABLE"), obj(1, ours, "STABLE"), false},
		{"spec edit", obj(1, ours, "STABLE"), obj(2, ours, "STABLE"), true},
		{"operator status moved", obj(1, ours, "STABLE"), obj(1, ours, "SUSPENDED"), true},
		{"someone else's annotation", obj(1, ours, "STABLE"), obj(1, map[string]string{p + "/mode": "auto", "team": "b", p + "/state": "active"}, "STABLE"), true},
		{"policy annotation edited", obj(1, ours, "STABLE"), obj(1, map[string]string{p + "/mode": "off", "team": "a", p + "/state": "active"}, "STABLE"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := changedOutside(p, c.old, c.new); got != c.want {
				t.Fatalf("want %v, got %v", c.want, got)
			}
		})
	}
}
