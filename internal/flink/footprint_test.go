package flink

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestParseMemory(t *testing.T) {
	cases := map[string]int64{
		"1024m": 1024 << 20, "2g": 2 << 30, "512 mb": 512 << 20, "1 GB": 1 << 30, "4096": 4096,
		"2Gi": 2 << 30, "1536Mi": 1536 << 20, "junk": 0, "": 0,
	}
	for in, want := range cases {
		if got := ParseMemory(in); got != want {
			t.Errorf("ParseMemory(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestFootprintFollowsTheOperatorsSizing(t *testing.T) {
	u := New()
	_ = unstructured.SetNestedMap(u.Object, map[string]any{
		"job":                map[string]any{"parallelism": int64(6)},
		"flinkConfiguration": map[string]any{"taskmanager.numberOfTaskSlots": "4"},
		"jobManager":         map[string]any{"resource": map[string]any{"cpu": 0.5, "memory": "1024m"}},
		"taskManager":        map[string]any{"resource": map[string]any{"cpu": int64(2), "memory": "2g"}},
	}, "spec")
	got := FootprintOf(u)
	// one JobManager plus ceil(6/4) = 2 TaskManagers
	if got.CPU != 0.5+2*2 || got.Memory != 1024<<20+2*(2<<30) {
		t.Fatalf("footprint = %+v", got)
	}
	_ = unstructured.SetNestedField(u.Object, int64(5), "spec", "taskManager", "replicas")
	if got := FootprintOf(u); got.CPU != 0.5+5*2 {
		t.Fatalf("explicit replicas must win: %+v", got)
	}
	if got := FootprintOf(New()); got != (Footprint{}) {
		t.Fatalf("an empty spec is zero, not a guess: %+v", got)
	}
}
