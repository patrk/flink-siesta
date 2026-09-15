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

// Operator 1.16 lets pods use Kubernetes ResourceRequirements instead of the custom block.
func TestFootprintReadsKubernetesResourceRequirements(t *testing.T) {
	u := New()
	_ = unstructured.SetNestedMap(u.Object, map[string]any{
		"job":        map[string]any{"parallelism": int64(2)},
		"jobManager": map[string]any{"resources": map[string]any{"requests": map[string]any{"cpu": "500m", "memory": "1Gi"}}},
		"taskManager": map[string]any{"resources": map[string]any{
			"limits": map[string]any{"cpu": int64(2), "memory": "2Gi"}}},
	}, "spec")
	got := FootprintOf(u)
	// one JobManager at 0.5 and 1 GiB, two TaskManagers at 2 cores and 2 GiB from their limits
	if got.CPU != 0.5+2*2 || got.Memory != 1<<30+2*(2<<30) {
		t.Fatalf("footprint = %+v", got)
	}
	// The custom block wins when both are set.
	_ = unstructured.SetNestedMap(u.Object, map[string]any{"cpu": 0.25, "memory": "512m"}, "spec", "jobManager", "resource")
	if got := FootprintOf(u); got.CPU != 0.25+2*2 || got.Memory != 512<<20+2*(2<<30) {
		t.Fatalf("custom block must win: %+v", got)
	}
}
