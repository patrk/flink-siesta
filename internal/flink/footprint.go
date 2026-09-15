package flink

import (
	"math"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Footprint is what a deployment occupies while it runs: the JobManager plus every TaskManager
// the spec implies. While the job is suspended this is what the cluster gets back, which is
// the number a savings dashboard multiplies by suspended time.
type Footprint struct {
	CPU    float64 // cores
	Memory int64   // bytes
}

// FootprintOf reads the spec. TaskManager count is spec.taskManager.replicas when set, otherwise
// ceil(parallelism / taskmanager.numberOfTaskSlots), which is how the operator sizes the job in
// application mode. Missing fields count as zero rather than guessing.
func FootprintOf(u *unstructured.Unstructured) Footprint {
	jmCPU, jmMem := resourceOf(u, "jobManager")
	tmCPU, tmMem := resourceOf(u, "taskManager")
	jmN := int64Or(u, 1, "spec", "jobManager", "replicas")
	tmN, has := nestedInt64(u, "spec", "taskManager", "replicas")
	if !has || tmN <= 0 {
		parallelism := int64Or(u, 1, "spec", "job", "parallelism")
		slots := int64(1)
		if s, _, _ := unstructured.NestedString(u.Object, "spec", "flinkConfiguration", "taskmanager.numberOfTaskSlots"); s != "" {
			if n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil && n > 0 {
				slots = n
			}
		}
		tmN = int64(math.Ceil(float64(parallelism) / float64(slots)))
	}
	return Footprint{
		CPU:    jmCPU*float64(jmN) + tmCPU*float64(tmN),
		Memory: jmMem*jmN + tmMem*tmN,
	}
}

// resourceOf reads the operator's own resource block, cpu as a number and memory as a Flink
// memory string, and otherwise the Kubernetes ResourceRequirements that operator 1.16 added,
// requests first and limits as the fallback, both as Kubernetes quantities.
func resourceOf(u *unstructured.Unstructured, component string) (cpu float64, memory int64) {
	switch v := nestedAny(u, "spec", component, "resource", "cpu").(type) {
	case float64:
		cpu = v
	case int64:
		cpu = float64(v)
	case string:
		cpu, _ = strconv.ParseFloat(v, 64)
	}
	if m, _, _ := unstructured.NestedString(u.Object, "spec", component, "resource", "memory"); m != "" {
		memory = ParseMemory(m)
	}
	for _, kind := range []string{"requests", "limits"} {
		if cpu == 0 {
			if q, ok := quantity(nestedAny(u, "spec", component, "resources", kind, "cpu")); ok {
				cpu = q.AsApproximateFloat64()
			}
		}
		if memory == 0 {
			if q, ok := quantity(nestedAny(u, "spec", component, "resources", kind, "memory")); ok {
				memory = q.Value()
			}
		}
	}
	return cpu, memory
}

// quantity reads a Kubernetes quantity that the CRD allows as a string or a number.
func quantity(v any) (resource.Quantity, bool) {
	switch x := v.(type) {
	case string:
		q, err := resource.ParseQuantity(x)
		return q, err == nil
	case int64:
		return *resource.NewQuantity(x, resource.DecimalSI), true
	case float64:
		q, err := resource.ParseQuantity(strconv.FormatFloat(x, 'f', -1, 64))
		return q, err == nil
	}
	return resource.Quantity{}, false
}

// ParseMemory reads a Flink memory string, "1024m", "2g", "512 mb", where the units are powers
// of 1024 and a bare number is bytes, and falls back to a Kubernetes quantity such as "2Gi".
// Unreadable strings are zero.
func ParseMemory(s string) int64 {
	t := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(s), " ", ""))
	units := []struct {
		suffix string
		mult   int64
	}{
		{"tb", 1 << 40}, {"t", 1 << 40}, {"gb", 1 << 30}, {"g", 1 << 30},
		{"mb", 1 << 20}, {"m", 1 << 20}, {"kb", 1 << 10}, {"k", 1 << 10}, {"bytes", 1}, {"b", 1},
	}
	for _, unit := range units {
		if n, ok := strings.CutSuffix(t, unit.suffix); ok {
			if v, err := strconv.ParseInt(n, 10, 64); err == nil {
				return v * unit.mult
			}
			break
		}
	}
	if v, err := strconv.ParseInt(t, 10, 64); err == nil {
		return v
	}
	if q, err := resource.ParseQuantity(strings.TrimSpace(s)); err == nil {
		return q.Value()
	}
	return 0
}

func nestedAny(u *unstructured.Unstructured, fields ...string) any {
	v, _, _ := unstructured.NestedFieldNoCopy(u.Object, fields...)
	return v
}

func nestedInt64(u *unstructured.Unstructured, fields ...string) (int64, bool) {
	switch v := nestedAny(u, fields...).(type) {
	case int64:
		return v, true
	case float64:
		return int64(v), true
	}
	return 0, false
}

func int64Or(u *unstructured.Unstructured, dflt int64, fields ...string) int64 {
	if v, has := nestedInt64(u, fields...); has && v > 0 {
		return v
	}
	return dflt
}
