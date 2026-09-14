package flink

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestLiveReadsBothErrorFields(t *testing.T) {
	u := New()
	if err := unstructured.SetNestedField(u.Object, "something reconciliation", "status", "reconciliationStatus", "error"); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(u.Object, `{"message":"UnknownTopicOrPartitionException"}`, "status", "error"); err != nil {
		t.Fatal(err)
	}
	l := Live(u)
	if !strings.Contains(l.ReconcileError, "reconciliation") || !strings.Contains(l.ReconcileError, "UnknownTopicOrPartition") {
		t.Fatalf("both error fields must be visible, got %q", l.ReconcileError)
	}
	if l.UpgradeMode != "stateless" || l.SpecJobState != "running" {
		t.Fatalf("absent spec fields must take the CRD defaults, got %+v", l)
	}
}

func TestLiveReadsTheOperatorHealthCheckFlag(t *testing.T) {
	u := New()
	if err := unstructured.SetNestedField(u.Object, "true", "spec", "flinkConfiguration", "kubernetes.operator.cluster.health-check.enabled"); err != nil {
		t.Fatal(err)
	}
	if !Live(u).OperatorRestarts {
		t.Fatal("the health-check flag must be visible to the decider")
	}
}
