package flink

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/patrk/flink-siesta/internal/decide"
)

var GVK = schema.GroupVersionKind{Group: "flink.apache.org", Version: "v1beta1", Kind: "FlinkDeployment"}

func New() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(GVK)
	return u
}

// Live maps status into the decider's view. Missing fields become "", which the decider treats as unknown.
func Live(u *unstructured.Unstructured) decide.Live {
	specState, _, _ := unstructured.NestedString(u.Object, "spec", "job", "state")
	if specState == "" {
		specState = "running" // the operator's default when the field is absent
	}
	jobState, _, _ := unstructured.NestedString(u.Object, "status", "jobStatus", "state")
	jobID, _, _ := unstructured.NestedString(u.Object, "status", "jobStatus", "jobId")
	lifecycle, _, _ := unstructured.NestedString(u.Object, "status", "lifecycleState")
	// The operator reports reconciliation problems in status.reconciliationStatus.error and the
	// job's own failure, as a serialized throwable, in status.error. Patterns match either.
	recErr, _, _ := unstructured.NestedString(u.Object, "status", "reconciliationStatus", "error")
	jobErr, _, _ := unstructured.NestedString(u.Object, "status", "error")
	if jobErr != "" {
		recErr = recErr + " " + jobErr
	}
	upgradeMode, _, _ := unstructured.NestedString(u.Object, "spec", "job", "upgradeMode")
	if upgradeMode == "" {
		upgradeMode = "stateless" // the CRD default
	}
	savepoint, _, _ := unstructured.NestedString(u.Object, "status", "jobStatus", "upgradeSavepointPath")
	healthCheck, _, _ := unstructured.NestedString(u.Object, "spec", "flinkConfiguration", "kubernetes.operator.cluster.health-check.enabled")
	nonce, _, _ := unstructured.NestedInt64(u.Object, "spec", "restartNonce")
	return decide.Live{
		SpecJobState:     specState,
		UpgradeMode:      upgradeMode,
		SavepointPath:    savepoint,
		OperatorRestarts: healthCheck == "true",
		Generation:       u.GetGeneration(),
		RestartNonce:     nonce,
		JobID:            jobID,
		JobState:         jobState,
		LifecycleState:   lifecycle,
		ReconcileError:   recErr,
	}
}
