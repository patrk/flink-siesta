# 1. Policy and state live in annotations, not a new CRD

**Context.** The controller needs a place for user intent (idle window, sources, opt-out) and for its own memory (last snapshot, timestamps, restart budget). The obvious operator answer is a new CRD with spec and status.

**Decision.** Annotations on the existing FlinkDeployment, under one configurable prefix. Policy keys are written by whoever creates the FlinkDeployment; state keys, under the same prefix, are written only by the controller. The controller never writes the `.status` subresource, which the Flink operator owns.

**Consequences.** No CRD to install, `kubectl describe` on the one
object every operator already looks at shows the decision inputs next to the decision.
The cons: no schema validation on the values, and annotation size limits
define how many partitions one job may declare. Revisit when policy grows.
