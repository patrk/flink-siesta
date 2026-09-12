# 10. State a human needs stays on the object; state only the controller needs lives next to it

**Context.** ADR 1 put all controller state in annotations on the FlinkDeployment. Some of
it changes every tick for a busy topic (the offsets snapshot, the activity timestamp), and
every write to the object is an update event for the Flink operator, which watches the same
object. ADR 8 reduced the writes; it could not remove them.

**Decision.** The deployment keeps the policy annotations plus `state` and `reason`, written
only when they change. Everything else, the offsets snapshot, timestamps, restart budget,
observed generation, pending records, lives in a ConfigMap named `siesta-<deployment>`,
owned by the deployment (garbage-collected with it), with one readable key per field, read
without a cache and watched by nobody. State written by earlier versions as annotations is
read once and migrated.

**Consequences.** The operator sees an external write only when a transition or a reason
change happens. Debugging is two commands: `kubectl describe
flinkdeployment` for state, reason and events; `kubectl get cm siesta-<name> -o yaml` for
the numbers behind them. The write throttle from ADR 8 is no longer needed and is gone;
persisted activity is exact again after a restart. One more object per deployment and one
more RBAC rule.
