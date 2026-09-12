# 6. The controller never changes `upgradeMode`

**Context.** We had the suspend patch set `spec.job.upgradeMode: savepoint` so the operator
would take a savepoint on suspend. That silently changed how the deployment behaves on every
later rollout too, and on a `stateless` deployment a resume would have replayed the topic from
the beginning.

**Decision.** The controller writes `spec.job.state` and its own annotations.
It suspends only deployments whose `upgradeMode` is `savepoint` or `last-state`, the two modes
under which the operator keeps the job's position. On `stateless` it does not act and raises a
Warning event saying why. Choosing the upgrade mode stays with whoever owns the deployment.

A new `metadata.generation`, meaning someone edited the spec, clears `unrecoverable` and the
restart budget: the edit is the human saying "try again".

**Consequences.** The controller cannot change the semantics of a rollout it did not make.
Users of stateless deployments get a clear message instead of a replay. One more field read
from the object, `upgradeMode`, and one more annotation, `generation`.
