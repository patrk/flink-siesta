# 5. One merge patch carries the spec change and the state together

**Context.** A transition changes `spec.job.state` and records new state annotations. Two writes leave a window where the job is suspended but the controller does not remember doing it.

**Decision.** Every transition is exactly one JSON merge patch with both `metadata.annotations`
and `spec`. Empty annotation values are sent as JSON null so a merge patch removes the key.

**Amended by ADR 10.** Since memory moved to a ConfigMap, a transition is two writes: the
object first, memory second. The order is what keeps the guarantee: if the second write is
lost, the next tick repairs memory from the object's state annotation, and for a restart from
the `restartNonce` the budget recorded before patching. Every patch is also conditional on the
resourceVersion that was read, so a concurrent edit conflicts instead of being overwritten.

**Consequences.** A crash between "decided" and "recorded" cannot happen. Merge patches
are idempotent, so a retried reconcile is harmless. The controller does not use server-side apply, because it deliberately does not want to own any spec field for longer than one patch.
