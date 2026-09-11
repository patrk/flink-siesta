# 5. One merge patch carries the spec change and the state together

**Context.** A transition changes `spec.job.state` and records new state annotations. Two writes leave a window where the job is suspended but the controller does not remember doing it.

**Decision.** Every transition is exactly one JSON merge patch with both `metadata.annotations`
and `spec`. Empty annotation values are sent as JSON null so a merge patch removes the key.

**Consequences.** A crash between "decided" and "recorded" cannot happen. Merge patches
are idempotent, so a retried reconcile is harmless. The controller does not use server-side apply, because it deliberately does not want to own any spec field for longer than one patch.
