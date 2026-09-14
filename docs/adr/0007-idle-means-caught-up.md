# 7. Idle means "nothing new" and, when we can tell, "nothing pending"

**Context.** ADR 3 defines activity as "the end offsets moved". That detects producers going
quiet, not the job finishing its work. A job still working through a backlog when its
producers stop would be suspended mid-backlog. Nothing is lost, the savepoint keeps the
position, but the backlog then waits until the next record arrives to wake the job.

**Decision.** An optional `consumer-group` annotation. When present, suspend additionally
requires that the group's committed offsets equal the end offsets on every partition, i.e.
zero lag. Flink commits offsets on each checkpoint, so this is reliable exactly when
checkpointing is on, which is also when a suspend is safe. Lag that cannot be measured, a
partition the group never committed, a fetch error, a probe without lag support, blocks the
suspend, consistent with "unknown never acts".

Without the annotation the ADR 3 behaviour stands and the README says so. The check is a
second, optional probe interface so that a source without consumer groups still fits.

**Consequences.** "Idle" has the meaning an operator expects when the group is known. Two
extra admin calls per reconcile for annotated deployments, the committed and the end offsets. Users must know their job's group
id; for Flink's Kafka source that is the `group.id` the job sets.
