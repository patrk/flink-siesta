# 15. A stateless job may sleep when it resumes from its consumer group

**Context.** ADR 6 refuses `stateless` deployments because a stateless restart has no state to
restore, and a Kafka source without restored state could replay its topic. That is not true for
every source. One built with `OffsetsInitializer.committedOffsets(...)` starts from what its
consumer group last committed, and fleets exist that run this way on purpose: no checkpoints,
no savepoints, auto-commit on. For them the group's offsets are the position, and the refusal
excluded nearly every job.

Two things make it unsafe without care. The operator cancels a stateless job without a final
checkpoint, so anything not yet committed is read again. And the broker deletes a group's
offsets `offsets.retention.minutes` after the last commit, seven days by default and not
configurable on Confluent Cloud. A job that slept longer would start from its reset strategy.

**Decision.** `resume-from: group-offsets` is an opt-in that needs `consumer-group`, and it
applies only while `upgradeMode` is `stateless`. With it:

- The job is suspended under the usual gates, and the lag gate is always on: committed equals
  end offsets, so nothing is left to read again.
- `max-sleep`, six days by default, wakes the job without input so that it commits before the
  offsets expire. Such a wake keeps the time of the last input and is remembered in memory, so
  the job sleeps again after `min-awake` once the group is caught up, without a new idle window.
- Before a resume the controller makes, on input or max-sleep, the group must have a committed offset for every
  partition of the sources. If it has none, the resume is held with a `ResumeHeld` Warning
  and a reason on the object, and the input that asked for it is kept, so restoring the
  offsets is enough to wake the job. A held job can always be resumed by hand, and `mode: off`, a human decision, resumes without the check.

**Rejected.** Writing the offsets back from the controller's memory: at lag zero it knows them
exactly, but it would need write access to the group and make the controller responsible for
the job's data. Resuming anyway with a warning: a replay into the job's sinks cannot be undone,
a sleeping job can be woken. Setting `upgradeMode` for the user: ADR 6.

**Consequences.** Stateless jobs that resume from their group can sleep, at the cost of one
short wake per `max-sleep`. Windows, joins and other operator state are lost on every suspend,
as on any restart of such a job. A partition added while the job sleeps has no commit and
holds the resume like an expired one. The ConfigMap grows by one key, `refresh-wake`.
