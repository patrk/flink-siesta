# 13. The job may only object

**Context.** ADR 12 gave the controller a view from inside the running job. The first real run
showed what that view is worth and where it misleads. `pendingRecords` is the Kafka client's
`records-lag`, registered lazily on the first non-empty poll, and it counts records not yet
fetched into the client's buffer, not records not yet processed: a burst of 400 records read
0 pending while 151 were still being emitted. `sourceIdleTime` is the time since the source
last emitted a record, and it starts counting the moment the reader reports nothing
available, so an empty topic is idle within seconds and a job restart resets it. The
per-partition `currentOffset` gauge is the last emitted offset, which is exactly the
position a savepoint would record.

With two views of the same job, the question was how to combine them: merge, weigh, or
fall back. We wrote the invariants down and had the alternatives argued against them.

**Decision.** Gates, evaluated in a fixed order, each able to say only "not yet". The suspend
happens when every configured gate agrees. Any busy or unknown gate blocks and its reason is
written to the object. Nothing on the wake side changes: the broker's end offsets remain the
only wake signal, because a suspended job has no metrics to ask.

1. The operator is not busy (ADR 11).
2. The broker gate: end offsets unchanged for `idle-after`, awake for `min-awake` (ADR 3).
3. The deployment keeps its position, else the suspend is refused, once (ADR 6).
4. The lag gate, with `consumer-group`: committed offsets equal the end offsets (ADR 7).
5. The job gate, with `idle: job`, one gate with three values:
   - **idle**: every declared partition's `currentOffset` plus one equals the end offset the
     broker gate read this tick, and `sourceIdleTime` is at least one poll interval. A
     reader still at its initial offset counts as caught up only if it also reports idle,
     which is a reader that had nothing to read from where it started.
   - **busy**: records between the last emitted offset and the end offset, or a record
     emitted within the last poll interval.
   - **unknown**: the REST endpoint did not answer, the job is not RUNNING, or the job
     exposes no offset for a declared partition.

`pendingRecords` is not used. The exact comparison uses data the controller already holds
and has no lazy registration to wait for. It is the one place that reads the broker snapshot
as numbers rather than as an opaque value (ADR 3): the gate lives next to the reconciler, the
decider still only receives its three-valued answer.

**Rejected.** A weighted score lets a busy vote be outweighed and has no table to enumerate.
A fallback changes the rule silently when the REST endpoint is down, so the same job would
suspend under different conditions on different days. A majority lets two stale idle votes
beat a live busy one. An abstaining variant, where unknown does not block, degrades a
missing network policy into today's behaviour without anyone noticing, blocking with
`job unknown` on the object is the visible failure we want.

**Consequences.** `idle: job` is opt-in, so nothing changes for existing deployments. A job
restarted by the operator waits one poll interval, not a whole idle window, before the gate
can say idle. The worst remaining case is a non-checkpointing job without `idle: job` and
without a consumer group, whose reader is stuck with a fetched backlog: the broker sees no
change and the job is suspended mid-backlog. The backlog waits for the next produced
record. `idle: job` is the fix, and the README says so where it explains the annotation.
The party present in both states decides; the party present in one state may only object.
