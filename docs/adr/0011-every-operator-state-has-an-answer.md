# 11. Every operator state has a defined answer

**Context.** The Flink operator reports a lifecycle state (CREATED, DEPLOYED, STABLE,
SUSPENDED, UPGRADING, ROLLING_BACK, ROLLED_BACK, FAILED) and a Flink job state (CREATED,
RECONCILING, RUNNING, RESTARTING, FAILED, FINISHED, CANCELED). Combined with the spec's
job state and our own phase, that is a few hundred combinations. Guards added one incident
at a time leave the rest undefined.

**Decision.** The decider is specified by invariants that hold across the whole space, and a
test enumerates every combination to check them:

| Situation | Answer |
|---|---|
| Operator UPGRADING or ROLLING_BACK | wait, whatever else is true |
| Source could not be asked | wait; unknown is never idle and never activity |
| Our phase unrecoverable | wait for a human; a spec edit clears it |
| Spec says suspended but we think active | someone else suspended it: leave it, say so |
| Spec says running but we think suspended | someone else resumed it: treat as awake |
| STABLE and RUNNING, active, idle past the window, awake past min-awake | suspend |
| SUSPENDED, ours, input observed | resume |
| SUSPENDED, ours, no input | wait |
| Suspend patched, operator not yet SUSPENDED | wait; a resume now would race the savepoint |
| Job FAILED, or RESTARTING longer than the threshold | restart within budget, else unrecoverable |
| Resumed, not RUNNING after the stall window | warn once; keep waiting |
| Anything else (CREATED, DEPLOYED, ROLLED_BACK, FINISHED, CANCELED, ...) | wait, with a reason |

**Consequences.** A new operator lifecycle state is added to the test and the decider must
answer for it before the version is supported. "Wait" is the default for anything not listed,
so an unforeseen state costs at most a delay, never a wrong action.
