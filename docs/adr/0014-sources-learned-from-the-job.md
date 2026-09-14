# 14. Sources can be learned from the job, and are remembered

**Context.** ADR 12 verifies the hand-written `sources` annotation against the running job's
graph and refuses to derive it, for one reason: a suspended job has no JobManager, so a
controller that only knew the topics from the graph would have no wake signal while the job
sleeps. That reasoning holds. What changed is that the controller has memory (ADR 10), and
memory removes the objection: learn the topics while the job runs, remember them, and the
wake signal exists while it sleeps.

**Decision.** `sources: auto` is an additional value, not a changed meaning. With it:

- Once per job instance, the graph's Kafka topics are learned and written to the ConfigMap,
  with a `SourcesLearned` event naming them. The list replaces the previous one when it
  differs, so a job that changes its topics teaches the new list on its next run.
- The remembered list is the sources for everything else: the broker gate, the lag gate, the
  job gate, the reachability events. The decider never knows the difference.
- Before anything has been learned, the snapshot is unknown with the reason
  `sources: auto, waiting for the job to run once`, and unknown never acts. A job that
  exposes no Kafka source teaches nothing and is reported once as `SourcesUnverified`.
- `sources: auto` with the REST API switched off is an invalid policy, said once.

The hand-written list stays the default and the recommendation. It is the contract a human
wrote, it needs no REST access, and it lets a job read a topic it should not be woken by.

**Rejected.** Deriving without remembering, ADR 12's reason. Deriving at every tick, which
would make the wake signal depend on a value that changes under the controller's feet.
Merging learned and written lists, which would make it unclear which one a human reads.

**Consequences.** The one failure mode is a job whose topics change while it sleeps: it wakes
on the old list, runs, teaches the new one, and from then on is right. The README says so.
The ConfigMap grows by one key. Removing the policy removes the memory with it.
