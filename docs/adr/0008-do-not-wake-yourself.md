# 8. The controller does not wake itself, and does not write what it already knows

**Context.** Every reconcile wrote the state annotations, and every annotation write is an
update event on the object we watch. So each of our own patches triggered another reconcile,
and a busy topic, whose end offsets move every poll, produced one write per job per minute
carrying no new information.

**Decision.** Two filters. An update-event predicate lets a reconcile through only when the
spec generation, the status fields the decider reads, or annotations outside our prefix
changed; our own writes no longer wake us, the periodic requeue drives the poll. And for an
active job whose only change is "still busy", the activity timestamp is written back at
most every ten minutes.

**Consequences.** Kafka calls per job per minute drop from two to one. On a quiet cluster
the object is not touched at all. After a controller restart the persisted activity may be
up to ten minutes stale, so a job might be judged idle ten minutes early against a window
measured in days. Transitions, refusals and restarts are still written immediately.

*Amended by ADR 10: the write throttle is superseded by moving the controller's memory to a ConfigMap; the update-event predicate stays.*
