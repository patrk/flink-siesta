# 12. Learn from the running job, not only from the broker

**Context.** Until 0.2 the controller knew a job only through two hand-written annotations,
`sources` and `consumer-group`, and through Kafka. That has two weak spots. The `sources`
list can drift from what the job really reads, and then a suspend is decided on the wrong
topic. And "caught up" needs a consumer group with committed offsets, which needs
checkpointing and Describe on the group. The job itself knows both answers. While it runs,
its JobManager serves a REST API on `<deployment>-rest:8081`, and the Kafka source
publishes one metric per topic and partition under its operator scope, plus a
`pendingRecords` gauge, the number the operator's own autoscaler reads. These endpoints and
metrics are the same from Flink 1.20 through 2.2.

**Decision.** Two uses, one client, both off the critical path of the decision:

1. **Sources are verified, not derived.** Once per job instance the controller lists the
   source vertices' metrics, collects the topics named in them and compares the set with the
   `sources` annotation. A match raises `SourcesVerified` once, a mismatch `SourcesDrift`
   once, and a job that exposes no Kafka source at all is reported the same way. Nothing
   is changed: the annotation stays the contract, because a job can read a topic the user
   deliberately does not want to be woken by, and because a job that is suspended has no
   REST API to ask. Deriving would also make the wake signal depend on a value the
   controller could not read while the job sleeps.
2. **`lag: job` is a second way to say caught up.** With this annotation, idle additionally
   requires that the source vertices report zero `pendingRecords`. It needs no consumer
   group, no checkpointing and no group ACL. It can be combined with `consumer-group`, and
   then both must be zero. If the REST API cannot be reached, lag is unknown, and unknown
   never acts. The reason on the object says so, which is how a missing network policy
   shows up.

Everything the REST API is asked is read-only, and `--flink-rest=false` switches it off
entirely for clusters that will not open that path.

**Consequences.** The controller now dials a third destination, the JobManager Service in
its own namespace, and the README says which network policy line that is. One extra HTTP
round trip per tick for `lag: job` deployments, and a handful once per job instance for
the check. The verification is a Warning, not a refusal: a wrong `sources` list still
suspends, but now it is visible in `kubectl describe` before anyone is surprised.
