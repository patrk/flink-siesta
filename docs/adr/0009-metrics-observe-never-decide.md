# 9. Metrics observe, they never decide

**Context.** Operators need to see what the controller does: which deployments are
suspended, how often it acts, how long a resume takes, whether it can reach Kafka. The
controller could also read metrics as an input, for example an input-rate metric to detect
idleness.

**Decision.** The controller exposes Prometheus metrics for its own decisions and actions, so
they are observable. It does not read metrics to make decisions. Decisions use only the
FlinkDeployment and one call to the source.

**Consequences.** Any scraper the cluster already runs works. The controller has no
dependency on the monitoring stack, so it keeps working when that stack does not.
