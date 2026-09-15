# Operations

## Reading a namespace

`kubectl describe flinkdeployment <name>` shows the state, the reason and the transition events. `kubectl get cm siesta-<name> -o yaml` shows the numbers behind them. For the whole namespace at a glance:

    kubectl get flinkdeployments -o custom-columns=\
    'NAME:.metadata.name,JOB:.status.jobStatus.state,SPEC:.spec.job.state,SIESTA:.metadata.annotations.siesta\.flink\.io/state,REASON:.metadata.annotations.siesta\.flink\.io/reason'

## Events

Every transition and every situation worth a human's attention is an Event on the FlinkDeployment. Kubernetes may fold a repeat of the same reason into the first event with a count, so read the count as well as the list.

| Event | When |
|---|---|
| `Suspended`, `Resumed`, `Restarted` | the controller changed `spec.job.state` or `spec.restartNonce`. The message is the reason |
| `Refused` | a `stateless` job became idle. Said once per reason, not once per minute, and counted as one transition. In a dry run no event is raised, but the reason reads `dry-run: would refuse` and `siesta_dry_run_would_act{action="refuse"}` lists the jobs that would sleep once they run with a savepoint-preserving upgrade mode |
| `Unrecoverable` | the restart budget is exhausted, or the failure matched an unrecoverable pattern. Fix the cause, then edit the FlinkDeployment spec: a new generation resets the state and the restart budget |
| `InvalidPolicy` | an annotation value the controller cannot read, said once. The job is left untouched until you fix it |
| `SourceUnreachable`, `SourceReachable` | the broker stopped answering, and answered again. One event per edge, never per tick, and nothing happens in between. The messages name when the outage began and how long it lasted |
| `SuspendedWithoutSavepoint` | the operator completed the suspend but the savepoint failed, so it fell back to its last checkpoint. The resume still works from that checkpoint. A common cause is that the operator asks for canonical savepoints by default and some operators cannot produce them, the Print sink on Flink 2.x for one. If the job only ever resumes on the same state backend, set `kubernetes.operator.savepoint.format.type: NATIVE` in its `flinkConfiguration` |
| `SuspendStalled`, `ResumeStalled` | the operator has not completed a suspend, or a resumed job is not RUNNING, after `--resume-stall-after`, ten minutes by default. Said once. The usual causes are a full cluster, a missing image or a savepoint that no longer restores |
| `SourcesVerified`, `SourcesDrift`, `SourcesUnverified` | once per job instance, the written `sources` compared with what the job's Kafka sources read. Never corrected |
| `SourcesLearned` | with `sources: auto`, the topics learned from the job and remembered |
| `Released` | the `mode` annotation was removed. The controller's annotations and ConfigMap are gone |
| `SuspendedFailed`, `ResumedFailed`, `RestartedFailed` | a patch on the FlinkDeployment was rejected. The message is the API server's error, and the tick is retried |
| `SaveFailed` | the controller could not write its memory. The next tick repairs it from the object |

Two things outside the events are worth knowing. If a deployment enables `kubernetes.operator.cluster.health-check.enabled`, Siesta leaves failed jobs to the operator and says `restart left to the operator's health check`, so the two never fight. And a suspended job has no JobManager: anything outside Kubernetes that polls the JobManager, a status page, a health check, a deployment service, will read a suspended job as gone. Such a system must key on `spec.job.state` or the operator's `lifecycleState`, where `suspended` and `SUSPENDED` are the intended state, not a failure.

## Metrics

Metrics are exposed on `:8080/metrics` next to controller-runtime's own reconcile and work-queue metrics. The chart creates a Service for them and, if you enable it, a ServiceMonitor. Any scraper works, whether plain Prometheus, the Prometheus operator, or an OpenTelemetry Collector with its `prometheus` receiver.

| Metric | Type | What it answers |
|---|---|---|
| `siesta_deployment_state{namespace,name,state}` | gauge | which deployments are suspended right now |
| `siesta_transitions_total{namespace,name,action}` | counter | how often the controller acts, and whether a job is flapping |
| `siesta_resume_latency_seconds` | histogram | how long from the input that woke a job until it reports RUNNING |
| `siesta_probe_errors_total{kind}` | counter | how often Kafka or the job's REST API could not be asked |
| `siesta_probe_duration_seconds{kind}` | histogram | how long each probe takes, by kind |
| `siesta_held_awake{namespace,name,gate}` | gauge | which gate keeps an idle job awake: min-awake, lag-unknown, lag-pending, job-unknown, job-busy |
| `siesta_suspended_seconds_total{namespace,name}` | counter | how long each job has slept, in total |
| `siesta_suspended_since_timestamp_seconds{namespace,name}` | gauge | when the current sleep began, 0 while awake |
| `siesta_released_cpu_cores{namespace,name}`, `siesta_released_memory_bytes{namespace,name}` | gauge | the JobManager and TaskManager footprint the cluster has back while a job sleeps, 0 while awake |
| `siesta_idle_seconds{namespace,name}` | gauge | seconds since the last input, for every managed job, awake or not |
| `siesta_dry_run_would_act{namespace,name,action}` | gauge | in dry-run, what the controller would do right now |

The last two turn a dry run on an existing namespace into an inventory: sort by `siesta_idle_seconds` and read `siesta_dry_run_would_act{action="suspend"}` to see which jobs would sleep, and `action="refuse"` to see which are idle but run `stateless`. The savings over a month are `sum(avg_over_time(siesta_released_cpu_cores[30d])) * 720` core-hours, and the same with memory.

Metrics describe what the controller did. Nothing in the controller reads them. They observe, they never decide. Two alerts are worth having, probe errors for ten minutes and any job marked unrecoverable. Both are in `docs/alerts.yaml`, and a dashboard for every metric above is in `docs/grafana/`.

