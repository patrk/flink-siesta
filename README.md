<p align="center"><img src="docs/logo.png" alt="Flink Siesta: two dormice asleep around a pause button" width="180"></p>

# Flink Siesta

A Kubernetes controller that suspends idle Flink jobs and resumes them when data arrives. Focused on Kafka sources today, but other sources may follow behind the same interface.

The Flink Kubernetes Operator can suspend a running job and later restore it from its savepoint. You do that by setting `spec.job.state` to `suspended` and back to `running` on the FlinkDeployment, as described in the operator's [job management documentation](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/job-management/). It is a manual step. The operator has no notion of "this job has had no input for two weeks", and its autoscaler never goes below one running JobManager.

Siesta automates that step. It watches the Kafka topics a job consumes, sets `spec.job.state: suspended` once nothing has arrived for a configured idle window, and sets it back to `running` on the first new record. The operator does the actual work, the savepoint, the teardown and the restore. Siesta only decides when. It also restarts failing jobs within a persisted budget.

It works with any application-mode `FlinkDeployment`. The core only needs one thing from a source: a snapshot that changes when new input exists. Kafka end offsets ship first. Other probes, for Pulsar, Kinesis or object-store prefixes, plug in behind the same interface. There is no CRD, no database and no metrics pipeline in the control path.

## Alternatives

The operator's own [autoscaler](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/autoscaler/) right-sizes a running job by vertex, and KEDA with the Kafka scaler can scale the TaskManagers of a job in [reactive mode](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/elastic_scaling/) by consumer lag. Both answer "how big should this job be while it runs". Neither reaches zero: the autoscaler keeps at least one running JobManager, and a reactive-mode job with no TaskManagers fails rather than pausing. KEDA cannot drive a FlinkDeployment directly either, because it thinks in replicas through the `/scale` subresource and a suspend is a savepoint followed by a teardown.

Siesta answers the other question, "should this job be running at all", and composes with both: a job can be autoscaled while awake and suspended while idle.

## Who it is for

Streaming jobs whose input is bursty or dormant for days. Development and test environments, per-tenant pipelines, change-data-capture from systems that only change during business hours. Those jobs keep a JobManager and TaskManagers allocated around the clock for nothing. Siesta takes them down after `idle-after` and brings them back on the first record.

It is not for latency-sensitive jobs. A resume takes the operator's restore from savepoint plus pod scheduling, which is one to a few minutes, on top of a poll interval of one minute. If a job must react within seconds of the first record, do not suspend it.

Every design decision is recorded in [`docs/adr/`](docs/adr/), one file per decision.

## How it works

1. You annotate a `FlinkDeployment` with the topics it consumes and an idle window.
2. Once a minute the controller reads the end offsets of those topics with a single admin call.
3. When nothing has moved for `idle-after`, and the job has consumed everything if you gave it the consumer group, the controller sets `spec.job.state: suspended`. The operator takes a savepoint and tears the job down. Your `upgradeMode` decides how, and the controller never changes it.
4. When the offsets move, the controller sets `spec.job.state: running`. The operator restores from the savepoint, so the Kafka source continues exactly where it stopped.
5. The deployment itself carries only two annotations, its state and the reason. The controller keeps everything else it needs to remember in a small ConfigMap next to the deployment.

## Annotations

The prefix is configurable through `siesta.annotation-prefix` and defaults to `siesta.flink.io`.

| Annotation | Written by | Value |
|---|---|---|
| `<prefix>/mode` | you | `auto` or `off` |
| `<prefix>/sources` | you | comma-separated topic names |
| `<prefix>/source-type` | you | `kafka`, the default. Other probes can be added without touching the core. |
| `<prefix>/consumer-group` | you | optional. When set, idle also means the group has consumed everything. |
| `<prefix>/lag` | you | optional. `job` makes idle also mean the running job reports zero `pendingRecords` on its sources. Needs no consumer group. |
| `<prefix>/idle-after` | you | a duration such as `336h` or `14d` |
| `<prefix>/min-awake` | you | a duration, default `1h` |
| `<prefix>/restart` | you | `auto` or `off` |
| `<prefix>/state` | controller | `active`, `suspended` or `unrecoverable` |
| `<prefix>/reason` | controller | why it is in that state, or why it has not changed yet |

The controller's own memory, the offsets per topic, timestamps and the restart budget, lives in a ConfigMap named `siesta-<deployment>`. It is owned by the deployment and has one readable key per field. Debugging takes two commands. `kubectl describe flinkdeployment <name>` shows the state, the reason and the transition events. `kubectl get cm siesta-<name> -o yaml` shows the numbers behind them.

## Connecting to Kafka

The controller uses Kafka's own vocabulary. Set these chart values, or the matching `KAFKA_*` environment variables when you run the binary directly.

| Setup | `securityProtocol` | `sasl.mechanism` | credentials |
|---|---|---|---|
| Local or in-cluster broker without auth | `PLAINTEXT` | | none |
| Confluent Cloud | `SASL_SSL` | `PLAIN` | API key as username, API secret as password |
| SCRAM-secured cluster | `SASL_SSL` | `SCRAM-SHA-256` or `SCRAM-SHA-512` | username and password |
| mTLS | `SSL` | | client certificate and key in a Secret, `tls.clientCert: true` |

Credentials come from Secrets you create. The chart never renders them into values.

    kubectl create secret generic kafka-auth --from-literal=username=APIKEY --from-literal=password=SECRET
    helm install siesta ./helm/flink-siesta \
      --set kafka.bootstrapServers=pkc-xxxxx.eu-central-1.aws.confluent.cloud:9092 \
      --set kafka.securityProtocol=SASL_SSL \
      --set kafka.sasl.existingSecret=kafka-auth

A private CA goes in a Secret referenced by `kafka.tls.existingSecret` under the key `ca.crt`. The credential needs Describe on the topics you annotate, and Describe on the consumer group when `consumer-group` is set. On Confluent that is DeveloperRead on the topic prefix and on the group. One controller instance talks to one Kafka cluster, so run one instance per namespace and cluster. `config.pollInterval` sets how often each deployment is visited and defaults to one minute. `config.probeTimeout` bounds a single Kafka call and defaults to ten seconds.

## Behaviour worth knowing

- **Manual changes are respected.** A job someone else suspends is left alone and marked `suspended outside siesta`. A job someone else resumes is treated as awake with a fresh idle window.
- **Clearing `unrecoverable`.** Fix the cause, then edit the FlinkDeployment spec. A new generation resets the state and the restart budget, and a Warning event marks the change.
- **Invalid annotations are reported, not guessed.** A bad duration, an unknown mode or a missing `sources` raises `InvalidPolicy` once, and the job is left untouched until you fix it.
- **Kafka outages are reported once.** When the broker becomes unreachable the controller raises `SourceUnreachable` a single time, does nothing until it is back, and then raises `SourceReachable` once.
- **Credential rotation needs no restart.** The chart mounts the SASL Secret as files and the controller reads them on every new connection, so a rotated Secret is picked up as soon as the kubelet refreshes the mount and the next connection authenticates. When running the binary outside the chart with `KAFKA_SASL_USERNAME` and `KAFKA_SASL_PASSWORD` in the environment, a rotation still needs a restart.
- **The operator's own restart wins.** If a deployment enables `kubernetes.operator.cluster.health-check.enabled`, Siesta leaves failed jobs to the operator and says `restart left to the operator's health check`, so the two never fight.
- **A stalled resume is reported.** If a resumed job is not RUNNING after `--resume-stall-after`, ten minutes by default, `ResumeStalled` is raised once. The usual causes are a full cluster, a missing image or a savepoint that no longer restores.
- **The sources annotation is checked against the job.** While a job runs, its JobManager knows which topics its Kafka sources read. Once per job instance the controller compares that with `sources` and raises `SourcesVerified`, `SourcesDrift` or, for a job without Kafka source metrics, `SourcesUnverified`. It never edits the annotation. A job may read a topic you do not want it woken by, and a sleeping job has no JobManager to ask.
- **Caught up, as the job sees it.** `lag: job` reads the same `pendingRecords` gauge the operator's autoscaler uses, so it works without a consumer group, without checkpointing and without a group ACL. If the REST API is unreachable, lag is unknown and the job is not suspended. The reason says `lag unknown in the job`, which is how a missing network policy shows up. Both features need egress from the controller to the JobManager pods on port 8081, the operator's `<deployment>-rest` Service. `config.flinkRest: false` switches them off.
- **A suspend without a savepoint is reported.** If the savepoint fails, the operator falls back to its last checkpoint and still reports the suspend as done. Siesta raises `SuspendedWithoutSavepoint` once, and the resume still works from that checkpoint. A common cause is that the operator asks for canonical savepoints by default and some operators cannot produce them, the Print sink on Flink 2.x for one. If the job only ever resumes on the same state backend, set `kubernetes.operator.savepoint.format.type: NATIVE` in its `flinkConfiguration`.

## Limits

These follow from what suspending a Flink job means, and Siesta cannot remove them.

- **Processing time stops while a job is suspended.** Processing-time timers and windows fire late, all at once, after a resume. Event-time jobs are unaffected. Only suspend jobs whose semantics survive a pause.
- **Group lag needs checkpointing.** Flink commits consumer-group offsets only on checkpoints. If you set `consumer-group` on a job that does not checkpoint, the lag is never known and the job is never suspended. The reason will say `lag unknown`. `lag: job` does not have this limit, but a job without checkpointing has no position to resume from anyway.
- **Application mode only.** `FlinkSessionJob` has no pods of its own to take down and no lifecycle state to key on. Run one controller instance per namespace. Two instances in one namespace would share ConfigMap names and the leader lease.

- **Only the declared sources wake a job.** A job that also reads a non-Kafka source, such as a broadcast stream or a JDBC lookup, is not resumed by activity there. Drift between the annotation and the job's Kafka sources is reported, see above, but not corrected.

## With Argo CD or Flux

Siesta sets `spec.job.state` and two annotations on FlinkDeployments, using the field manager `siesta`. A GitOps tool that owns those fields sees drift and, with self-heal on, reverts it. There are three ways to avoid that, best first.

1. **Do not declare `spec.job.state` in Git.** The CRD defaults it to `running`, so the manifest deploys the same, and GitOps tools only detect drift on fields they manage. A field nobody declares belongs to whoever sets it. This works for Argo CD and Flux alike.
2. **Argo CD: ignore by manager.** No paths to list.

       spec:
         ignoreDifferences:
           - group: flink.apache.org
             kind: FlinkDeployment
             managedFieldsManagers: [siesta]
         syncPolicy:
           syncOptions: [RespectIgnoreDifferences=true]

3. **Argo CD: ignore by path**, if you cannot use managed fields. Put `/spec/job/state`, `/metadata/annotations/siesta.flink.io~1state` and `/metadata/annotations/siesta.flink.io~1reason` under `jsonPointers`.

The ConfigMaps Siesta creates are owned by the deployment and labelled `app.kubernetes.io/managed-by: flink-siesta`. GitOps tools ignore objects they did not apply.

## Testing

    make test      the pure decision table, the state codec and the store, no Docker needed
    make it        the Kafka probe against Confluent's image, and against Redpanda with SASL_SSL, SCRAM and TLS
    make envtest   the reconciler on a real kube-apiserver with the FlinkDeployment CRD
    make e2e       KinD with the Flink operator and Kafka: suspend, savepoint restore, resume,
                   a controller restart mid-flight, a Kafka outage, the restart budget, garbage collection
    make bench     one worker over 200 deployments on envtest, reports reconciles per minute
    make soak      a simulated soak: sources flapping, probes failing, writes dropped, a fake operator
                   reacting late, and the controller crashed every 400 ticks. Asserts consistency and
                   flat memory. 30 deployments over 20 simulated hours by default, which takes minutes.
                   The nightly runs SOAK_DEPLOYMENTS=100 SOAK_TICKS=3000.
    make e2e-chaos the leader killed mid-transition, and the operator away while we act
    make e2e-soak  45 real minutes on KinD: three jobs, random traffic, the controller killed every
                   10 minutes, one Kafka outage. Asserts consistency and flat RSS.

Every push runs the first four on Flink 2.2 and operator 1.15. A nightly workflow, which also runs on tags, repeats the e2e on the full grid of operator 1.13, 1.14 and 1.15 by Flink 1.20, 2.0 and 2.2, and runs the chaos and soak scenarios.

## Least privilege

RBAC grants `patch` on FlinkDeployments, and RBAC cannot be narrowed to fields. The chart therefore ships a `ValidatingAdmissionPolicy`, on by default on Kubernetes 1.30 and later, that rejects any write from the controller's ServiceAccount which changes anything in `spec` other than `job.state` and `restartNonce`. A compromised controller could suspend and resume jobs. It could not change their image, jar, configuration or resources. Set `admissionPolicy.enabled: false` on clusters without that API.

Release images and charts carry SLSA provenance and an SBOM, and they are signed with cosign, keyless, by the release workflow's identity. The release notes show the verify command.

## Scale

One reconcile is one cached read of the deployment, one read of its ConfigMap, one or two Kafka admin calls, and a write only when something changed. Every deployment is visited once a minute. Tens to low hundreds of deployments per namespace run on the default single worker with plenty of headroom. Around a thousand, raise `MaxConcurrentReconciles` and the client rate limit, or batch the Kafka calls per tick.

`make bench` measures the controller's own overhead, not capacity. It comes to about 2 ms per reconcile on an Apple M4 Pro, against a local envtest API server with a fake probe on the quiet path. Add your Kafka and API server round trips to that. With 10 ms to Kafka and 5 ms to the API server, one worker handles a few thousand deployments per minute, which is far beyond the population this is built for.

Horizontal scale is per namespace: one instance, one Kafka cluster, one credential. Replicas exist for failover, not throughput. Leader election keeps one active.

## Metrics

Metrics are exposed on `:8080/metrics` next to controller-runtime's own reconcile and work-queue metrics. The chart creates a Service for them and, if you enable it, a ServiceMonitor. Any scraper works, whether plain Prometheus, the Prometheus operator, or an OpenTelemetry Collector with its `prometheus` receiver.

| Metric | Type | What it answers |
|---|---|---|
| `siesta_deployment_state{namespace,name,state}` | gauge | which deployments are suspended right now |
| `siesta_transitions_total{namespace,name,action}` | counter | how often the controller acts, and whether a job is flapping |
| `siesta_resume_latency_seconds` | histogram | how long from the input that woke a job until it reports RUNNING |
| `siesta_probe_errors_total{kind}` | counter | how often Kafka could not be asked |

Metrics describe what the controller did. Nothing in the controller reads them. They observe, they never decide. Two alerts are worth having: `siesta_probe_errors_total` rising for ten minutes, and any transition with `action="mark-unrecoverable"`.

## Requirements

- Flink Kubernetes Operator 1.10 or later. Siesta uses `spec.job.state`, `status.jobStatus.upgradeSavepointPath` and `status.lifecycleState`.
- A savepoint directory configured on the FlinkDeployment, `execution.checkpointing.savepoint-dir` on Flink 2.x or `state.savepoints.dir` on 1.x, and `upgradeMode: savepoint` or `last-state`. The controller never changes the upgrade mode. On `stateless` it refuses to suspend and says so in an Event, because a resume would replay the topic from the start.
- A Kafka credential with Describe on the topics, and on the consumer group if you use one.
- Network access from the controller to the JobManager pods on port 8081, unless `config.flinkRest` is off. With Cilium that is one egress rule to the pods labelled `type: flink-native-kubernetes`.

## Install

The image and the chart are published to the GitHub Container Registry on every tag.

    helm install siesta oci://ghcr.io/patrk/charts/flink-siesta --version 0.2.2 \
      --set kafka.bootstrapServers=... --set kafka.securityProtocol=SASL_SSL \
      --set kafka.sasl.existingSecret=kafka-auth

The chart's `image.tag` defaults to its `appVersion`, so the chart and the image always move together.

## Try it

    make deps         # once, pins the dependencies
    make test         # the pure decision tests, no Docker and no cluster
    make it           # the Kafka probe against a Testcontainers broker, needs Docker
    make envtest      # the controller against a local kube-apiserver with the FlinkDeployment CRD
    make kind-up e2e  # KinD with the Flink operator and a single-node Kafka, real suspend and resume

## Built with

Go, controller-runtime and franz-go. The image is about 20 MB and starts in under a second. The FlinkDeployment is handled as an unstructured object, since the CRD is external and only a handful of fields are read.

## Status

0.2.x is a well-tested beta. The decision model is small and covered by a table test, every transition has been run against the real operator, and the failure modes we could think of have events or tests. What it lacks is time. It has not yet run for weeks on a real cluster with real jobs. Run it in dry-run on a development namespace first, then live on non-critical jobs.

1.0 will mean thirty days on a real cluster with more than twenty jobs and no manual intervention, the nightly version matrix green for a month, and a soak that kills the leader mid-transition. Until then the annotation contract is stable, and any change to it bumps the major version.

## Logo

The mascot is a Siebenschläfer, dormouse. They are Berlin neighbours of Flink's squirrel and sleep seven months a year. The artwork is AI-generated and original, and it is not affiliated with the Apache Flink logo.

## License

Apache-2.0
