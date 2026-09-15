<p align="center"><img src="docs/logo.png" alt="Flink Siesta: two dormice asleep around a pause button" width="180"></p>

# Flink Siesta

A Kubernetes controller that suspends idle Flink jobs and resumes them when data arrives.

The Flink Kubernetes Operator can suspend a running job and later restore it from its savepoint. You do that by setting `spec.job.state` to `suspended` and back to `running` on the FlinkDeployment, as described in the operator's [job management documentation](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/job-management/). It is a manual step. The operator has no notion of "this job has had no input for two weeks", and its autoscaler never goes below one running JobManager.

Siesta automates that step. It watches the Kafka topics a job consumes, sets `spec.job.state: suspended` once nothing has arrived for a configured idle window, and sets it back to `running` on the first new record. The operator does the actual work, the savepoint, the teardown and the restore. Siesta only decides when. It also restarts failing jobs within a persisted budget.

It works with any application-mode `FlinkDeployment` that reads Kafka. There is no CRD, no database and no metrics pipeline in the control path: two annotations on the deployment, one small ConfigMap next to it.

## Who it is for

Streaming jobs whose input is bursty or dormant for days. Development and test environments, per-tenant pipelines, change-data-capture from systems that only change during business hours. Those jobs keep a JobManager and TaskManagers allocated around the clock for nothing. Siesta takes them down after `idle-after` and brings them back on the first record.

It is not for latency-sensitive jobs. A resume takes the operator's restore from savepoint plus pod scheduling, which is one to a few minutes, on top of a poll interval of one minute. If a job must react within seconds of the first record, do not suspend it.

## Requirements

- Flink Kubernetes Operator 1.13 to 1.15, tested nightly on every combination with Flink 1.20, 2.0 and 2.2. Older operators from 1.10 should work, since only `spec.job.state`, `status.jobStatus.upgradeSavepointPath` and `status.lifecycleState` are read, but they are not tested.
- Kubernetes 1.30 or later for the admission policy the chart installs by default. On older clusters set `admissionPolicy.enabled: false`. Nothing else needs a recent version.
- A savepoint directory configured on the FlinkDeployment, `execution.checkpointing.savepoint-dir` on Flink 2.x or `state.savepoints.dir` on 1.x, and `upgradeMode: savepoint` or `last-state`. The controller never changes the upgrade mode. On `stateless` it refuses to suspend and says so in an Event, because a resume would replay the topic from the start.
- A Kafka credential with Describe on the topics, and on the consumer group if you use one.
- Network access from the controller to the Kafka brokers, the Kubernetes API server and, unless `config.flinkRest` is off, the JobManager pods on port 8081. The install section shows the policy.
- One controller instance per namespace and Kafka cluster. Two in one namespace would share ConfigMap names and the leader lease.

## Install

The image and the chart are published to the GitHub Container Registry on every tag.

    helm install siesta oci://ghcr.io/patrk/charts/flink-siesta --version 0.3.0 \
      --set kafka.bootstrapServers=... --set kafka.securityProtocol=SASL_SSL \
      --set kafka.sasl.existingSecret=kafka-auth

The chart's `image.tag` defaults to its `appVersion`, so the chart and the image always move together. On a locked-down cluster the values you will reach for are `image.digest`, `imagePullSecrets`, `podLabels`, `nodeSelector` and `tolerations`, `priorityClassName`, `serviceAccount.annotations` for workload identity, `admissionPolicy.enabled: false` when the tenant may not create cluster-scoped objects, and `extraObjects` for the network policy below, rendered through `tpl` so it can use the release name. Two replicas prefer different nodes by default.

### Your first sleeping job

Annotate one deployment. Nothing else changes about it.

    metadata:
      annotations:
        siesta.flink.io/mode: auto
        siesta.flink.io/sources: orders
        siesta.flink.io/idle-after: 2h

Two hours after the last record on `orders`, `kubectl describe flinkdeployment` shows `Suspended  no input for 2h0m0s` and the pods are gone. Produce one record, and within a minute or two it shows `Resumed  input observed` and the job continues from its savepoint. To watch without letting the controller act, install with `--set config.dryRun=true` first: the reason annotation then says what it would have done.

### Network policy

On a cluster with default-deny egress the controller needs three paths: the Kubernetes API server, the Kafka brokers, and the JobManager pods on port 8081 for the job graph and the job gate. The last one is the only line that is not obvious.

    apiVersion: networking.k8s.io/v1
    kind: NetworkPolicy
    metadata:
      name: siesta-to-jobmanagers
    spec:
      podSelector:
        matchLabels: { app.kubernetes.io/name: flink-siesta }
      policyTypes: [Egress]
      egress:
        - to:
            - podSelector:
                matchLabels: { type: flink-native-kubernetes }
          ports:
            - port: 8081

The API server and Kafka lines depend on your cluster and are usually the same ones your Flink jobs already have. With Cilium, `toEntities: [kube-apiserver]` and `toFQDNs` on the broker names.

### Defaults

Every knob has a default that fits a typical namespace. Change them through `config` in the chart values, or the matching flag.

| Setting | Default | What it does |
|---|---|---|
| `idle-after` (annotation) | `14d` | no input for this long before a suspend |
| `min-awake` (annotation) | `1h` | never suspend sooner than this after a resume or restart |
| `--poll-interval` | `1m` | how often each deployment is visited |
| `--probe-timeout` | `10s` | bound for the probes of one tick |
| `--restart-max`, `--restart-window`, `--restart-backoff` | `3`, `30m`, `1m` | restart budget for failing jobs, backoff doubles within the window |
| `--failing-after` | `10m` | RESTARTING longer than this counts as failing |
| `--resume-stall-after` | `10m` | warn once if a resume or a suspend has not completed after this |
| `--unrecoverable-patterns` | four substrings | errors that mean: never restart, mark unrecoverable |
| `--flink-rest`, `--flink-rest-port` | `true`, `8081` | ask the running job's REST API, and where |

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
| `<prefix>/sources` | you | comma-separated topic names, or `auto` to learn them from the running job and remember them |
| `<prefix>/consumer-group` | you | optional. When set, idle also means the group has consumed everything. |
| `<prefix>/idle` | you | optional. `job` adds the running job's own view as a last gate: it may hold a suspend while it still has records to emit or emitted one within the last poll interval. Needs no consumer group. |
| `<prefix>/idle-after` | you | a duration such as `336h` or `14d` |
| `<prefix>/min-awake` | you | a duration, default `1h` |
| `<prefix>/restart` | you | `auto` or `off` |
| `<prefix>/bootstrap-servers` | you | reserved for a per-deployment Kafka cluster, not implemented yet. Setting it raises `InvalidPolicy`, so that implementing it later breaks nothing. |
| `<prefix>/state` | controller | `active`, `suspended` or `unrecoverable` |
| `<prefix>/reason` | controller | why it is in that state, or why it has not changed yet |

The controller's own memory, the offsets per topic, timestamps and the restart budget, lives in a ConfigMap named `siesta-<deployment>`. It is owned by the deployment and has one readable key per field. Debugging takes two commands. `kubectl describe flinkdeployment <name>` shows the state, the reason and the transition events. `kubectl get cm siesta-<name> -o yaml` shows the numbers behind them.

For the whole namespace at a glance, one line:

    kubectl get flinkdeployments -o custom-columns=\
    'NAME:.metadata.name,JOB:.status.jobStatus.state,SPEC:.spec.job.state,SIESTA:.metadata.annotations.siesta\.flink\.io/state,REASON:.metadata.annotations.siesta\.flink\.io/reason'

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
- **Clearing `unrecoverable`.** Fix the cause, then edit the FlinkDeployment spec. A new generation resets the state and the restart budget.
- **Invalid annotations are reported, not guessed.** A bad duration, an unknown mode or a missing `sources` raises `InvalidPolicy` once, and the job is left untouched until you fix it.
- **Kafka outages are reported once.** When the broker becomes unreachable the controller raises `SourceUnreachable` a single time, does nothing until it is back, and then raises `SourceReachable` once. The messages name when the outage began and how long it lasted. Kubernetes may fold a repeat into the first event with a count, so read the count as well as the list.
- **A dry run on `stateless` jobs is still an inventory.** The refusal fires only when the idle criteria are met, so on a namespace whose jobs cannot be suspended yet, `Refused` events and `siesta_dry_run_would_act{action="refuse"}` list exactly the jobs that would sleep once they keep their position.
- **A suspended job has no JobManager.** Anything outside Kubernetes that polls the JobManager, a status page, a health check, a deployment service, will read a suspended job as gone. Such a system must key on `spec.job.state` or the operator's `lifecycleState`, where `suspended` and `SUSPENDED` are the intended state, not a failure.
- **A recreated topic counts as input.** Offsets are compared for equality, not for growth. A topic deleted and recreated starts at offset 0, which differs from what was remembered, so a suspended job reading it is resumed. It restores its savepoint and its Kafka source handles the out-of-range position with its reset strategy.
- **Credential rotation needs no restart.** The chart mounts the SASL Secret as files and the controller reads them on every new connection, so a rotated Secret is picked up as soon as the kubelet refreshes the mount and the next connection authenticates. When running the binary outside the chart with `KAFKA_SASL_USERNAME` and `KAFKA_SASL_PASSWORD` in the environment, a rotation still needs a restart.
- **The operator's own restart wins.** If a deployment enables `kubernetes.operator.cluster.health-check.enabled`, Siesta leaves failed jobs to the operator and says `restart left to the operator's health check`, so the two never fight.
- **A stalled resume or suspend is reported.** If a resumed job is not RUNNING after `--resume-stall-after`, ten minutes by default, `ResumeStalled` is raised once. The usual causes are a full cluster, a missing image or a savepoint that no longer restores. The mirror image, an operator that has not completed a suspend within the same window, raises `SuspendStalled` once.
- **A refusal is said once.** A `stateless` job that becomes idle gets one `Refused` event and one line in the reason, not one per minute, and counts as one transition.
- **Removing the policy releases the job.** Delete the `mode` annotation and the controller removes its two annotations and the ConfigMap, with a `Released` event. Nothing stale is left behind.
- **Sources can be learned instead of written.** With `sources: auto` the controller reads the running job's Kafka topics once per job instance, remembers them in the ConfigMap with a `SourcesLearned` event, and watches those. Until the job has run once under the controller nothing is known and nothing happens, and the reason says so. A job whose topics change while it sleeps wakes on the old list and teaches the new one on that run. The written list stays the default: it is a contract a human can read, and it lets a job read a topic it should not be woken by.
- **The sources annotation is checked against the job.** While a job runs, its JobManager knows which topics its Kafka sources read. Once per job instance the controller compares that with `sources` and raises `SourcesVerified`, `SourcesDrift` or, for a job without Kafka source metrics, `SourcesUnverified`. It never edits the annotation. A job may read a topic you do not want it woken by, and a sleeping job has no JobManager to ask.
- **The job may object, never decide.** With `idle: job`, the controller compares each declared partition's end offset with the last offset the job's reader emitted, which is the position a savepoint would record, and reads the source's own idle time. Records still to emit, or a record emitted within the last poll interval, hold the suspend with `job busy` on the object. An unreachable REST API holds it with `job unknown`, which is how a missing network policy shows up. Nothing in this view can wake a job, since a suspended job has no JobManager. This needs no consumer group, no checkpointing and no group ACL. Both features need egress from the controller to the JobManager pods on port 8081, the operator's `<deployment>-rest` Service. `config.flinkRest: false` switches them off.
- **A suspend without a savepoint is reported.** If the savepoint fails, the operator falls back to its last checkpoint and still reports the suspend as done. Siesta raises `SuspendedWithoutSavepoint` once, and the resume still works from that checkpoint. A common cause is that the operator asks for canonical savepoints by default and some operators cannot produce them, the Print sink on Flink 2.x for one. If the job only ever resumes on the same state backend, set `kubernetes.operator.savepoint.format.type: NATIVE` in its `flinkConfiguration`.

## Limits

These follow from what suspending a Flink job means, and Siesta cannot remove them.

- **Processing time stops while a job is suspended.** Processing-time timers and windows fire late, all at once, after a resume. Event-time jobs are unaffected. Only suspend jobs whose semantics survive a pause.
- **Group lag needs checkpointing.** Flink commits consumer-group offsets only on checkpoints. If you set `consumer-group` on a job that does not checkpoint, the lag is never known and the job is never suspended. The reason will say `lag unknown`. `idle: job` does not have this limit, but a job without checkpointing has no position to resume from anyway.
- **Application mode only.** `FlinkSessionJob` has no pods of its own to take down and no lifecycle state to key on.
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

## Least privilege

RBAC grants `patch` on FlinkDeployments, and RBAC cannot be narrowed to fields. The chart therefore ships a `ValidatingAdmissionPolicy`, on by default on Kubernetes 1.30 and later, that rejects any write from the controller's ServiceAccount which changes anything in `spec` other than `job.state` and `restartNonce`. A compromised controller could suspend and resume jobs. It could not change their image, jar, configuration or resources. Set `admissionPolicy.enabled: false` on clusters without that API.

Release images and charts carry SLSA provenance and an SBOM, and they are signed with cosign, keyless, by the release workflow's identity. The release notes show the verify command.

## Alternatives

The operator's own [autoscaler](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/autoscaler/) right-sizes a running job by vertex, and KEDA with the Kafka scaler can scale the TaskManagers of a job in [reactive mode](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/elastic_scaling/) by consumer lag. Both answer "how big should this job be while it runs". Neither reaches zero: the autoscaler keeps at least one running JobManager, and a reactive-mode job with no TaskManagers fails rather than pausing. KEDA cannot drive a FlinkDeployment directly either, because it thinks in replicas through the `/scale` subresource and a suspend is a savepoint followed by a teardown.

Siesta answers the other question, "should this job be running at all", and composes with both: a job can be autoscaled while awake and suspended while idle.

## Upgrading

- **From 0.2.x.** `helm upgrade` is enough. The Role gains `patch` on events, the admission objects are renamed to include the namespace, and memory in the ConfigMaps is read as before.
- **From 0.1.x.** 0.1 kept its memory in annotations on the FlinkDeployment. 0.3.0 neither reads nor removes them. Remove them once before upgrading, and every job starts with a fresh idle window:

      kubectl annotate flinkdeployments --all -n <namespace> \
        siesta.flink.io/offsets- siesta.flink.io/restarts- siesta.flink.io/last-activity-at- \
        siesta.flink.io/suspended-at- siesta.flink.io/awake-since- siesta.flink.io/generation- siesta.flink.io/resumed-at-

  A job that 0.1 had suspended stays suspended and is resumed on the next record, as before.

## Status

0.3.x is a well-tested beta. The decision model is small and covered by a table test, every transition has been run against the real operator, and the failure modes we could think of have events or tests. What it lacks is time. It has not yet run for weeks on a real cluster with real jobs. Run it in dry-run on a development namespace first, then live on non-critical jobs.

1.0 will mean thirty days on a real cluster with more than twenty jobs and no manual intervention, and the nightly grid, chaos and soak suites green for a month. Until then the annotation contract is stable, and any change to it bumps the major version.

## Development

Design decisions live in [`docs/adr/`](docs/adr/), one file per decision. How the suites work, from the pure decision table to the nightly grid, and what the controller costs at scale, is in [`docs/development.md`](docs/development.md).

    make test         # seconds, no Docker and no cluster
    make kind-up e2e  # a KinD cluster with the operator and Kafka, then the five scenarios

## Logo

The mascot is a Siebenschläfer, dormouse. They are Berlin neighbours of Flink's squirrel and sleep seven months a year. The artwork is AI-generated and original, and it is not affiliated with the Apache Flink logo.

## License

Apache-2.0
