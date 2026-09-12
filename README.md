<p align="center"><img src="docs/logo.png" alt="Flink Siesta: a dormice asleep around a pause button" width="180"></p>

# Flink Siesta

A Flink idle suspender. Focused on Kafka sources today, but other sources may follow behind the same interface.

Suspends idle FlinkDeployments when their Kafka input stops and resumes them on the
first new record, using the
Flink Kubernetes Operator `spec.job.state` and Kafka end offsets. Also enforces a persisted restart budget for failing jobs.

Works with any `FlinkDeployment` (application mode). The core only needs a
snapshot per source that changes when new input exists. Kafka end offsets ship
first, other probes (Pulsar, Kinesis, object-store prefixes) plug in behind one
interface. No CRD, no database, no metrics pipeline in the control path.

## Who it is for

Streaming jobs whose input is bursty or dormant for days: development and test
environments, per-tenant pipelines, change-data-capture from systems that only change during
business hours. Those jobs keep a JobManager and TaskManagers allocated around the clock for
nothing. Siesta takes them down after `idle-after` and brings them back on the first record.

It is not for latency-sensitive jobs. A resume takes the Flink operator's restore from
savepoint plus pod scheduling, one to a few minutes, on top of a poll interval of one minute.
If a job must react within seconds of the first record, do not suspend it.

Decisions are recorded in [`docs/adr/`](docs/adr/), one file per decision.

## How it works

1. You annotate a `FlinkDeployment` with the sources it consumes and an idle window.
2. Every 60 s the controller reads the log end offsets of those topics (one
   `AdminClient.listOffsets` call).
3. No movement for `idle-after`, and, if you gave it the job's consumer group, nothing left
   to consume -> `spec.job.state: suspended`. The operator takes a
   savepoint (your `upgradeMode` decides how; the controller never changes it) and tears
   the job down.
4. Offsets move -> `spec.job.state: running`. The operator restores from the
   savepoint, so the Kafka source resumes at the offsets it had.
5. All state (last offsets, timestamps, restart budget) lives in annotations on
   the same object. `kubectl describe` shows every decision and its inputs.

## Annotations

Prefix is configurable (`siesta.annotation-prefix`, default `siesta.flink.io`).

| Annotation | Written by | Value |
|---|---|---|
| `<prefix>/mode` | you | `auto` or `off` |
| `<prefix>/sources` | you | comma-separated source refs (for Kafka, topic names) |
| `<prefix>/source-type` | you | `kafka` (default). Other probes can be added without touching the core. |
| `<prefix>/consumer-group` | you | optional; when set, idle also requires the group to have consumed everything (lag 0) |
| `<prefix>/idle-after` | you | duration, e.g. `336h` |
| `<prefix>/min-awake` | you | duration, default `1h` |
| `<prefix>/restart` | you | `auto` or `off` |
| `<prefix>/state` | controller | `active`, `suspended`, `unrecoverable` |
| `<prefix>/reason` | controller | why it is in that state, or why it has not changed yet |

The controller's own memory (offsets snapshot, timestamps, restart budget) lives in a
ConfigMap named `siesta-<deployment>`, owned by the deployment, one readable key per field.
Debugging is two commands: `kubectl describe flinkdeployment <name>` for state, reason and
the transition events; `kubectl get cm siesta-<name> -o yaml` for the numbers behind them.

## Connecting to Kafka

The controller speaks Kafka's own vocabulary. Set these values (or the matching `KAFKA_*`
environment variables when running the binary directly):

| Setup | `securityProtocol` | `sasl.mechanism` | credentials |
|---|---|---|---|
| Local or in-cluster broker without auth | `PLAINTEXT` | | none |
| Confluent Cloud | `SASL_SSL` | `PLAIN` | API key as username, API secret as password |
| SCRAM-secured cluster | `SASL_SSL` | `SCRAM-SHA-256` or `SCRAM-SHA-512` | username and password |
| mTLS | `SSL` | | client certificate and key in a Secret, `tls.clientCert: true` |

Credentials come from Secrets you create; the chart never renders them into values:

    kubectl create secret generic kafka-auth --from-literal=username=APIKEY --from-literal=password=SECRET
    helm install siesta ./helm/flink-siesta \
      --set kafka.bootstrapServers=pkc-xxxxx.eu-central-1.aws.confluent.cloud:9092 \
      --set kafka.securityProtocol=SASL_SSL \
      --set kafka.sasl.existingSecret=kafka-auth

A private CA goes in a Secret referenced by `kafka.tls.existingSecret` under key `ca.crt`.
The credential needs Describe on the topics you annotate, and Describe on the consumer group
when `consumer-group` is set (Confluent RBAC: DeveloperRead on the topic prefix and on the
group). One controller instance talks to one Kafka cluster; run one instance per namespace
and cluster. `config.pollInterval` (default 1m) sets how often each deployment is visited,
`config.probeTimeout` (default 10s) bounds one Kafka call.

## Caveats

- **Processing time stops while suspended.** Processing-time timers and windows fire late,
  all at once, after a resume. Event-time jobs are unaffected. Suspend only jobs whose
  semantics survive a pause.
- **Only the declared sources are watched.** A job reading a second, non-Kafka source (a
  broadcast stream, a JDBC lookup) is not resumed by activity there. `sources` is written by
  hand and can drift from the job graph; keep it next to the job definition that owns it.
- **Clearing `unrecoverable`.** Fix the cause, then edit the FlinkDeployment spec. A new
  generation resets the state and the restart budget; a Warning event marks the change. Kafka
  going unreachable raises `SourceUnreachable` once and `SourceReachable` once it is back.
- **Lag needs checkpointing.** Flink commits consumer-group offsets only on checkpoints. With
  `consumer-group` set on a job that does not checkpoint, lag is never known and the job is
  never suspended; the reason says `lag unknown`.
- **Two restart mechanisms fight.** If the operator's own health-check restart
  (`kubernetes.operator.cluster.health-check.enabled`) is on, set `restart: off` here, or the
  other way round.
- **A suspend can complete without a savepoint.** If the savepoint fails, the operator falls
  back to its last checkpoint and reports the suspend as done. Siesta raises
  `SuspendedWithoutSavepoint` once; resume still works, from that checkpoint.
- **Manual changes are respected.** A job someone else suspends is left alone and marked
  `suspended outside siesta`; a job someone else resumes is treated as awake with a fresh
  idle window.
- **Invalid annotations are reported, not guessed.** A bad duration, an unknown mode, or a
  missing `sources` raises `InvalidPolicy` once and the job is left untouched until fixed.
- **Application mode only.** `FlinkSessionJob` is not managed. One controller instance per
  namespace; two instances in one namespace would share ConfigMap names and the leader lease.

## With Argo CD or Flux

Siesta sets `spec.job.state` and two annotations on FlinkDeployments, using the field
manager `siesta`. A GitOps tool that owns those fields sees drift and, with self-heal on,
reverts it. Three ways to avoid that, best first:

1. **Do not declare `spec.job.state` in Git.** The CRD defaults it to `running`, so the
   manifest deploys the same, and GitOps tools only detect drift on fields they manage. A
   field nobody declares belongs to whoever sets it. Works for Argo CD and Flux alike.
2. **Argo CD: ignore by manager.** No paths to list:

       spec:
         ignoreDifferences:
           - group: flink.apache.org
             kind: FlinkDeployment
             managedFieldsManagers: [siesta]
         syncPolicy:
           syncOptions: [RespectIgnoreDifferences=true]

3. **Argo CD: ignore by path**, if you cannot use managed fields:
   `/spec/job/state`, `/metadata/annotations/siesta.flink.io~1state`,
   `/metadata/annotations/siesta.flink.io~1reason` under `jsonPointers`.

The ConfigMaps Siesta creates are owned by the deployment and labelled
`app.kubernetes.io/managed-by: flink-siesta`; GitOps tools ignore objects they did not apply.

## Testing

    make test      pure decision table, state codec, store (fake client), no Docker
    make it        Kafka probe against Confluent's image and against Redpanda with SASL_SSL + SCRAM + TLS
    make envtest   reconciler on a real kube-apiserver with the FlinkDeployment CRD
    make e2e       KinD + Flink operator + Kafka: suspend, savepoint restore, resume, controller
                   restart mid-flight, Kafka outage events, restart budget, garbage collection
    make bench     one worker over 200 deployments on envtest, reports reconciles per minute

Every push runs the first four on Flink 2.2 and operator 1.15. A nightly workflow, also run on
tags, repeats the e2e on the full grid of operator 1.13, 1.14, 1.15 by Flink 1.20, 2.0, 2.2.

## Scale

One reconcile is one cached read of the deployment, one read of its ConfigMap, one or two
Kafka admin calls, and a write only when something changed. Every deployment is visited
once a minute. Tens to low hundreds of deployments per namespace run on the default single
worker with headroom; around a thousand, raise `MaxConcurrentReconciles` and the client
rate limit, or batch the Kafka calls per tick. `make bench` measures the loop itself: on an
Apple M4 Pro, one worker does 32,000 reconciles per minute against envtest with a fake probe,
1.9 ms each, so the API server and Kafka are the limits long before the controller is.
Horizontal scale is per namespace: one
instance, one Kafka cluster, one credential. Replicas exist for failover, not throughput;
leader election keeps one active.

## Metrics

Exposed on `:8080/metrics` next to controller-runtime's own reconcile and work-queue metrics.
The chart creates a Service for it and, opt-in, a ServiceMonitor. Any scraper works: plain
Prometheus, the Prometheus operator, or an OpenTelemetry Collector's `prometheus` receiver.

| Metric | Type | What it answers |
|---|---|---|
| `siesta_deployment_state{namespace,name,state}` | gauge | which deployments are suspended right now |
| `siesta_transitions_total{namespace,name,action}` | counter | how often the controller acts; a flapping job shows here |
| `siesta_resume_latency_seconds` | histogram | from the input that woke a job until it reports RUNNING |
| `siesta_probe_errors_total{kind}` | counter | how often Kafka could not be asked |

Metrics describe what the controller did. Nothing in the controller reads them: they observe,
they never decide. Two alerts worth having: `siesta_probe_errors_total` rising for ten
minutes, and any transition with `action="mark-unrecoverable"`.

## Requirements

- Flink Kubernetes Operator 1.10+ (uses `spec.job.state`, `upgradeMode: savepoint`,
  `status.jobStatus.upgradeSavepointPath`, `status.lifecycleState`).
- A savepoint directory configured on the FlinkDeployment (`execution.checkpointing.savepoint-dir`
  on Flink 2.x, `state.savepoints.dir` on 1.x), and `upgradeMode: savepoint` or
  `last-state`. The controller never changes the upgrade mode; on `stateless` it refuses to
  suspend and says so in an Event, because a resume would replay the topic from the start.
- A Kafka credential with DESCRIBE on the topics.

## Install

Image and chart are published to the GitHub Container Registry on every tag:

    helm install siesta oci://ghcr.io/patrk/charts/flink-siesta --version 0.1.0 \
      --set kafka.bootstrapServers=... --set kafka.securityProtocol=SASL_SSL \
      --set kafka.sasl.existingSecret=kafka-auth

The chart's `image.tag` defaults to the chart's `appVersion`, so chart and image versions move together.

## Try it

    make deps         # once: pin dependencies
    make test         # pure decision tests, no Docker, no cluster
    make it           # Kafka probe against a Testcontainers broker (Docker)
    make envtest      # controller against a local kube-apiserver with the FlinkDeployment CRD
    make kind-up e2e  # KinD + Flink operator + single-node Kafka, real suspend/resume

## Built with

Go, controller-runtime, franz-go. ~20 MB image, starts in under a second. The FlinkDeployment is
handled as an unstructured object: the CRD is external and only six fields are read.

## Status

0.2.x. The annotation contract above is stable; a change to it gets a new major version.
Proven end to end on KinD against the Flink Kubernetes Operator: suspend with savepoint,
resume from it, restart budget, unrecoverable marking. Not yet run at scale in anger; if you
do, an issue with your numbers is the most useful thing you can send.

## Logo

The mascot is a *Siebenschläfer*, dormouse: Berlin neighbours of Flink's squirrel that sleep
seven months a year. AI-generated original artwork. Not affiliated with the Apache Flink logo.

## License

Apache-2.0
