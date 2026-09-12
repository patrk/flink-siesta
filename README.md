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

## How it works

1. You annotate a `FlinkDeployment` with the sources it consumes and an idle window.
2. Every 60 s the controller reads the log end offsets of those topics (one
   `AdminClient.listOffsets` call).
3. No movement for `idle-after` -> `spec.job.state: suspended`,
   `upgradeMode: savepoint`. The operator takes a savepoint and tears the job down.
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
| `<prefix>/idle-after` | you | duration, e.g. `336h` |
| `<prefix>/min-awake` | you | duration, default `1h` |
| `<prefix>/restart` | you | `auto` or `off` |
| `<prefix>/state` | controller | `active`, `suspended`, `unrecoverable` |
| `<prefix>/offsets` | controller | JSON map of source unit to opaque position; Kafka: `topic-partition` to end offset |
| `<prefix>/last-activity-at` | controller | RFC 3339 |
| `<prefix>/suspended-at`, `<prefix>/awake-since` | controller | RFC 3339 |
| `<prefix>/restarts` | controller | JSON `{count, windowStart, nextAfter}` |
| `<prefix>/reason` | controller | last transition reason |

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
The credential needs DESCRIBE on the topics you annotate (Confluent RBAC: DeveloperRead on
the topic or prefix). One controller instance talks to one Kafka cluster; run one instance
per namespace and cluster.

## Requirements

- Flink Kubernetes Operator 1.10+ (uses `spec.job.state`, `upgradeMode: savepoint`,
  `status.jobStatus.upgradeSavepointPath`, `status.lifecycleState`).
- `state.savepoints.dir` configured on the FlinkDeployment.
- A Kafka credential with DESCRIBE on the topics.

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

Initial development. Contract may change until first release version 0.1.0.

## Logo

The mascot is a *Siebenschläfer*, dormouse: Berlin neighbours of Flink's squirrel that sleep
seven months a year. AI-generated original artwork. Not affiliated with the Apache Flink logo.

## License

Apache-2.0
