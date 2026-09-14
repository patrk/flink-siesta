# Changelog

## Unreleased, 0.3.0

- The `sources` annotation is verified against the running job's Kafka sources once per job instance: `SourcesVerified`, `SourcesDrift` or `SourcesUnverified` events. Never corrected.
- `idle: job` adds the running job's own view as a last gate, ADR 13: exact pending from the reader's emitted offsets against the broker's end offsets, plus the source's idle time. It may hold a suspend, never cause one or wake a job.
- `--flink-rest` and `--flink-rest-port` flags, `config.flinkRest` chart value. The controller now dials the operator's `<deployment>-rest` Service.
- `siesta_probe_errors_total{kind="flink-rest"}`.
- The e2e job now reads Kafka: a small DataStream job under `e2e/job`, built per Flink version. The e2e asserts `SourcesVerified`, `SourcesDrift` and a suspend held back by the job gate while a burst drains.

## 0.2.3

- Fix: the Role lacked `patch` on events. The events API folds an identical event within six minutes into a series on the first one, which is a patch, so every repeat of a reason with the same message was dropped. Symptoms: one `SourceUnreachable` for two outages, one `Suspended` for two suspends. Found by the e2e once its steps ran closer together.

## 0.2.2

- The `lag unknown` and `records pending` reasons now reach the object's `reason` annotation, as the README already claimed. Before, they were only logged.
- Chart: `podLabels` is rendered (it was declared and ignored), and `imagePullSecrets` and `priorityClassName` are new knobs. Clusters that mandate labels, a pull secret or a priority class on every pod can now install without a wrapper chart.

## 0.2.1

- Kafka credentials can be read from files and are read on every new connection, so a rotated Secret needs no restart. The chart mounts the SASL Secret as files.
- When a deployment enables the operator's own health-check restart, Siesta's restart budget steps back with a reason instead of competing.

## 0.2.0

- Idle also means caught up when a consumer group is known (`consumer-group` annotation).
- The controller never changes `upgradeMode` and refuses to suspend `stateless` deployments.
- Suspend only from STABLE, resume only from SUSPENDED: no races with the operator.
- Prometheus metrics, events on every transition, edge-triggered source reachability events.
- Controller memory in an owned ConfigMap; only `state` and `reason` on the deployment.
- Writes with the `siesta` field manager; a ValidatingAdmissionPolicy pins them to two fields.
- Signed, attested multi-arch releases. e2e on Flink 2.2, chaos and soak scenarios, nightly operator-by-Flink grid.

## 0.1.0

- First release: suspend on idle Kafka input, resume on the first record, restart budget.
