# Changelog

## Unreleased, 0.3.0

- The `sources` annotation is verified against the running job's Kafka sources once per job instance: `SourcesVerified`, `SourcesDrift` or `SourcesUnverified` events. Never corrected.
- `lag: job` makes idle also require zero `pendingRecords` as the job reports it, with or without a consumer group.
- `--flink-rest` and `--flink-rest-port` flags, `config.flinkRest` chart value. The controller now dials the operator's `<deployment>-rest` Service.
- `siesta_probe_errors_total{kind="flink-rest"}`.

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
