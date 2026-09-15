# Changelog

## Unreleased, 0.3.1

- Operator 1.16 and Flink 2.3 in the nightly grid, and the savings footprint reads the Kubernetes `resources` block that 1.16 allows on the pods, next to the operator's own `resource` fields.

## 0.3.0, 2026-09-16

- `sources: auto` learns the topics from the running job and remembers them, ADR 14. The written list stays the default.
- The job gate now lives in the decider on typed end offsets from the probe, so the reconciler only observes and acts, and the state-space test enumerates real readings.
- The controller no longer reads or removes the annotations 0.1.x wrote. The README's upgrade section has the one-line cleanup for a 0.1.x cluster.
- `idle: job` adds the running job's own view as a last gate, ADR 13: exact pending from the reader's emitted offsets against the broker's end offsets, plus the source's idle time. It may hold a suspend, never cause one or wake a job.
- The `sources` annotation is verified against the running job's Kafka sources once per job instance: `SourcesVerified`, `SourcesDrift` or `SourcesUnverified` events. Never corrected.
- `--flink-rest` and `--flink-rest-port` flags, `config.flinkRest` chart value. The controller now dials the operator's `<deployment>-rest` Service, and `siesta_probe_errors_total` has a `flink-rest` kind.
- Fix: a restart whose memory write was lost is now counted. The budget remembers the `restartNonce` it wrote, and a nonce on the object that memory does not know consumes a slot and starts a backoff. Before, a crash between the patch and the save let the next tick restart again for free.
- Fix: an `unrecoverable` mark that reached the object but not memory is repaired from the object, like suspend and resume already were.
- Fix: a ConfigMap timestamp that does not parse makes the whole memory unreadable instead of reading as the zero time, which could suspend a job on the spot.
- Fix: every patch is conditional on the resourceVersion that was read, so a concurrent edit conflicts and is retried instead of being overwritten.
- `Refused` is raised once per reason, not once per tick, and counts as one transition.
- Removing the `mode` annotation releases the deployment: annotations and ConfigMap are removed, with a `Released` event.
- `SuspendStalled` mirrors `ResumeStalled`. `SaveFailed` reports a memory write that failed.
- Outage events name when the outage began and how long it lasted. A recreated topic, whose offsets start at 0 again, counts as input and resumes a sleeping job.
- `bootstrap-servers` is reserved in the annotation contract and refused with `InvalidPolicy` until implemented.
- Savings and inventory metrics: `siesta_suspended_seconds_total`, `siesta_suspended_since_timestamp_seconds`, `siesta_released_cpu_cores`, `siesta_released_memory_bytes`, `siesta_idle_seconds` and, in dry-run, `siesta_dry_run_would_act`. A dry run on an existing namespace now yields an inventory and a savings estimate without reading events.
- `siesta_held_awake{gate}` names the gate keeping an idle job awake, `siesta_probe_duration_seconds{kind}` times each probe, and the `decided` log line carries every gate's input.
- A Grafana dashboard under `docs/grafana/` and two alert rules in `docs/alerts.yaml`. a `kubectl get` line for the namespace at a glance.
- Chart: the ValidatingAdmissionPolicy and its binding carry the release namespace in their name, so two installs in two namespaces no longer collide.
- Chart: `image.digest` and `image.pullPolicy`, `serviceAccount.annotations`, `extraArgs`, `extraVolumes` and `extraVolumeMounts`, node anti-affinity by default above one replica, and `extraObjects` rendered through `tpl`.
- The e2e is five standalone scenarios under `e2e/scenarios` on a shared `e2e/lib.sh`, each in its own namespace. Locally `E2E_PARALLEL` says how many run at once, CI and the nightly grid run them on separate clusters, and one reruns alone in minutes.
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
- Controller memory in an owned ConfigMap. only `state` and `reason` on the deployment.
- Writes with the `siesta` field manager. a ValidatingAdmissionPolicy pins them to two fields.
- Signed, attested multi-arch releases. e2e on Flink 2.2, chaos and soak scenarios, nightly operator-by-Flink grid.

## 0.1.0

- First release: suspend on idle Kafka input, resume on the first record, restart budget.
