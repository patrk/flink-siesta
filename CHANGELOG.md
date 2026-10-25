# Changelog

## Unreleased

- Fix: the FlinkDeployment was read from the API server on every tick, not from the watch's cache, because controller-runtime serves unstructured objects from its cache only when told to. Every tick cost two requests instead of one, which capped a pass at 20 deployments a second under the client's rate limit.
- Fix: controller-runtime's priority queue ranks the objects of the initial list below requeues. Once the controller was saturated, the deployments requeued by their own ticks took every turn, and at 2,000 deployments 795 were never visited again after the first pass, so they never slept. The queue is first in, first out now.
- `--kube-api-qps` and `--kube-api-burst` default to 50 and 100. A quiet tick costs one request, so 50 carries about 2,500 deployments.
- `make e2e-load-test` runs N deployments with simulated status against one broker on KinD. At 2,000: a first pass in 78 s, every deployment visited every 60 s, 33.5 API requests a second, all asleep 397 s after install, 200 resumed within 69 s of their records, 162 MiB. It found both fixes above.
- OAUTHBEARER for Kafka, two ways: a token file read on every authentication, such as a projected ServiceAccount token for a cluster that trusts this cluster's OIDC issuer, which the chart can mount; or the client credentials grant against an identity provider, the token reused until a minute before it expires. SASL extensions for Confluent Cloud's `logicalCluster` and `identityPoolId`. Tested against an Apache Kafka broker.
- The Kafka CA, client certificate and key are read again for every new connection, so a certificate rotated by cert-manager needs no restart. They were read once at startup.
- `--metrics-secure`, chart value `metrics.secure`: metrics over HTTPS, only for callers whose token the Kubernetes API authenticates and authorizes for `GET /metrics`. Off by default. Done with a TokenReview and a SubjectAccessReview instead of controller-runtime's filters, which would have grown the binary from 34 to 55 MB.
- Fix: every sleep of a `last-state` job raised `SuspendedWithoutSavepoint`. The operator takes no savepoint for a `last-state` suspend by design. Found by the new `last-state` e2e scenario, which also checks the resume restores from the last checkpoint.
- The chart adds a PodDisruptionBudget with more than one replica, and an optional NetworkPolicy.
- Base images pinned by digest, Dependabot for them and the Go modules, `govulncheck` on every pull request and every night, `setup-envtest` pinned, `golang.org/x/net` 0.60.0, and a `SECURITY.md`.
- `docs/roadmap.md` lists what is known and not built yet.
- Fix: with transactional producers, `idle: job` never let a job sleep. Each transaction ends in a commit marker, which takes an offset no reader is given, so the gate always counted one record pending. For a quiet job short of the end offset, the controller now asks the broker whether anything a consumer is given lies in between: markers, aborted transactions and compacted-away offsets do not count, an open transaction does. Tested against a real broker and, in the `job-graph` e2e scenario, a transactional producer.
- Fix: a metrics read for a source with more than about 50 partitions did not fit the 4096-byte request line Flink's REST server accepts, which answers 404, so `idle: job` held such a job with `job unknown` for good. The read is now split across requests.
- Fix: the unrecoverable patterns were matched on every tick, also against an old `status.error` on a job that had recovered and was running, which marked it unrecoverable and kept it awake. They are now matched only with `restart` on and on a job that is not STABLE and RUNNING.
- Fix: a tick that read the deployment from a stale cache right after siesta's own suspend could write `state: active` over the suspend, after which the job counted as suspended outside siesta and input no longer woke it. A write is no longer retried when `spec.job.state` changed since the read.
- Fix: with `sources: auto`, a job whose ConfigMap was lost while it slept had nothing to watch and slept for good. It is now resumed so its topics can be learned again, or, resuming from group offsets, held with a reason, since its group's offsets cannot be checked without them. `sources-unknown` in `siesta_held_awake`.
- Fix: `sources: auto` before anything was learned raised `SourceUnreachable`, though nothing had been asked.
- The `dry-run` annotation records what the controller would do to one job without doing it, as `config.dryRun` does for all.
- Durations take days followed by a Go duration, such as `1d12h`.
- `--max-concurrent-reconciles`, 4 by default, reconciles that many deployments at once. It was one, so a pass over a thousand deployments overran the poll interval, and during a Kafka outage each waited out `--probe-timeout` in turn. Chart value `config.maxConcurrentReconciles`.
- `--kube-api-qps` and `--kube-api-burst`, 20 and 30, set the client's rate limit towards the Kubernetes API, so it can be raised with the workers. Chart values under `config.kubeAPI`.
- The consumer-group lag is measured against the end offsets the tick already read, instead of asking the broker for them a second time.
- A tick reads its ConfigMap once. The save compared against a second, uncached read before.

## 0.4.1, 2026-10-24

- Fix: a job seen RESTARTING was restarted on the spot once it had been awake longer than `--failing-after`, because the time was counted from its last resume or restart. Flink's own restart strategy passes through RESTARTING for seconds, so one poll catching it cost a redeploy and a slot of the budget. RESTARTING now counts as failing once it has lasted `--failing-after`.
- Fix: clearing an unrecoverable deployment with a spec edit counted siesta's own last restart again, so the retry started with one slot of the budget used.
- Fix: the Kafka reads and the job's REST calls shared one `--probe-timeout`. A JobManager whose REST port swallowed connections used it up, and every job reported `SourceUnreachable` and stayed awake. Each now has its own.
- Fix: removing the `mode` annotation from a job siesta had suspended left it suspended for good, with nothing left on it to say why. It is now resumed first, once the operator has finished suspending it, then released.
- Fix: negative durations were accepted, and a `d` value too large to count wrapped around to a negative one. A negative `idle-after` called every job idle. They now raise `InvalidPolicy`, and so does `idle-after: 0`.
- Fix: `SiestaJobUnrecoverable` alerted on the transition counter, which Prometheus sees no increase in when its series starts at 1, so the first job marked unrecoverable never fired it, and it cleared after an hour. It now alerts on `siesta_deployment_state{state="unrecoverable"}`.
- `docs/install.md` no longer says every `stateless` job is refused, and the chart in the repository carries the released version instead of 0.1.0, so `helm install` from a checkout runs a published image.
- The admission policy now pins every field of the spec but `spec.job.state` and `spec.restartNonce`, and every annotation outside the controller's prefix, labels, finalizers and owner references. It named a few fields before, so the controller's identity could still change `spec.ingress`, `spec.mode` or `allowNonRestoredState`.
- A second admission policy keeps the controller's ConfigMap writes to its own, named `siesta-<deployment>` and labelled `app.kubernetes.io/managed-by: flink-siesta` before and after the write. Its Role covers every ConfigMap in the namespace, the Flink HA ConfigMaps among them.
- The release job pins its actions by commit and passes the tag to the shell through the environment, refusing anything but `vX.Y.Z`. The verify command in the release notes accepts only the release workflow's identity. Dependabot keeps the actions current.

## 0.4.0, 2026-10-24

- `resume-from: group-offsets` lets a `stateless` job sleep when its Kafka source resumes from the consumer group's committed offsets, ADR 15. It needs `consumer-group`, and the job sleeps only at lag zero.
- `max-sleep`, six days by default, wakes such a job before the broker expires its group's offsets. It commits, and sleeps again after `min-awake` without a new idle window.
- A resume whose group has no committed offsets is held with a `ResumeHeld` Warning instead of starting the source from its reset strategy. The held input still wakes the job once the offsets are back.
- Fix: input that arrived while the operator was still suspending was absorbed into memory, so the job slept on until the next record. It now resumes the job once the suspend completes.
- Fix: `SuspendedWithoutSavepoint` was raised on every sleep of a job that resumes from group offsets, which takes no savepoint by design.
- The `group-offsets` e2e scenario runs that mode against the real operator: a stateless job with auto-commit and no checkpoints, a broker that expires offsets after four minutes, two `max-sleep` wakes, and a held resume. The e2e job gains `--starting-offsets committed`, `--auto-commit-ms` and `--out`, a Kafka sink that makes a replay visible as a duplicate.
- `SiestaResumeHeld` in `docs/alerts.yaml`, and `offsets-missing` in `siesta_held_awake`.
- Run against the real operator by the `group-offsets` e2e scenario: a woken job commits again with no new records, so `max-sleep` resets the broker's expiry clock.
- The compatibility grid runs operator 1.16.1, and the e2e setup installs the operator chart from `archive.apache.org`, which keeps every release. `downloads.apache.org` drops a patch release when the next one ships, which had the grid failing since 1.16.1.
- Fix: a job restored from a savepoint could be verified on part of its graph, its old topics before the newly added one showed up, and report `SourcesVerified` instead of `SourcesDrift`. The topics now count once two reads 30 seconds apart agree. This was the `job-graph` flake in the grid.
- The Kafka integration test waits for the group coordinator instead of passing on its first transient error.

## 0.3.1, 2026-09-16

- The README is a two-minute read with a sketched timeline, drawn in code under `docs/illustration`. Concepts, install and operations moved to `docs/`, rewritten rather than moved, with the state and gate diagrams.
- Operator 1.16 and Flink 2.3 in the compatibility grid, and the savings footprint reads the Kubernetes `resources` block that 1.16 allows on the pods, next to the operator's own `resource` fields.
- The compatibility grid gates the release: a tag runs operator 1.13 to 1.16 by Flink, one box per operator, and publishes only when all of it passes. The grid also runs weekly and by hand. The nightly keeps chaos, soaks and the benchmark. CI builds the controller image and the job jar once per run instead of once per cluster, which is what had Maven Central refusing the runners.

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
