# Install

## Requirements

- Flink Kubernetes Operator 1.13 to 1.16, tested nightly on every combination with Flink 1.20, 2.0 and 2.2, and 2.3 on operator 1.16. Older operators are not tested.
- Kubernetes 1.30 or later for the admission policy the chart installs by default. On an older cluster the chart skips it by itself. Nothing else needs a recent version.
- A savepoint directory configured on the FlinkDeployment, `execution.checkpointing.savepoint-dir` on Flink 2.x or `state.savepoints.dir` on 1.x, and `upgradeMode: savepoint` or `last-state`. The controller never changes the upgrade mode. On `stateless` it refuses to suspend and says so in an Event, because a resume would replay the topic from the start.
- A Kafka credential, see Connecting to Kafka below.
- Network access from the controller to the Kafka brokers, the Kubernetes API server and, unless `config.flinkRest` is off, the JobManager pods on port 8081. The policy is below.
- One controller instance per namespace and Kafka cluster. Two in one namespace would share ConfigMap names and the leader lease.

## The chart

The image and the chart are published to the GitHub Container Registry on every tag.

    helm install siesta oci://ghcr.io/patrk/charts/flink-siesta --version 0.3.0 \
      --set kafka.bootstrapServers=... --set kafka.securityProtocol=SASL_SSL \
      --set kafka.sasl.existingSecret=kafka-auth

The chart's `image.tag` defaults to its `appVersion`, so the chart and the image always move together. On a locked-down cluster the values you will reach for are `image.digest`, `imagePullSecrets`, `podLabels`, `nodeSelector` and `tolerations`, `priorityClassName`, `serviceAccount.annotations` for workload identity, `admissionPolicy.enabled: false` when the tenant may not create cluster-scoped objects, and `extraObjects` for the network policy below, rendered through `tpl` so it can use the release name. Two replicas prefer different nodes by default.

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

A private CA goes in a Secret referenced by `kafka.tls.existingSecret` under the key `ca.crt`. The credential needs Describe on the topics you annotate, and Describe on the consumer group when `consumer-group` is set. On Confluent that is DeveloperRead on the topic prefix and on the group. The chart mounts the SASL Secret as files and the controller reads them on every new connection, so a rotated Secret is picked up as soon as the kubelet refreshes the mount and the next connection authenticates. When running the binary outside the chart with `KAFKA_SASL_USERNAME` and `KAFKA_SASL_PASSWORD` in the environment, a rotation still needs a restart.

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

Every knob has a default that fits a typical namespace. Change them through `config` in the chart values, or the matching flag. Three flags have no chart value and go through `extraArgs`: `--resume-stall-after`, `--flink-rest-port` and `--unrecoverable-patterns`.

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


## With Argo CD or Flux

Siesta sets `spec.job.state` and two annotations on FlinkDeployments, using the field manager `siesta`. A GitOps tool that owns those fields sees drift and, with self-heal on, reverts it. There are three ways to avoid that, best first.

1. Do not declare `spec.job.state` in Git. The CRD defaults it to `running`, so the manifest deploys the same, and Argo CD and Flux only diff the fields their manifests declare, so a field left out of Git is never seen as drift.
2. Argo CD, ignore by manager. No paths to list.

       spec:
         ignoreDifferences:
           - group: flink.apache.org
             kind: FlinkDeployment
             managedFieldsManagers: [siesta]
         syncPolicy:
           syncOptions: [RespectIgnoreDifferences=true]

3. Argo CD, ignore by path, if you cannot use managed fields. Put `/spec/job/state`, `/metadata/annotations/siesta.flink.io~1state` and `/metadata/annotations/siesta.flink.io~1reason` under `jsonPointers`.

The ConfigMaps Siesta creates are owned by the deployment and labelled `app.kubernetes.io/managed-by: flink-siesta`. GitOps tools ignore objects they did not apply.

## Least privilege

RBAC grants `patch` on FlinkDeployments, and RBAC cannot be narrowed to fields. The chart therefore ships a `ValidatingAdmissionPolicy`, on by default on Kubernetes 1.30 and later and skipped on older clusters, that rejects any write from the controller's ServiceAccount which changes the image, the Flink version, the service account, the Flink configuration, the jar, entry class, arguments, parallelism, upgrade mode or initial savepoint of the job, the pod template, or the JobManager and TaskManager specs. A compromised controller could suspend and resume jobs. It could not change their image, jar, configuration or resources. Set `admissionPolicy.enabled: false` when the tenant may not create cluster-scoped objects.

Release images and charts carry SLSA provenance and an SBOM, and they are signed with cosign, keyless, by the release workflow's identity. The release notes show the verify command.

## Upgrading

From 0.2.x, `helm upgrade` is enough. The Role gains `patch` on events, the admission objects are renamed to include the namespace, and memory in the ConfigMaps is read as before.

From 0.1.x: 0.1 kept its memory in annotations on the FlinkDeployment, and 0.3.0 neither reads nor removes them. Remove them once before upgrading, and every job starts with a fresh idle window. A job that 0.1 had suspended stays suspended and is resumed on the next record, as before.

    kubectl annotate flinkdeployments --all -n <namespace> \
      siesta.flink.io/offsets- siesta.flink.io/restarts- siesta.flink.io/last-activity-at- \
      siesta.flink.io/suspended-at- siesta.flink.io/awake-since-
