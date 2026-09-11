#!/usr/bin/env bash
# End-to-end: KinD + Flink operator + single-node Kafka + the controller.
# Asserts: idle -> suspended; produce -> running
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
ns=default
ctx=${KUBE_CONTEXT:-kind-siesta}
k() { kubectl --context "$ctx" "$@"; }

wait_for() { # $1 = jsonpath expr, $2 = expected, $3 = timeout s
  local i=0; until [ "$(k -n $ns get flinkdeployment example -o jsonpath="$1" 2>/dev/null)" = "$2" ]; do
    i=$((i+5)); [ $i -ge "$3" ] && { echo "timeout waiting for $1 = $2"; k -n $ns describe flinkdeployment example | tail -30; exit 1; }
    sleep 5; done; }

echo "clean up any previous run"
k -n $ns delete flinkdeployment example --ignore-not-found --wait=true
helm --kube-context "$ctx" uninstall siesta -n $ns 2>/dev/null || true

k -n $ns apply -f "$here/storage.yaml" -f "$here/kafka.yaml"
k -n $ns rollout status deploy/kafka --timeout=120s
until k -n $ns exec deploy/kafka -- /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; do sleep 2; done
k -n $ns exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic e2e-in --partitions 1

k -n $ns apply -f "$here/flinkdeployment.yaml"
wait_for '{.status.jobStatus.state}' RUNNING 300

helm --kube-context "$ctx" upgrade --install siesta "$here/../helm/flink-siesta" -n $ns \
  --set image.repository="${IMAGE_REPO:-siesta}" --set image.tag="${IMAGE_TAG:-e2e}" \
  --set kafka.bootstrapServers=kafka.$ns.svc:9092 --set replicaCount=1
k -n $ns rollout status deploy/siesta --timeout=120s

echo "expect suspend after idle-after (2m) + min-awake (30s)"
wait_for '{.spec.job.state}' suspended 360
wait_for '{.metadata.annotations.siesta\.flink\.io/state}' suspended 60

echo "produce one record, expect resume within ~1 min"
k -n $ns exec deploy/kafka -- sh -c 'echo hello | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic e2e-in'
wait_for '{.spec.job.state}' running 120
wait_for '{.status.jobStatus.state}' RUNNING 300
k -n $ns get events --field-selector involvedObject.name=example,involvedObject.kind=FlinkDeployment -o custom-columns=TIME:.metadata.creationTimestamp,REASON:.reason,MESSAGE:.message | tail -6
echo "e2e OK"
