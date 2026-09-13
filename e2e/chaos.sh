#!/usr/bin/env bash
# Chaos scenarios against the same KinD cluster: leader failover mid-transition, and the
# operator being away while we act. Run after e2e/run.sh has proven the happy path.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
ns=default
ctx=${KUBE_CONTEXT:-kind-siesta}
k() { kubectl --context "$ctx" "$@"; }
FLINK_VERSION=${FLINK_VERSION:-2.2}
render() {
  local out; out=$(mktemp)
  sed -e "s#image: flink:2.2#image: flink:${FLINK_VERSION}#" -e "s#flinkVersion: v2_2#flinkVersion: v${FLINK_VERSION/./_}#" "$1" > "$out"
  if [ "${FLINK_VERSION%%.*}" = 1 ]; then
    sed -i.bak -e 's#execution.checkpointing.savepoint-dir#state.savepoints.dir#' -e 's#execution.checkpointing.dir#state.checkpoints.dir#' "$out" && rm -f "$out.bak"
  fi
  echo "$out"
}
wait_for() { local i=0; until [ "$(k -n $ns get flinkdeployment example -o jsonpath="$1" 2>/dev/null)" = "$2" ]; do
  i=$((i+5)); [ $i -ge "$3" ] && { echo "timeout waiting for $1 = $2"; k -n $ns describe flinkdeployment example | tail -20; exit 1; }; sleep 5; done; }
events() { local uid; uid=$(k -n $ns get flinkdeployment example -o jsonpath='{.metadata.uid}'); k -n $ns get events --field-selector involvedObject.uid=$uid -o custom-columns=REASON:.reason,MESSAGE:.message; }

echo "clean up"
k -n $ns delete flinkdeployment example --ignore-not-found --wait=true
helm --kube-context "$ctx" uninstall siesta -n $ns 2>/dev/null || true
k -n $ns delete deploy kafka --ignore-not-found --wait=true
k -n $ns delete pvc flink-data kafka-data --ignore-not-found --wait=true
k -n $ns apply -f "$here/storage.yaml" -f "$here/kafka.yaml"
k -n $ns rollout status deploy/kafka --timeout=120s
until k -n $ns exec deploy/kafka -- /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; do sleep 2; done
k -n $ns exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic e2e-in --partitions 1
k -n $ns apply -f "$(render "$here/flinkdeployment.yaml")"
wait_for '{.status.jobStatus.state}' RUNNING 300

echo "chaos 1: two replicas; the leader dies right after it suspends"
helm --kube-context "$ctx" upgrade --install siesta "$here/../helm/flink-siesta" -n $ns \
  --set image.repository="${IMAGE_REPO:-siesta}" --set image.tag="${IMAGE_TAG:-e2e}" \
  --set kafka.bootstrapServers=kafka.$ns.svc:9092 --set replicaCount=2
k -n $ns rollout status deploy/siesta --timeout=120s
wait_for '{.spec.job.state}' suspended 360
leader=$(k -n $ns get lease siesta.flink.io -o jsonpath='{.spec.holderIdentity}' | cut -d_ -f1)
echo "leader is $leader; deleting it"
k -n $ns delete pod "$leader" --wait=false
wait_for '{.status.lifecycleState}' SUSPENDED 180
k -n $ns exec deploy/kafka -- sh -c 'echo hello | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic e2e-in'
wait_for '{.spec.job.state}' running 180
wait_for '{.status.jobStatus.state}' RUNNING 300
[ "$(events | grep -c '^Suspended')" = 1 ] && [ "$(events | grep -c '^Resumed')" = 1 ] \
  || { echo "failover must not double-act:"; events | grep -E '^(Suspended|Resumed)'; exit 1; }

echo "chaos 2: the operator is away when we suspend; nothing must happen twice"
k -n $ns annotate flinkdeployment example siesta.flink.io/min-awake=30s --overwrite
k scale deploy/flink-kubernetes-operator --replicas=0
k rollout status deploy/flink-kubernetes-operator --timeout=60s || true
wait_for '{.spec.job.state}' suspended 360     # our patch lands even without the operator
sleep 120                                       # we must sit still while nothing happens
[ "$(k -n $ns get flinkdeployment example -o jsonpath='{.status.lifecycleState}')" = STABLE ] || { echo "operator was not down as intended"; exit 1; }
k scale deploy/flink-kubernetes-operator --replicas=1
k rollout status deploy/flink-kubernetes-operator --timeout=180s
wait_for '{.status.lifecycleState}' SUSPENDED 300
[ "$(events | grep -c '^Suspended')" = 2 ] || { echo "exactly one more Suspended expected:"; events | grep '^Suspended'; exit 1; }
k -n $ns delete flinkdeployment example --wait=false
echo "chaos OK"
