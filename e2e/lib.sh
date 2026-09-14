#!/usr/bin/env bash
# Shared plumbing for the e2e scenarios: cluster access, manifest rendering for the Flink version
# under test, waits, and event helpers that survive the events API's series folding. Sourced,
# never run.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# Every scenario gets its own namespace, so scenarios can run side by side on one cluster.
ns=${E2E_NAMESPACE:-default}
ctx=${KUBE_CONTEXT:-kind-siesta}
k() { kubectl --context "$ctx" "$@"; }

# Manifests are written for Flink 2.x; FLINK_VERSION renders them for the version under test.
# 1.x still uses the pre-2.0 configuration keys, everything else is the same.
FLINK_VERSION=${FLINK_VERSION:-2.2}
render() {
  local out; out=$(mktemp)
  sed -e "s#image: flink:2.2#image: flink:${FLINK_VERSION}#" -e "s#siesta-e2e-job:2.2#siesta-e2e-job:${FLINK_VERSION}#" \
      -e "s#flinkVersion: v2_2#flinkVersion: v${FLINK_VERSION/./_}#" \
      -e "s#namespace: default#namespace: ${ns}#" -e "s#kafka.default.svc#kafka.${ns}.svc#" "$1" > "$out"
  if [ "${FLINK_VERSION%%.*}" = 1 ]; then
    sed -i.bak -e 's#execution.checkpointing.savepoint-dir#state.savepoints.dir#' \
               -e 's#execution.checkpointing.dir#state.checkpoints.dir#' "$out" && rm -f "$out.bak"
  fi
  echo "$out"
}

fd=example  # the deployment the waits and event helpers look at; scenarios may point it elsewhere
wait_for() { # $1 = jsonpath expr, $2 = expected, $3 = timeout s
  local i=0; until [ "$(k -n $ns get flinkdeployment "$fd" -o jsonpath="$1" 2>/dev/null)" = "$2" ]; do
    i=$((i+5)); [ $i -ge "$3" ] && { echo "timeout waiting for $1 = $2"; k -n $ns describe flinkdeployment "$fd" | tail -30; exit 1; }
    sleep 5; done; }
wait_on() { # like wait_for, substring match
  local i=0; until k -n $ns get flinkdeployment "$fd" -o jsonpath="$1" 2>/dev/null | grep -q "$2"; do
    i=$((i+5)); [ $i -ge "$3" ] && { echo "timeout waiting for $1 ~ $2"; k -n $ns describe flinkdeployment "$fd" | tail -20; exit 1; }
    sleep 5; done; }
until_count() { # $1 = description, $2 = timeout s, $3.. = command that prints a number, waits until it prints $EXPECT
  local what=$1 timeout=$2; shift 2; local i=0
  until [ "$("$@")" = "$EXPECT" ]; do i=$((i+5)); [ $i -ge "$timeout" ] && { echo "timeout: $what"; events | grep -E '^(Source|Suspended|Resumed|Restarted)'; exit 1; }; sleep 5; done; }
reason() { k -n $ns get flinkdeployment "$fd" -o jsonpath='{.metadata.annotations.siesta\.flink\.io/reason}'; }

# Events outlive deleted objects by an hour; select by UID so earlier runs do not show up.
uid() { k -n $ns get flinkdeployment "$fd" -o jsonpath='{.metadata.uid}'; }
events() { k -n $ns get events --field-selector involvedObject.uid=$(uid) --sort-by=.metadata.creationTimestamp -o custom-columns=REASON:.reason,MESSAGE:.message; }
# The events API folds a repeat of the same reason on the same object into a series on the
# first event, keyed on reason and resourceVersion, not on the message, so a repeat may or may
# not fold. occurrences sums the series counts; transitions_while_unknown reads each occurrence's
# time from eventTime and series.lastObservedTime, exact for the two occurrences it needs.
occurrences() { k -n $ns get events --field-selector involvedObject.uid=$(uid) -o json | python3 -c '
import sys, json
r = sys.argv[1]
print(sum((e.get("series") or {}).get("count") or e.get("count") or 1 for e in json.load(sys.stdin)["items"] if e["reason"] == r))' "$1"; }
transitions_while_unknown() { k -n $ns get events --field-selector involvedObject.uid=$(uid) -o json | python3 -c '
import sys, json
n = int(sys.argv[1]); items = [e for e in json.load(sys.stdin)["items"] if e.get("eventTime")]
def times(reason):
    out = []
    for e in items:
        if e["reason"] != reason: continue
        out.append(e["eventTime"])
        s = e.get("series") or {}
        if s.get("count", 1) >= 2: out.append(s["lastObservedTime"])
    return sorted(out)
u, r = times("SourceUnreachable")[n - 1], times("SourceReachable")[n - 1]
print(sum(1 for e in items if e["reason"] in ("Suspended", "Resumed", "Restarted") and u < e["eventTime"] < r))' "$1"; }

kafka() { k -n $ns exec deploy/kafka -- "$@"; }
produce() { kafka sh -c "$1 | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic ${2:-e2e-in}"; }
topic() { kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"; }

# fresh_volumes: no HA metadata, checkpoints, savepoints or topics inherited from an earlier run.
# The driver does this once; a scenario run on its own reuses what is there.
fresh_volumes() {
  k -n $ns delete flinkdeployment example failing --ignore-not-found --wait=true
  k -n $ns delete deploy kafka --ignore-not-found --wait=true
  k -n $ns delete pvc flink-data kafka-data --ignore-not-found --wait=true
}

# The operator chart creates the flink ServiceAccount and its Role in its own namespace only;
# a scenario namespace gets a copy, since the JobManager pods run as that account.
ensure_namespace() {
  k get ns "$ns" >/dev/null 2>&1 || k create ns "$ns" >/dev/null
  k -n $ns get sa flink >/dev/null 2>&1 && return
  for obj in serviceaccount/flink role/flink rolebinding/flink-role-binding; do
    k -n default get "$obj" -o json | python3 -c '
import sys, json
o = json.load(sys.stdin); m = o["metadata"]
for key in ("namespace", "resourceVersion", "uid", "creationTimestamp", "managedFields", "annotations", "labels"): m.pop(key, None)
for s in o.get("subjects", []): s["namespace"] = sys.argv[1]
print(json.dumps(o))' "$ns" | k -n $ns apply -f - >/dev/null
  done
}

# scenario_start: a clean slate a scenario can rely on. Its namespace exists, deployments and
# the controller gone, fresh volumes, Kafka up, the two topics empty.
scenario_start() {
  ensure_namespace
  echo "=== scenario $1 in namespace $ns (flink ${FLINK_VERSION}, operator chart $(k get deploy flink-kubernetes-operator -o jsonpath='{.metadata.labels.helm\.sh/chart}' 2>/dev/null))"
  helm --kube-context "$ctx" uninstall siesta -n $ns 2>/dev/null || true
  fresh_volumes
  k -n $ns apply -f "$(render "$here/storage.yaml")" -f "$(render "$here/kafka.yaml")"
  k -n $ns scale deploy/kafka --replicas=1
  k -n $ns rollout status deploy/kafka --timeout=120s
  until kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; do sleep 2; done
  for t in e2e-in e2e-other; do
    topic --delete --topic $t --if-exists >/dev/null 2>&1 || true
    topic --create --if-not-exists --topic $t --partitions 1 >/dev/null
  done
}

deploy_example() {
  k -n $ns apply -f "$(render "$here/flinkdeployment.yaml")"
  wait_for '{.status.jobStatus.state}' RUNNING 300
}

install_siesta() { # extra --set flags as arguments
  helm --kube-context "$ctx" upgrade --install siesta "$here/../helm/flink-siesta" -n $ns \
    --set image.repository="${IMAGE_REPO:-siesta}" --set image.tag="${IMAGE_TAG:-e2e}" \
    --set kafka.bootstrapServers=kafka.$ns.svc:9092 --set replicaCount=1 \
    --set config.restart.maxRestarts=2 --set config.restart.backoff=15s "$@"
  k -n $ns rollout status deploy/siesta --timeout=120s
}
