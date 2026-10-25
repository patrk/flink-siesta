#!/usr/bin/env bash
# Load on KinD: LOAD_DEPLOYMENTS FlinkDeployments whose status this script writes, as the operator
# would, so no Flink pod runs; one Kafka broker; the controller at its defaults. Measures the
# first pass, the steady state, a suspend wave and the time from a record to the resume.
# The Flink operator is scaled to zero meanwhile, or it would try to deploy every one of them.
#   make e2e-load-test LOAD_DEPLOYMENTS=2000
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
ctx=${KUBE_CONTEXT:-kind-siesta}
k() { kubectl --context "$ctx" "$@"; }
ns=${LOAD_NAMESPACE:-load}
N=${LOAD_DEPLOYMENTS:-1000}
TOPICS=${LOAD_TOPICS:-100}
WOKEN_TOPICS=${LOAD_WOKEN_TOPICS:-10}
POLL=60 # the controller's default --poll-interval, in seconds
report=${LOAD_REPORT:-$here/../load-$N.txt}
operator_ns=$(k get deploy -A -l app.kubernetes.io/name=flink-kubernetes-operator -o jsonpath='{.items[0].metadata.namespace}' 2>/dev/null || true)
[ -n "$operator_ns" ] || operator_ns=$(k get deploy -A -o jsonpath='{range .items[?(@.metadata.name=="flink-kubernetes-operator")]}{.metadata.namespace}{end}')

log() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$report"; }
# The operator puts a finalizer on every FlinkDeployment it has seen. With it scaled to zero
# nothing removes them, so the namespace is emptied by hand before the operator comes back:
# otherwise it would try to tear down a thousand jobs that never ran.
empty_namespace() {
  k get ns "$ns" >/dev/null 2>&1 || return 0
  k proxy --port=18002 >/dev/null 2>&1 & local p=$!; sleep 2
  k -n "$ns" get flinkdeployments -o name 2>/dev/null | sed 's#.*/##' | xargs -P 16 -I{} curl -s -o /dev/null -X PATCH \
    -H 'Content-Type: application/merge-patch+json' --data '{"metadata":{"finalizers":null}}' \
    "http://127.0.0.1:18002/apis/flink.apache.org/v1beta1/namespaces/$ns/flinkdeployments/{}"
  kill "$p" 2>/dev/null || true
  k delete ns "$ns" --wait=true --timeout=300s >/dev/null 2>&1 || true
}
cleanup() {
  [ -n "${proxy:-}" ] && kill "$proxy" 2>/dev/null || true
  helm --kube-context "$ctx" uninstall siesta -n "$ns" >/dev/null 2>&1 || true
  empty_namespace
  k -n "$operator_ns" scale deploy/flink-kubernetes-operator --replicas=1 >/dev/null 2>&1 || true
}
trap cleanup EXIT
: > "$report"
log "load test: $N deployments over $TOPICS topics, controller defaults (poll ${POLL}s, 4 workers, API 50 qps / 100 burst per client)"

k -n "$operator_ns" scale deploy/flink-kubernetes-operator --replicas=0 >/dev/null
empty_namespace
k create ns "$ns" >/dev/null
sed -e "s#kafka.default.svc#kafka.$ns.svc#" -e "s#namespace: default#namespace: $ns#" "$here/kafka.yaml" > "$here/.load-kafka.yaml"
sed -e "s#namespace: default#namespace: $ns#" "$here/storage.yaml" | k -n "$ns" apply -f - >/dev/null 2>&1 || true
k -n "$ns" apply -f "$here/.load-kafka.yaml" >/dev/null; rm -f "$here/.load-kafka.yaml"
k -n "$ns" rollout status deploy/kafka --timeout=180s >/dev/null
until k -n "$ns" exec deploy/kafka -- /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; do sleep 2; done
log "creating $TOPICS topics"
k -n "$ns" exec deploy/kafka -- sh -c "for i in \$(seq 0 $((TOPICS-1))); do /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic load-\$i --partitions 1 >/dev/null; done"

log "creating $N FlinkDeployments"
python3 - "$N" "$TOPICS" "$ns" > "$here/.load-deployments.yaml" <<'EOF'
import sys
n, topics, ns = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
for i in range(n):
    print(f"""---
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: load-{i}
  namespace: {ns}
  annotations:
    siesta.flink.io/mode: auto
    siesta.flink.io/sources: load-{i % topics}
    siesta.flink.io/idle-after: 5m
    siesta.flink.io/min-awake: 1m
spec:
  image: flink:2.2
  flinkVersion: v2_2
  serviceAccount: flink
  jobManager: {{ resource: {{ memory: "1024m", cpu: 0.5 }} }}
  taskManager: {{ resource: {{ memory: "1024m", cpu: 0.5 }} }}
  job: {{ jarURI: "local:///opt/flink/job.jar", upgradeMode: savepoint, state: running }}""")
EOF
start=$(date +%s)
k apply --server-side -f "$here/.load-deployments.yaml" >/dev/null; rm -f "$here/.load-deployments.yaml"
log "created in $(( $(date +%s) - start ))s"

k proxy --port=18001 >/dev/null 2>&1 & proxy=$!
sleep 2
status() { # $1 = JSON status, applied to every deployment, 16 at a time, as the operator would
  seq 0 $((N-1)) | xargs -P 16 -I{} curl -s -o /dev/null -X PATCH -H 'Content-Type: application/merge-patch+json' \
    --data "{\"status\":$1}" "http://127.0.0.1:18001/apis/flink.apache.org/v1beta1/namespaces/$ns/flinkdeployments/load-{}/status"
}
start=$(date +%s)
status '{"lifecycleState":"STABLE","jobStatus":{"state":"RUNNING","jobId":"j"}}'
log "status STABLE/RUNNING written in $(( $(date +%s) - start ))s"

helm --kube-context "$ctx" upgrade --install siesta "$here/../helm/flink-siesta" -n "$ns" \
  --set image.repository="${IMAGE_REPO:-siesta}" --set image.tag="${IMAGE_TAG:-e2e}" \
  --set kafka.bootstrapServers="kafka.$ns.svc:9092" --set replicaCount=1 --set config.flinkRest=false >/dev/null
k -n "$ns" rollout status deploy/siesta --timeout=180s >/dev/null
installed=$(date +%s)

scrape() { k -n "$ns" exec deploy/kafka -- sh -c "wget -qO- http://siesta-metrics.$ns.svc:8080/metrics 2>/dev/null || curl -s http://siesta-metrics.$ns.svc:8080/metrics"; }
metric() { # $1 = series name, $2 = a label it must carry, such as controller="siesta"; prints the sum
  scrape | awk -v name="$1" -v label="${2:-}" \
    'index($1, name) == 1 && (substr($1, length(name) + 1, 1) == "{" || substr($1, length(name) + 1, 1) == "") && index($1, label) { s += $NF } END { printf "%.0f", s }'
}
memory() { scrape | awk '/^process_resident_memory_bytes/ { printf "%d", $2/1048576 }'; }
configmaps() { k -n "$ns" get cm -l app.kubernetes.io/managed-by=flink-siesta --no-headers 2>/dev/null | wc -l | tr -d ' '; }
specs() { # $1 = running|suspended: how many deployments have spec.job.state $1
  k -n "$ns" get flinkdeployments -o jsonpath='{range .items[*]}{.spec.job.state}{"\n"}{end}' | grep -c "^$1$" || true; }

log "first pass: every deployment visited once"
until [ "$(configmaps)" -ge "$N" ]; do
  [ $(( $(date +%s) - installed )) -gt 1800 ] && { log "first pass did not finish in 30 minutes: $(configmaps)/$N"; exit 1; }
  sleep 5; done
log "first pass: ${N} deployments in $(( $(date +%s) - installed ))s, memory $(memory) MiB"

log "steady state: 3 minutes"
r0=$(metric controller_runtime_reconcile_total 'controller="siesta"'); a0=$(metric rest_client_requests_total); t0=$(date +%s)
o0=$(metric siesta_probe_duration_seconds_count 'kind="offsets"')
breakdown() { scrape | awk '/^rest_client_requests_total/ { match($0, /method="[A-Z]+"/); m = substr($0, RSTART+8, RLENGTH-9); match($0, /code="[0-9]+"/); c = substr($0, RSTART+6, RLENGTH-7); s[m "/" c] += $NF } END { for (k in s) printf "%s %d\n", k, s[k] }' | sort; }
before=$(breakdown)
sleep 180
r1=$(metric controller_runtime_reconcile_total 'controller="siesta"'); a1=$(metric rest_client_requests_total); t1=$(date +%s)
python3 - "$N" "$POLL" "$r0" "$r1" "$a0" "$a1" "$t0" "$t1" <<'EOF' | tee -a "$report"
import sys
n, poll, r0, r1, a0, a1, t0, t1 = map(float, sys.argv[1:])
rate = (r1 - r0) / (t1 - t0)
print(f"  reconciles: {rate:.1f}/s, so each deployment is visited every {n / rate:.0f}s against a {poll:.0f}s poll interval")
print(f"  API requests: {(a1 - a0) / (t1 - t0):.1f}/s, the cached client and the uncached reader each limited to 50/s")
EOF
log "  memory $(memory) MiB, workqueue depth $(metric workqueue_depth 'name="siesta"')"
log "  API requests in the 3 minutes, by method and code:"
join -a2 -e0 -o '2.1,2.2,1.2' <(echo "$before") <(breakdown) | awk '{ printf "    %s: %d\n", $1, $2 - $3 }' | tee -a "$report"
o1=$(metric siesta_probe_duration_seconds_count 'kind="offsets"')
log "  offsets probes: $(( o1 - o0 )) in 3 minutes"

log "suspend wave: idle-after 5m and min-awake 1m from the first observation"
until [ "$(specs suspended)" -ge "$N" ]; do
  [ $(( $(date +%s) - installed )) -gt 2400 ] && { log "suspend wave incomplete after 40 minutes: $(specs suspended)/$N"; exit 1; }
  sleep 10; done
log "all $N suspended $(( $(date +%s) - installed ))s after install (idle-after alone is 300s)"
start=$(date +%s)
status '{"lifecycleState":"SUSPENDED","jobStatus":{"state":"FINISHED","jobId":"j"}}'
log "status SUSPENDED written in $(( $(date +%s) - start ))s; waiting two passes so every one has seen it"
sleep $((2 * POLL + 30))

woken=$(( N / TOPICS * WOKEN_TOPICS ))
log "resume latency: one record on each of $WOKEN_TOPICS topics, which $woken deployments watch"
produced=$(date +%s)
for t in $(seq 0 $((WOKEN_TOPICS-1))); do
  k -n "$ns" exec deploy/kafka -- sh -c "echo wake | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic load-$t" >/dev/null 2>&1
done
log "  records produced in $(( $(date +%s) - produced ))s"
first=""; until [ "$(specs running)" -ge "$woken" ]; do
  [ -z "$first" ] && [ "$(specs running)" -gt 0 ] && first=$(( $(date +%s) - produced ))
  [ $(( $(date +%s) - produced )) -gt 900 ] && { log "only $(specs running)/$woken resumed after 15 minutes"; exit 1; }
  sleep 2; done
log "  first resume after ${first:-<2}s, all $woken resumed after $(( $(date +%s) - produced ))s"
extra=$(( $(specs running) - woken ))
[ "$extra" -eq 0 ] || { log "  $extra deployments resumed that had no input"; exit 1; }
log "  no deployment without input resumed"
log "end: memory $(memory) MiB, API requests total $(metric rest_client_requests_total), probe errors $(metric siesta_probe_errors_total)"
log "report in $report"
