#!/usr/bin/env bash
# End-to-end: KinD + Flink operator + single-node Kafka + the controller.
# Asserts: idle -> suspended; produce -> running
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
ns=default
ctx=${KUBE_CONTEXT:-kind-siesta}
k() { kubectl --context "$ctx" "$@"; }

# Manifests are written for Flink 2.x; FLINK_VERSION renders them for the version under test.
# 1.x still uses the pre-2.0 configuration keys, everything else is the same.
FLINK_VERSION=${FLINK_VERSION:-2.2}
render() {
  local out; out=$(mktemp)
  sed -e "s#image: flink:2.2#image: flink:${FLINK_VERSION}#" -e "s#siesta-e2e-job:2.2#siesta-e2e-job:${FLINK_VERSION}#" \
      -e "s#flinkVersion: v2_2#flinkVersion: v${FLINK_VERSION/./_}#" "$1" > "$out"
  if [ "${FLINK_VERSION%%.*}" = 1 ]; then
    sed -i.bak -e 's#execution.checkpointing.savepoint-dir#state.savepoints.dir#' \
               -e 's#execution.checkpointing.dir#state.checkpoints.dir#' "$out" && rm -f "$out.bak"
  fi
  echo "$out"
}
echo "flink ${FLINK_VERSION}, operator chart $(k get deploy flink-kubernetes-operator -o jsonpath='{.metadata.labels.helm\.sh/chart}' 2>/dev/null)"

wait_for() { # $1 = jsonpath expr, $2 = expected, $3 = timeout s
  local i=0; until [ "$(k -n $ns get flinkdeployment example -o jsonpath="$1" 2>/dev/null)" = "$2" ]; do
    i=$((i+5)); [ $i -ge "$3" ] && { echo "timeout waiting for $1 = $2"; k -n $ns describe flinkdeployment example | tail -30; exit 1; }
    sleep 5; done; }

echo "clean up any previous run"
k -n $ns delete flinkdeployment example failing --ignore-not-found --wait=true
helm --kube-context "$ctx" uninstall siesta -n $ns 2>/dev/null || true
# A fresh volume per run: no HA metadata, checkpoints or savepoints inherited from the last one.
k -n $ns delete deploy kafka --ignore-not-found --wait=true
k -n $ns delete pvc flink-data kafka-data --ignore-not-found --wait=true

k -n $ns apply -f "$here/storage.yaml" -f "$here/kafka.yaml"
k -n $ns rollout status deploy/kafka --timeout=120s
until k -n $ns exec deploy/kafka -- /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; do sleep 2; done
k -n $ns exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic e2e-in --partitions 1
k -n $ns exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic e2e-other --partitions 1

k -n $ns apply -f "$(render "$here/flinkdeployment.yaml")"
wait_for '{.status.jobStatus.state}' RUNNING 300

helm --kube-context "$ctx" upgrade --install siesta "$here/../helm/flink-siesta" -n $ns \
  --set image.repository="${IMAGE_REPO:-siesta}" --set image.tag="${IMAGE_TAG:-e2e}" \
  --set kafka.bootstrapServers=kafka.$ns.svc:9092 --set replicaCount=1 \
  --set config.restart.maxRestarts=2 --set config.restart.backoff=15s
k -n $ns rollout status deploy/siesta --timeout=120s

echo "expect suspend after idle-after (2m) + min-awake (30s)"
wait_for '{.spec.job.state}' suspended 360
wait_for '{.metadata.annotations.siesta\.flink\.io/state}' suspended 60
wait_for '{.status.lifecycleState}' SUSPENDED 180
sp=$(k -n $ns get flinkdeployment example -o jsonpath='{.status.jobStatus.upgradeSavepointPath}')
[ -n "$sp" ] || {
  echo "operator recorded no savepoint on suspend; operator events for this object:"
  uid=$(k -n $ns get flinkdeployment example -o jsonpath='{.metadata.uid}')
  k -n $ns get events --field-selector involvedObject.uid=$uid -o custom-columns=T:.metadata.creationTimestamp,R:.reason,M:.message \
    | grep -E "SavepointError|JobException|JobStatusChanged" | cut -c1-300 | tail -12
  exit 1; }
echo "savepoint recorded: $sp"

echo "restart the controller while suspended: state must survive us"
k -n $ns rollout restart deploy/siesta
k -n $ns rollout status deploy/siesta --timeout=120s
k -n $ns get configmap siesta-example -o jsonpath='{.data.state}' | grep -q suspended || { echo "ConfigMap must hold the suspended state"; exit 1; }

echo "produce one record, expect resume within ~1 min"
k -n $ns exec deploy/kafka -- sh -c 'echo hello | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic e2e-in'
wait_for '{.spec.job.state}' running 120
wait_for '{.status.jobStatus.state}' RUNNING 300
echo "expect the new JobManager to have restored from that savepoint"
jm=$(k -n $ns get pods -l app=example,component=jobmanager -o jsonpath='{.items[0].metadata.name}')
k -n $ns logs "$jm" | grep -i -E "restoring job .* from savepoint|restored from savepoint" | head -2 | grep -q . \
  || { echo "JobManager log has no savepoint restore line"; k -n $ns logs "$jm" | grep -i savepoint | head; exit 1; }
# Events outlive deleted objects by an hour; select by UID so earlier runs do not show up.
uid=$(k -n $ns get flinkdeployment example -o jsonpath='{.metadata.uid}')
events() { k -n $ns get events --field-selector involvedObject.uid=$uid --sort-by=.metadata.creationTimestamp -o custom-columns=REASON:.reason,MESSAGE:.message; }
# The events API folds an identical event within six minutes into a series on the first one,
# so a line count undercounts. occurrences sums series counts; transitions_since counts our
# transitions after a point in time, whatever their aggregation.
occurrences() { k -n $ns get events --field-selector involvedObject.uid=$uid -o json | python3 -c '
import sys, json
r = sys.argv[1]
print(sum((e.get("series") or {}).get("count") or e.get("count") or 1 for e in json.load(sys.stdin)["items"] if e["reason"] == r))' "$1"; }
# transitions_while_unknown N: our transitions strictly between the Nth SourceUnreachable and the
# Nth SourceReachable, by event time. Our events carry eventTime; the operator's do not and are
# not counted, they follow ours anyway.
transitions_while_unknown() { k -n $ns get events --field-selector involvedObject.uid=$uid -o json | python3 -c '
import sys, json
n = int(sys.argv[1]); items = [e for e in json.load(sys.stdin)["items"] if e.get("eventTime")]
def nth(reason): return sorted(e["eventTime"] for e in items if e["reason"] == reason)[n - 1]
u, r = nth("SourceUnreachable"), nth("SourceReachable")
print(sum(1 for e in items if e["reason"] in ("Suspended", "Resumed", "Restarted") and u < e["eventTime"] < r))' "$1"; }
events | grep -E "Suspended|Resumed" | tail -4

echo "the job reads exactly the annotated topic: SourcesVerified once per job instance (before and after the resume)"
i=0; until [ "$(occurrences SourcesVerified)" = 2 ]; do i=$((i+5)); [ $i -ge 120 ] && { echo "expected two SourcesVerified events, got:"; events | grep -E '^Sources'; exit 1; }; sleep 5; done
[ "$(occurrences SourcesDrift)" = 0 ] || { echo "no drift expected yet"; exit 1; }

echo "make the job read a second topic, and slow it down: the new job instance must be reported as drift"
k -n $ns patch flinkdeployment example --type merge -p '{"spec":{"job":{"args":["--bootstrap","kafka.default.svc:9092","--topics","e2e-in,e2e-other","--group","e2e","--sleep-ms","1000","--max-poll-records","50"]}}}'
# The burst goes in now, while the operator upgrades the job: it restarts the idle clock, and the
# new job instance restores its position from the savepoint and works through the burst slowly.
k -n $ns exec deploy/kafka -- sh -c 'seq 400 | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic e2e-in'
i=0; until [ "$(occurrences SourcesDrift)" = 1 ]; do i=$((i+5)); [ $i -ge 420 ] && { echo "expected a SourcesDrift event after the job changed, got:"; events | grep -E '^Sources'; exit 1; }; sleep 5; done
events | grep '^SourcesDrift' | grep -q 'e2e-in,e2e-other' || { echo "drift message must name the job's topics:"; events | grep '^SourcesDrift'; exit 1; }
wait_for '{.status.jobStatus.state}' RUNNING 300

echo "the job gate: the burst of 400 records at one record per second keeps the suspend back until the job has emitted them all"
i=0; until k -n $ns get flinkdeployment example -o jsonpath='{.metadata.annotations.siesta\.flink\.io/reason}' | grep -q 'job busy'; do
  i=$((i+5)); [ $i -ge 300 ] && { echo "expected a 'job busy' reason while the burst drains; reason is:"; k -n $ns get flinkdeployment example -o jsonpath='{.metadata.annotations.siesta\.flink\.io/reason}'; echo; k -n $ns get cm siesta-example -o jsonpath='{.data}'; exit 1; }; sleep 5; done
echo "pending held the suspend; now expect the suspend once the job has caught up"
wait_for '{.spec.job.state}' suspended 480
wait_for '{.status.lifecycleState}' SUSPENDED 180

echo "take Kafka away: one SourceUnreachable, no transitions; bring it back: one SourceReachable"
k -n $ns scale deploy/kafka --replicas=0
i=0; until [ "$(occurrences SourceUnreachable)" -ge 1 ]; do i=$((i+5)); [ $i -ge 180 ] && { echo "no SourceUnreachable event"; exit 1; }; sleep 5; done
k -n $ns scale deploy/kafka --replicas=1
k -n $ns rollout status deploy/kafka --timeout=120s
i=0; until [ "$(occurrences SourceReachable)" -ge 1 ]; do i=$((i+5)); [ $i -ge 180 ] && { echo "no SourceReachable event after the broker returned"; exit 1; }; sleep 5; done
[ "$(occurrences SourceUnreachable)" = 1 ] || { echo "SourceUnreachable must be reported once, not per tick"; exit 1; }
[ "$(transitions_while_unknown 1)" = 0 ] || { echo "nothing may change while the source is unknown:"; events | grep -E "^(Source|Suspended|Resumed|Restarted)"; exit 1; }

echo "delete the topic: unknown again, the job is left alone; recreate it: reachable again"
k -n $ns exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic e2e-in
i=0; until [ "$(occurrences SourceUnreachable)" = 2 ]; do i=$((i+5)); [ $i -ge 180 ] && { echo "a deleted topic must be reported unreachable"; exit 1; }; sleep 5; done
k -n $ns exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic e2e-in --partitions 1
i=0; until [ "$(occurrences SourceReachable)" = 2 ]; do i=$((i+5)); [ $i -ge 180 ] && { echo "a recreated topic must be reported reachable"; exit 1; }; sleep 5; done
[ "$(transitions_while_unknown 2)" = 0 ] || { echo "nothing may change while the topic is missing:"; events | grep -E "^(Source|Suspended|Resumed|Restarted)"; exit 1; }
echo "the recreated topic starts at offset 0, which differs from what was remembered: that counts as input and the suspended job is resumed"
wait_for '{.spec.job.state}' running 120

echo "admission policy: the controller's identity may not change the image, but may change job.state"
sa="system:serviceaccount:$ns:siesta"
if k -n $ns patch flinkdeployment example --as="$sa" --type merge -p '{"spec":{"image":"flink:evil"}}' 2>/dev/null; then
  echo "admission policy did not block an image change by the controller identity"; exit 1; fi
k -n $ns patch flinkdeployment example --as="$sa" --type merge -p '{"metadata":{"annotations":{"siesta.flink.io/reason":"admission check"}}}' >/dev/null

echo "delete the deployment: its ConfigMap must be garbage-collected"
k -n $ns delete flinkdeployment example --wait=true
i=0; until ! k -n $ns get configmap siesta-example >/dev/null 2>&1; do i=$((i+5)); [ $i -ge 120 ] && { echo "ConfigMap siesta-example was not garbage-collected"; exit 1; }; sleep 5; done

echo "scenario 2: a job that fails terminally is restarted twice, then marked unrecoverable"
k -n $ns apply -f "$(render "$here/failing.yaml")"
wait_on() { # like wait_for, for the failing deployment, substring match on annotation/field
  local i=0; until k -n $ns get flinkdeployment failing -o jsonpath="$1" 2>/dev/null | grep -q "$2"; do
    i=$((i+5)); [ $i -ge "$3" ] && { echo "timeout waiting for $1 ~ $2"; k -n $ns describe flinkdeployment failing | tail -20; exit 1; }
    sleep 5; done; }
# Intermediate states last seconds; assert the end state and the trail of events instead.
wait_on '{.metadata.annotations.siesta\.flink\.io/state}' unrecoverable 600
k -n $ns get configmap siesta-failing -o jsonpath='{.data.restarts}' | grep -q '"count":2' \
  || { echo "expected the restart budget to be fully used"; exit 1; }
uid=$(k -n $ns get flinkdeployment failing -o jsonpath='{.metadata.uid}')
trail=$(k -n $ns get events --field-selector involvedObject.uid=$uid -o custom-columns=REASON:.reason,MESSAGE:.message | grep -E "^(Restarted|Unrecoverable)")
echo "$trail"
[ "$(echo "$trail" | grep -c '^Restarted')" = 2 ] || { echo "expected exactly two Restarted events"; exit 1; }
echo "$trail" | grep -q '^Unrecoverable' || { echo "expected an Unrecoverable event"; exit 1; }
k -n $ns delete flinkdeployment failing --wait=false
echo "e2e OK"
