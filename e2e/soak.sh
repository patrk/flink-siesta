#!/usr/bin/env bash
# Real soak on KinD: three jobs, a producer that sends at random intervals, the controller
# killed every ten minutes, Kafka taken away once, memory read at minute 5 and at the end.
# ~45 minutes. Run nightly: make e2e-soak
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
ns=default
ctx=${KUBE_CONTEXT:-kind-siesta}
k() { kubectl --context "$ctx" "$@"; }
DURATION_MIN=${SOAK_MINUTES:-45}
FLINK_VERSION=${FLINK_VERSION:-2.2}
render() { local out; out=$(mktemp); sed -e "s#image: flink:2.2#image: flink:${FLINK_VERSION}#" -e "s#siesta-e2e-job:2.2#siesta-e2e-job:${FLINK_VERSION}#" -e "s#flinkVersion: v2_2#flinkVersion: v${FLINK_VERSION/./_}#" -e "s#name: example#name: $2#" -e "s#sources: e2e-in#sources: $2#" "$1" > "$out"
  if [ "${FLINK_VERSION%%.*}" = 1 ]; then sed -i.bak -e 's#execution.checkpointing.savepoint-dir#state.savepoints.dir#' -e 's#execution.checkpointing.dir#state.checkpoints.dir#' "$out" && rm -f "$out.bak"; fi; echo "$out"; }
rss() { k -n $ns exec deploy/kafka -- sh -c "wget -qO- http://siesta-metrics.$ns.svc:8080/metrics 2>/dev/null || curl -s http://siesta-metrics.$ns.svc:8080/metrics" | awk '/^process_resident_memory_bytes/ {print int($2/1048576)}'; }

echo "clean up"
for j in soak-a soak-b soak-c; do k -n $ns delete flinkdeployment $j --ignore-not-found --wait=true; done
helm --kube-context "$ctx" uninstall siesta -n $ns 2>/dev/null || true
k -n $ns delete deploy kafka --ignore-not-found --wait=true
k -n $ns delete pvc flink-data kafka-data --ignore-not-found --wait=true
k -n $ns apply -f "$here/storage.yaml" -f "$here/kafka.yaml"
k -n $ns rollout status deploy/kafka --timeout=120s
until k -n $ns exec deploy/kafka -- /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; do sleep 2; done
for j in soak-a soak-b soak-c; do
  k -n $ns exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $j --partitions 1
  k -n $ns apply -f "$(render "$here/flinkdeployment.yaml" $j)"
done
for j in soak-a soak-b soak-c; do
  i=0; until [ "$(k -n $ns get flinkdeployment $j -o jsonpath='{.status.jobStatus.state}' 2>/dev/null)" = RUNNING ]; do i=$((i+5)); [ $i -ge 600 ] && { echo "$j never ran"; exit 1; }; sleep 5; done
done
helm --kube-context "$ctx" upgrade --install siesta "$here/../helm/flink-siesta" -n $ns \
  --set image.repository="${IMAGE_REPO:-siesta}" --set image.tag="${IMAGE_TAG:-e2e}" --set kafka.bootstrapServers=kafka.$ns.svc:9092 --set replicaCount=1
k -n $ns rollout status deploy/siesta --timeout=120s

start=$(date +%s); end=$((start + DURATION_MIN*60)); rss5=""; kills=0; outage_done=0
while [ "$(date +%s)" -lt "$end" ]; do
  elapsed=$(( ($(date +%s) - start) / 60 ))
  # a random job gets a record every 3-6 minutes; the others stay idle and get suspended
  j=$(printf "soak-a\nsoak-b\nsoak-c\n" | shuf -n1)
  k -n $ns exec deploy/kafka -- sh -c "echo tick-$elapsed | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic $j" >/dev/null 2>&1 || true
  if [ $((elapsed % 10)) -eq 9 ] && [ $kills -lt $((elapsed / 10 + 1)) ]; then
    k -n $ns delete pod -l app.kubernetes.io/name=flink-siesta --wait=false; kills=$((kills+1)); echo "min $elapsed: killed the controller ($kills)"
  fi
  if [ $elapsed -ge 20 ] && [ $outage_done -eq 0 ]; then
    echo "min $elapsed: Kafka outage for 3 minutes"; k -n $ns scale deploy/kafka --replicas=0; sleep 180; k -n $ns scale deploy/kafka --replicas=1; k -n $ns rollout status deploy/kafka --timeout=120s; outage_done=1
  fi
  if [ $elapsed -ge 5 ] && [ -z "$rss5" ]; then rss5=$(rss || echo ""); echo "min 5: controller RSS ${rss5} MiB"; fi
  sleep $((180 + RANDOM % 180))
done

echo "soak ended after $DURATION_MIN min; final state:"
k -n $ns get flinkdeployment -o custom-columns='NAME:.metadata.name,SPEC:.spec.job.state,JOB:.status.jobStatus.state,LIFECYCLE:.status.lifecycleState,SIESTA:.metadata.annotations.siesta\.flink\.io/state'
rssEnd=$(rss || echo "")
echo "controller RSS: min5=${rss5} end=${rssEnd} MiB; controller restarts forced: $kills"
if [ -n "$rss5" ] && [ -n "$rssEnd" ] && [ "$rssEnd" -gt $((rss5 * 13 / 10 + 10)) ]; then echo "RSS grew more than 30%: leak?"; exit 1; fi
for j in soak-a soak-b soak-c; do
  st=$(k -n $ns get flinkdeployment $j -o jsonpath='{.metadata.annotations.siesta\.flink\.io/state}')
  spec=$(k -n $ns get flinkdeployment $j -o jsonpath='{.spec.job.state}')
  case "$st/$spec" in active/running|suspended/suspended) ;; *) echo "$j inconsistent: siesta=$st spec=$spec"; exit 1;; esac
done
k -n $ns get pods -l app.kubernetes.io/name=flink-siesta -o custom-columns='POD:.metadata.name,RESTARTS:.status.containerStatuses[0].restartCount' | tail -n +2 | awk '$2>0 {print "controller crashed on its own:", $0; exit 1}'
echo "soak OK"
