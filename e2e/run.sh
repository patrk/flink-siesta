#!/usr/bin/env bash
# End-to-end on KinD: the Flink operator, a single-node Kafka, a Kafka-reading job and the
# controller. Every scenario under e2e/scenarios runs in its own namespace, E2E_PARALLEL at a
# time (1 means in series), or one in the foreground: E2E_SCENARIO=job-graph. Each stands on its
# own, so a failing one reruns in minutes, and CI runs them on separate clusters.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
all="restart-budget job-graph outage suspend-resume sources-auto admission-gc"  # longest first, so the pairs overlap best
scenarios=${E2E_SCENARIO:-$all}
par=${E2E_PARALLEL:-2}
logdir=${E2E_LOGDIR:-/tmp/siesta-e2e}

ctx=${KUBE_CONTEXT:-kind-siesta}
# Leftovers eat the memory the scenarios need: a previous run's namespaces still run their
# Kafkas and jobs, and so does the default namespace after a single-namespace run. Start empty.
clear_cluster() {
  local stale
  stale=$(kubectl --context "$ctx" get ns -o name | grep '^namespace/e2e-' | sed 's#namespace/##' || true)
  kubectl --context "$ctx" -n default delete flinkdeployment --all --wait=true >/dev/null 2>&1 || true
  helm --kube-context "$ctx" uninstall siesta -n default >/dev/null 2>&1 || true
  kubectl --context "$ctx" -n default delete deploy kafka pvc/flink-data pvc/kafka-data --ignore-not-found >/dev/null 2>&1 || true
  if [ -n "$stale" ]; then
    echo "removing namespaces from an earlier run: $(echo $stale)"
    for n in $stale; do kubectl --context "$ctx" -n "$n" delete flinkdeployment --all --wait=true >/dev/null 2>&1 || true; done
    kubectl --context "$ctx" delete ns $stale --wait=true >/dev/null
  fi
}

set -- $scenarios
if [ $# -eq 1 ]; then
  clear_cluster
  E2E_NAMESPACE="e2e-$1" exec bash "$here/scenarios/$1.sh"
fi
clear_cluster

mkdir -p "$logdir"; rm -f "$logdir"/*.status
# One scenario per invocation, the name as an argument: BSD xargs caps -I replacements at 255 bytes.
run_one() {
  local s=$1
  if E2E_NAMESPACE="e2e-$s" bash "$here/scenarios/$s.sh" > "$logdir/$s.log" 2>&1; then echo PASS > "$logdir/$s.status"; echo "PASS $s"
  else echo FAIL > "$logdir/$s.status"; echo "FAIL $s"; fi
}
export -f run_one; export here logdir
echo "running $scenarios, $par at a time, logs in $logdir"
printf '%s\n' "$@" | xargs -P "$par" -n 1 bash -c 'run_one "$0"'
failed=""
for s in "$@"; do [ "$(cat "$logdir/$s.status" 2>/dev/null)" = PASS ] || failed="$failed $s"; done
if [ -n "$failed" ]; then
  for s in $failed; do echo "===== $s failed, last lines of $logdir/$s.log"; tail -25 "$logdir/$s.log"; done
  exit 1
fi
echo "e2e OK: $scenarios"
