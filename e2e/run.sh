#!/usr/bin/env bash
# End-to-end on KinD: the Flink operator, a single-node Kafka, a Kafka-reading job and the
# controller. Every scenario under e2e/scenarios runs in its own namespace, E2E_PARALLEL at a
# time (1 means in series), or one in the foreground: E2E_SCENARIO=job-graph. Each stands on its
# own, so a failing one reruns in minutes, and CI runs them on separate clusters.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
all="suspend-resume job-graph outage admission-gc restart-budget"
scenarios=${E2E_SCENARIO:-$all}
par=${E2E_PARALLEL:-2}
logdir=${E2E_LOGDIR:-/tmp/siesta-e2e}

set -- $scenarios
if [ $# -eq 1 ]; then
  E2E_NAMESPACE="e2e-$1" exec bash "$here/scenarios/$1.sh"
fi

mkdir -p "$logdir"; rm -f "$logdir"/*.status
export here logdir
echo "running $scenarios, $par at a time, logs in $logdir"
printf '%s\n' "$@" | xargs -P "$par" -I{} bash -c '
  if E2E_NAMESPACE="e2e-{}" bash "$here/scenarios/{}.sh" > "$logdir/{}.log" 2>&1; then echo PASS > "$logdir/{}.status"; echo "PASS {}"
  else echo FAIL > "$logdir/{}.status"; echo "FAIL {}"; fi'
failed=""
for s in "$@"; do [ "$(cat "$logdir/$s.status" 2>/dev/null)" = PASS ] || failed="$failed $s"; done
if [ -n "$failed" ]; then
  for s in $failed; do echo "===== $s failed, last lines of $logdir/$s.log"; tail -25 "$logdir/$s.log"; done
  exit 1
fi
echo "e2e OK: $scenarios"
