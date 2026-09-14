#!/usr/bin/env bash
# The admission policy pins the controller's identity to job.state, and a deleted deployment
# takes its ConfigMap with it.
source "$(dirname "$0")/../lib.sh"
scenario_start admission-gc
deploy_example
install_siesta
i=0; until k -n $ns get configmap siesta-example >/dev/null 2>&1; do i=$((i+5)); [ $i -ge 120 ] && { echo "no ConfigMap after the first tick"; exit 1; }; sleep 5; done

echo "admission policy: the controller's identity may not change the image, but may change annotations"
sa="system:serviceaccount:$ns:siesta"
if k -n $ns patch flinkdeployment example --as="$sa" --type merge -p '{"spec":{"image":"flink:evil"}}' 2>/dev/null; then
  echo "admission policy did not block an image change by the controller identity"; exit 1; fi
k -n $ns patch flinkdeployment example --as="$sa" --type merge -p '{"metadata":{"annotations":{"siesta.flink.io/reason":"admission check"}}}' >/dev/null

echo "delete the deployment: its ConfigMap must be garbage-collected"
k -n $ns delete flinkdeployment example --wait=true
i=0; until ! k -n $ns get configmap siesta-example >/dev/null 2>&1; do i=$((i+5)); [ $i -ge 120 ] && { echo "ConfigMap siesta-example was not garbage-collected"; exit 1; }; sleep 5; done
echo "admission-gc OK"
