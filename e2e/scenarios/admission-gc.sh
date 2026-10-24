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

echo "admission policy: nothing else in spec, metadata or the namespace's ConfigMaps"
denied() { # $1 = what, then the kubectl command: it must be refused for the controller identity
  local what=$1; shift
  if "$@" --as="$sa" --dry-run=server >/dev/null 2>&1; then echo "admission policy let the controller identity change $what"; exit 1; fi; }
denied spec.ingress k -n $ns patch flinkdeployment example --type merge -p '{"spec":{"ingress":{"template":"evil.example.com"}}}'
denied spec.mode k -n $ns patch flinkdeployment example --type merge -p '{"spec":{"mode":"standalone"}}'
denied spec.job.allowNonRestoredState k -n $ns patch flinkdeployment example --type merge -p '{"spec":{"job":{"allowNonRestoredState":true}}}'
denied spec.job.savepointRedeployNonce k -n $ns patch flinkdeployment example --type merge -p '{"spec":{"job":{"savepointRedeployNonce":42}}}'
denied "an annotation not its own" k -n $ns patch flinkdeployment example --type merge -p '{"metadata":{"annotations":{"other.example/x":"y"}}}'
denied "a label" k -n $ns patch flinkdeployment example --type merge -p '{"metadata":{"labels":{"x":"y"}}}'
denied "a finalizer" k -n $ns patch flinkdeployment example --type merge -p '{"metadata":{"finalizers":["example.com/hold"]}}'
denied "a ConfigMap not its own" k -n $ns create configmap flink-ha-example --from-literal=x=y
k -n $ns patch configmap siesta-example --as="$sa" --dry-run=server --type merge -p '{"metadata":{"labels":{"check":"admission"}}}' >/dev/null

echo "delete the deployment: its ConfigMap must be garbage-collected"
k -n $ns delete flinkdeployment example --wait=true
i=0; until ! k -n $ns get configmap siesta-example >/dev/null 2>&1; do i=$((i+5)); [ $i -ge 120 ] && { echo "ConfigMap siesta-example was not garbage-collected"; exit 1; }; sleep 5; done
scenario_end admission-gc
