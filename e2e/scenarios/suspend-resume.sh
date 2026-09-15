#!/usr/bin/env bash
# Idle -> suspended with a savepoint; the controller restarted while suspended; input -> resumed
# from that savepoint.
source "$(dirname "$0")/../lib.sh"
scenario_start suspend-resume
deploy_example
install_siesta

echo "expect suspend after idle-after (2m) + min-awake (30s)"
wait_for '{.spec.job.state}' suspended 360
wait_for '{.metadata.annotations.siesta\.flink\.io/state}' suspended 60
wait_for '{.status.lifecycleState}' SUSPENDED 180
sp=$(k -n $ns get flinkdeployment example -o jsonpath='{.status.jobStatus.upgradeSavepointPath}')
[ -n "$sp" ] || {
  echo "operator recorded no savepoint on suspend; operator events for this object:"
  k -n $ns get events --field-selector involvedObject.uid=$(uid) -o custom-columns=T:.metadata.creationTimestamp,R:.reason,M:.message \
    | grep -E "SavepointError|JobException|JobStatusChanged" | cut -c1-300 | tail -12
  exit 1; }
echo "savepoint recorded: $sp"

echo "restart the controller while suspended: state must survive us"
k -n $ns rollout restart deploy/siesta
k -n $ns rollout status deploy/siesta --timeout=120s
k -n $ns get configmap siesta-example -o jsonpath='{.data.state}' | grep -q suspended || { echo "ConfigMap must hold the suspended state"; exit 1; }

echo "produce one record, expect resume within ~1 min"
produce 'echo hello'
wait_for '{.spec.job.state}' running 120
wait_for '{.status.jobStatus.state}' RUNNING 300
echo "expect the new JobManager to have restored from that savepoint"
jm=$(k -n $ns get pods -l app=example,component=jobmanager -o jsonpath='{.items[0].metadata.name}')
k -n $ns logs "$jm" | grep -i -E "restoring job .* from savepoint|restored from savepoint" | head -2 | grep -q . \
  || { echo "JobManager log has no savepoint restore line"; k -n $ns logs "$jm" | grep -i savepoint | head; exit 1; }
events | grep -E "Suspended|Resumed" | tail -4
scenario_end suspend-resume
