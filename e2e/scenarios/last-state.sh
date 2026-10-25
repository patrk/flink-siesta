#!/usr/bin/env bash
# upgradeMode: last-state, with HA: idle -> suspended without a savepoint, by design; input ->
# resumed from the last checkpoint the HA metadata points at, with the records read before.
source "$(dirname "$0")/../lib.sh"
scenario_start last-state
m=$(render "$here/flinkdeployment.yaml")
# A checkpoint must complete between the last record and the suspend, or there is nothing to
# restore: with idle-after at 2m here, a 20s interval; in production idle-after is days.
perl -pi -e 's#upgradeMode: savepoint#upgradeMode: last-state#; s#^(\s+)(execution.checkpointing.dir: .*)$#$1$2\n$1execution.checkpointing.interval: 20 s#' "$m"
grep -q 'upgradeMode: last-state' "$m" && grep -q 'interval: 20 s' "$m" || { echo "rendering the last-state manifest failed"; exit 1; }
k -n $ns apply -f "$m"
wait_for '{.status.jobStatus.state}' RUNNING 300
produce 'printf "a\nb\nc\n"'
install_siesta

echo "expect suspend after idle-after (2m) + min-awake (30s)"
wait_for '{.spec.job.state}' suspended 360
wait_for '{.status.lifecycleState}' SUSPENDED 180
sp=$(k -n $ns get flinkdeployment example -o jsonpath='{.status.jobStatus.upgradeSavepointPath}')
echo "upgradeSavepointPath after a last-state suspend: '${sp}'"
# Give the controller a tick to judge the completed suspend.
sleep 75
echo "SuspendedWithoutSavepoint events: $(occurrences SuspendedWithoutSavepoint)"
[ "$(occurrences SuspendedWithoutSavepoint)" = 0 ] || { echo "a last-state suspend takes no savepoint by design and must not warn"; events; exit 1; }

echo "produce one record, expect resume from the last checkpoint"
produce 'echo d'
wait_for '{.spec.job.state}' running 120
wait_for '{.status.jobStatus.state}' RUNNING 300
jm=$(k -n $ns get pods -l app=example,component=jobmanager -o jsonpath='{.items[0].metadata.name}')
log=$(k -n $ns logs "$jm")
grep -q "No checkpoint found during restore" <<<"$log" && { echo "the job restarted from scratch, not from its last checkpoint"; exit 1; }
grep -i -E "restoring job .* from checkpoint" <<<"$log" | head -1 | grep -q . \
  || { echo "JobManager log has no checkpoint restore line"; grep -i -E "checkpoint|savepoint" <<<"$log" | head; exit 1; }
events | grep -E "^(Suspended|Resumed|SuspendedWithoutSavepoint)" | tail -4
scenario_end last-state
