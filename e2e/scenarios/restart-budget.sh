#!/usr/bin/env bash
# A job that fails terminally is restarted twice, then marked unrecoverable.
source "$(dirname "$0")/../lib.sh"
scenario_start restart-budget
install_siesta
fd=failing
k -n $ns apply -f "$(render "$here/failing.yaml")"
# Intermediate states last seconds; assert the end state and the trail of events instead.
wait_on '{.metadata.annotations.siesta\.flink\.io/state}' unrecoverable 600
k -n $ns get configmap siesta-failing -o jsonpath='{.data.restarts}' | grep -q '"count":2' \
  || { echo "expected the restart budget to be fully used"; exit 1; }
[ "$(occurrences Restarted)" = 2 ] || { echo "expected exactly two Restarted events:"; events | grep -E "^(Restarted|Unrecoverable)"; exit 1; }
[ "$(occurrences Unrecoverable)" = 1 ] || { echo "expected an Unrecoverable event:"; events; exit 1; }
events | grep -E "^(Restarted|Unrecoverable)"
k -n $ns delete flinkdeployment failing --wait=false
scenario_end restart-budget
