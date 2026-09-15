#!/usr/bin/env bash
# sources: auto. The topics are learned from the running job and remembered, the job sleeps on
# them, and the remembered list wakes it while it has no JobManager to ask (ADR 14).
source "$(dirname "$0")/../lib.sh"
scenario_start sources-auto
manifest=$(render "$here/flinkdeployment.yaml")
sed -i.bak 's#sources: e2e-in#sources: auto#' "$manifest" && rm -f "$manifest.bak"
k -n $ns apply -f "$manifest"
wait_for '{.status.jobStatus.state}' RUNNING 300
install_siesta

echo "the topics are learned from the job and remembered"
EXPECT=1 until_count "a SourcesLearned event" 180 occurrences SourcesLearned
events | grep '^SourcesLearned' | grep -q 'e2e-in' || { echo "the learned list must name the topic:"; events | grep '^Sources'; exit 1; }
k -n $ns get cm siesta-example -o jsonpath='{.data.sources}' | grep -q '^e2e-in$' || { echo "memory must hold the learned sources"; k -n $ns get cm siesta-example -o jsonpath='{.data}'; exit 1; }

echo "idle on the learned topic: suspend"
wait_for '{.spec.job.state}' suspended 360
wait_for '{.status.lifecycleState}' SUSPENDED 180

echo "input on the remembered topic wakes the job, which has no JobManager to ask"
produce 'echo hello'
wait_for '{.spec.job.state}' running 120
wait_for '{.status.jobStatus.state}' RUNNING 300
scenario_end sources-auto
