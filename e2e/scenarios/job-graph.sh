#!/usr/bin/env bash
# The running job is asked: sources verified once per job instance, drift reported when the
# job changes, and the job gate holding a suspend while a burst drains (ADR 12, ADR 13).
source "$(dirname "$0")/../lib.sh"
scenario_start job-graph
deploy_example
install_siesta

echo "the job reads exactly the annotated topic: SourcesVerified once for this job instance"
EXPECT=1 until_count "one SourcesVerified" 180 occurrences SourcesVerified
[ "$(occurrences SourcesDrift)" = 0 ] || { echo "no drift expected yet"; exit 1; }

echo "make the job read a second topic, and slow it down: the new job instance must be reported as drift"
k -n $ns patch flinkdeployment example --type merge -p "{\"spec\":{\"job\":{\"args\":[\"--bootstrap\",\"kafka.$ns.svc:9092\",\"--topics\",\"e2e-in,e2e-other\",\"--group\",\"e2e\",\"--sleep-ms\",\"1000\",\"--max-poll-records\",\"50\"]}}}"
# The burst goes in now, while the operator upgrades the job: it restarts the idle clock, and the
# new job instance restores its position from the savepoint and works through the burst slowly.
produce 'seq 400'
EXPECT=1 until_count "a SourcesDrift event after the job changed" 420 occurrences SourcesDrift
events | grep '^SourcesDrift' | grep -q 'e2e-in,e2e-other' || { echo "drift message must name the job's topics:"; events | grep '^SourcesDrift'; exit 1; }
wait_for '{.status.jobStatus.state}' RUNNING 300

echo "the job gate: 400 records at one record per second keep the suspend back until the job has emitted them all"
i=0; until reason | grep -q 'job busy'; do
  i=$((i+5)); [ $i -ge 300 ] && { echo "expected a 'job busy' reason while the burst drains; reason is: $(reason)"; k -n $ns get cm siesta-example -o jsonpath='{.data}'; exit 1; }; sleep 5; done
echo "held by: $(reason)"
wait_for '{.spec.job.state}' suspended 480
wait_for '{.status.lifecycleState}' SUSPENDED 180
echo "job-graph OK"
