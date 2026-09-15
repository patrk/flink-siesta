#!/usr/bin/env bash
# Kafka away and back, the topic deleted and recreated: reported once per edge, nothing changes
# while the source is unknown, and the recreated topic counts as input for the suspended job.
source "$(dirname "$0")/../lib.sh"
scenario_start outage
deploy_example
# A few records first, so the remembered end offset is not 0: a recreated topic starts at 0,
# and the last step needs that to differ from what was remembered.
produce 'seq 5'
install_siesta
echo "let the job consume the seed records and suspend, so the outage meets a sleeping job"
wait_for '{.spec.job.state}' suspended 360
wait_for '{.status.lifecycleState}' SUSPENDED 180

echo "take Kafka away: one SourceUnreachable, no transitions; bring it back: one SourceReachable"
k -n $ns scale deploy/kafka --replicas=0
EXPECT=1 until_count "a SourceUnreachable event" 180 occurrences SourceUnreachable
k -n $ns scale deploy/kafka --replicas=1
k -n $ns rollout status deploy/kafka --timeout=120s
EXPECT=1 until_count "a SourceReachable event after the broker returned" 180 occurrences SourceReachable
[ "$(transitions_while_unknown 1)" = 0 ] || { echo "nothing may change while the source is unknown:"; events | grep -E "^(Source|Suspended|Resumed|Restarted)"; exit 1; }

echo "delete the topic: unknown again, the job is left alone; recreate it: reachable again"
topic --delete --topic e2e-in
EXPECT=2 until_count "a second SourceUnreachable for the deleted topic" 180 occurrences SourceUnreachable
topic --create --if-not-exists --topic e2e-in --partitions 1
EXPECT=2 until_count "a second SourceReachable for the recreated topic" 180 occurrences SourceReachable
[ "$(transitions_while_unknown 2)" = 0 ] || { echo "nothing may change while the topic is missing:"; events | grep -E "^(Source|Suspended|Resumed|Restarted)"; exit 1; }
echo "the recreated topic starts at offset 0, which differs from what was remembered: that counts as input and the suspended job is resumed"
wait_for '{.spec.job.state}' running 120
scenario_end outage
