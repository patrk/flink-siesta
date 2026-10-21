#!/usr/bin/env bash
# ADR 15 against the real operator: a stateless job, no checkpoints, auto-commit, its Kafka
# source starting from the group's committed offsets. It sleeps caught up, resumes where the
# group left off, is woken by max-sleep before the broker expires the group's offsets, and a
# resume whose offsets are gone is held until they are back. Every record each run read is
# copied to e2e-out, so a replay shows up as a duplicate there.
source "$(dirname "$0")/../lib.sh"
scenario_start group-offsets

# The broker forgets a group's offsets four minutes after its last commit, checked every 30s.
# A job asleep longer than that keeps them only through the max-sleep wakes.
k -n $ns scale deploy/kafka --replicas=0
until [ -z "$(k -n $ns get pods -l app=kafka -o name)" ]; do sleep 2; done
k -n $ns set env deploy/kafka KAFKA_OFFSETS_RETENTION_MINUTES=4 KAFKA_OFFSETS_RETENTION_CHECK_INTERVAL_MS=30000
k -n $ns scale deploy/kafka --replicas=1
k -n $ns rollout status deploy/kafka --timeout=120s
until kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; do sleep 2; done
kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type brokers --entity-name 1 --all \
  | grep -q 'offsets.retention.minutes=4 ' || { echo "the broker did not take the short offset retention"; exit 1; }
topic --create --if-not-exists --topic e2e-out --partitions 1 >/dev/null

groups() { kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group e2e "$@"; }
committed() { groups --describe 2>/dev/null | awk '$2 == "e2e-in" && $3 == 0 { print $4; f = 1 } END { if (!f) print "-" }'; }
copied() { kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic e2e-out \
  --from-beginning --timeout-ms 10000 2>/dev/null | paste -sd, -; }
# The first runs of this scenario are in CI: on a failure, say what the controller and the group saw.
on_fail() { echo "--- events"; events || true; echo "--- controller"; k -n $ns logs deploy/siesta --tail=80 || true
  echo "--- group e2e"; groups --describe || true; echo "--- e2e-out: $(copied)"; }
trap '[ $? = 0 ] || on_fail' EXIT
expect_copied() { # $1 = the records e2e-out must hold, in order, each once
  local i=0; until [ "$(copied)" = "$1" ]; do
    i=$((i+15)); [ $i -ge 120 ] && { echo "e2e-out must hold exactly $1, holds $(copied)"; exit 1; }; sleep 5; done; }
expect_committed() {
  local i=0; until [ "$(committed)" = "$1" ]; do
    i=$((i+5)); [ $i -ge 90 ] && { echo "group e2e must have committed $1 on e2e-in, has $(committed)"; exit 1; }; sleep 5; done; }

# The example, made stateless: no HA, no savepoints, the position in group e2e.
m=$(render "$here/flinkdeployment.yaml")
perl -ni -e 'print unless /high-availability/' "$m"
perl -pi -e 's#upgradeMode: savepoint#upgradeMode: stateless#;
  s#"--group", "e2e"\]#"--group", "e2e", "--starting-offsets", "committed", "--auto-commit-ms", "5000", "--out", "e2e-out"]#;
  s#siesta.flink.io/idle-after: 2m#siesta.flink.io/idle-after: 1m#;
  s#siesta.flink.io/min-awake: 30s#siesta.flink.io/min-awake: 90s#;
  s#^(\s+)siesta.flink.io/idle: job$#$&\n$1siesta.flink.io/consumer-group: e2e\n$1siesta.flink.io/resume-from: group-offsets#' "$m"
grep -q 'upgradeMode: stateless' "$m" && grep -q 'resume-from: group-offsets' "$m" && grep -q -- '--auto-commit-ms' "$m" \
  || { echo "rendering the stateless manifest failed:"; cat "$m"; exit 1; }
k -n $ns apply -f "$m"
wait_for '{.status.jobStatus.state}' RUNNING 300

echo "auto-commit commits the position with nothing read: what a max-sleep wake relies on"
expect_committed 0
produce 'printf "before-1\nbefore-2\nbefore-3\n"'
expect_committed 3
expect_copied before-1,before-2,before-3

install_siesta --set config.pollInterval=20s

echo "caught up: the job sleeps, with no savepoint and no warning about one"
wait_for '{.spec.job.state}' suspended 300
wait_for '{.status.lifecycleState}' SUSPENDED 180
[ "$(occurrences SuspendedWithoutSavepoint)" = 0 ] || { echo "no SuspendedWithoutSavepoint for a job that resumes from the group"; events; exit 1; }
[ "$(occurrences Refused)" = 0 ] || { echo "resume-from: group-offsets must not be refused"; events; exit 1; }

echo "one record wakes it, and it reads only that record"
produce 'echo after-sleep'
wait_for '{.spec.job.state}' running 120
wait_for '{.status.jobStatus.state}' RUNNING 300
expect_committed 4
expect_copied before-1,before-2,before-3,after-sleep

echo "max-sleep 1m: woken twice without input, the group keeps its offsets past the 4m retention"
k -n $ns annotate flinkdeployment example --overwrite siesta.flink.io/max-sleep=1m
wait_for '{.spec.job.state}' suspended 300
wait_for '{.status.lifecycleState}' SUSPENDED 180
EXPECT=3 until_count "two max-sleep wakes" 720 occurrences Resumed
# Only a max-sleep wake ends in this reason; a series may fold its Resumed message away.
wait_on '{.metadata.annotations.siesta\.flink\.io/reason}' 'committed again' 300
wait_for '{.status.lifecycleState}' SUSPENDED 180
[ "$(occurrences ResumeHeld)" = 0 ] || { echo "the offsets expired although max-sleep woke the job"; events; exit 1; }
expect_committed 4
expect_copied before-1,before-2,before-3,after-sleep

echo "offsets gone while asleep: the resume is held, then happens on the held input once restored"
k -n $ns annotate flinkdeployment example --overwrite siesta.flink.io/max-sleep=0
wait_for '{.spec.job.state}' suspended 300
wait_for '{.status.lifecycleState}' SUSPENDED 180
groups --delete-offsets --topic e2e-in >/dev/null
expect_committed -
produce 'echo held'
EXPECT=1 until_count "one ResumeHeld" 120 occurrences ResumeHeld
[ "$(k -n $ns get flinkdeployment example -o jsonpath='{.spec.job.state}')" = suspended ] || { echo "a held resume must not resume"; exit 1; }
reason | grep -q 'resume held' || { echo "the reason must say the resume is held: $(reason)"; exit 1; }
groups --reset-offsets --topic e2e-in:0 --to-offset 4 --execute >/dev/null
wait_for '{.spec.job.state}' running 120
wait_for '{.status.jobStatus.state}' RUNNING 300
expect_copied before-1,before-2,before-3,after-sleep,held
[ "$(occurrences SuspendedWithoutSavepoint)" = 0 ] || { echo "no SuspendedWithoutSavepoint on any sleep"; events; exit 1; }
events | grep -E '^(Suspended|Resumed|ResumeHeld)'
scenario_end group-offsets
