#!/usr/bin/env bash
# Builds siesta-e2e-job:<flink version>: the job jar on the stock Flink image of that version.
# With E2E_JOB_JAR pointing at a jar from jar.sh the Maven stage is skipped, which is how CI
# keeps eighty parallel jobs from downloading the same dependencies from Maven Central.
set -euo pipefail
v=${1:-2.2}
here=$(cd "$(dirname "$0")" && pwd)
. "$here/versions.sh"
job_versions "$v"
from=build
if [ -n "${E2E_JOB_JAR:-}" ]; then
  cp "$E2E_JOB_JAR" "$here/e2e-job.jar"
  from=prebuilt
fi
DOCKER_BUILDKIT=1 docker build "$here" -t "siesta-e2e-job:$v" --build-arg JAR_FROM="$from" \
  --build-arg FLINK_VERSION="$v" --build-arg FLINK_MAVEN_VERSION="$maven" --build-arg KAFKA_CONNECTOR_VERSION="$connector"
