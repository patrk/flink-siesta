#!/usr/bin/env bash
# Builds the job jar with a local Maven, for CI, which builds it once per Flink version and
# hands it to every e2e job through E2E_JOB_JAR. Locally build.sh does the same inside Docker.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
. "$here/versions.sh"
job_versions "${1:-2.2}"
mvn -q -B -f "$here/pom.xml" package -Dflink.version="$maven" -Dkafka.connector.version="$connector"
ls -l "$here/target/e2e-job.jar"
