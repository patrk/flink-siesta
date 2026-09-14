#!/usr/bin/env bash
# Builds siesta-e2e-job:<flink version>. One Kafka connector line per supported Flink version.
set -euo pipefail
v=${1:-2.2}
case "$v" in
  1.20) maven=1.20.4; connector=3.4.0-1.20 ;;
  2.0)  maven=2.0.2;  connector=4.0.1-2.0 ;;
  2.1)  maven=2.1.2;  connector=5.0.0-2.1 ;;
  2.2)  maven=2.2.1;  connector=5.0.0-2.2 ;;
  *) echo "no connector mapping for Flink $v"; exit 1 ;;
esac
here=$(cd "$(dirname "$0")" && pwd)
DOCKER_BUILDKIT=1 docker build "$here" -t "siesta-e2e-job:$v" \
  --build-arg FLINK_VERSION="$v" --build-arg FLINK_MAVEN_VERSION="$maven" --build-arg KAFKA_CONNECTOR_VERSION="$connector"
