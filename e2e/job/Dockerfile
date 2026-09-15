# The e2e job image: the job jar on top of the stock Flink image of the version under test.
# Build args pick the runtime image and the matching Maven artifacts, see build.sh. JAR_FROM=prebuilt
# takes e2e-job.jar from the build context instead of building it.
ARG FLINK_VERSION=2.2
ARG JAR_FROM=build

FROM maven:3.9-eclipse-temurin-17 AS build
ARG FLINK_MAVEN_VERSION=2.2.1
ARG KAFKA_CONNECTOR_VERSION=5.0.0-2.2
WORKDIR /job
COPY pom.xml .
COPY src src
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -B package -Dflink.version=${FLINK_MAVEN_VERSION} -Dkafka.connector.version=${KAFKA_CONNECTOR_VERSION}

FROM scratch AS prebuilt
COPY e2e-job.jar /job/target/e2e-job.jar

FROM ${JAR_FROM} AS jar

FROM flink:${FLINK_VERSION}
COPY --from=jar /job/target/e2e-job.jar /opt/flink/usrlib/e2e-job.jar
