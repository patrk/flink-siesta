# The e2e job image: the job jar on top of the stock Flink image of the version under test.
# Build args pick the runtime image and the matching Maven artifacts, see build.sh.
ARG FLINK_VERSION=2.2
FROM maven:3.9-eclipse-temurin-17 AS build
ARG FLINK_MAVEN_VERSION=2.2.1
ARG KAFKA_CONNECTOR_VERSION=5.0.0-2.2
WORKDIR /job
COPY pom.xml .
COPY src src
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -B package -Dflink.version=${FLINK_MAVEN_VERSION} -Dkafka.connector.version=${KAFKA_CONNECTOR_VERSION}

FROM flink:${FLINK_VERSION}
COPY --from=build /job/target/e2e-job.jar /opt/flink/usrlib/e2e-job.jar
