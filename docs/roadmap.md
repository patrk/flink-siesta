# Roadmap

## 1.0

1.0 means the controller has run for thirty days on a real cluster with more than twenty jobs and no manual intervention, and the compatibility grid, chaos and soak suites have been green for a month. Nothing in the list below is needed for that; time on real clusters is.

## Not yet

Known gaps, each one waiting for someone who needs it. Open an issue if it is you.

- **AWS MSK IAM authentication.** OAUTHBEARER, SCRAM, PLAIN and mTLS are supported. MSK IAM needs the AWS SDK for its credentials, a large dependency for one provider.
- **A Kafka cluster per deployment.** One controller watches one Kafka cluster with one credential. The `bootstrap-servers` annotation is reserved for this and refused until it exists. Today, run one controller per Kafka cluster, each in its own namespace.
- **FlinkSessionJob.** Application mode only: a session job has no pods of its own to take down.
- **Batched Kafka calls across deployments.** Every deployment asks the broker on its own, about 45 admin requests a second at 1,300 deployments with a consumer group. That is well within what a broker serves; batching per tick would cut it to a few.
- **End-to-end runs with SASL, TLS and OAuth against a broker in the cluster, and with several namespaces under one controller.** The authentication paths are tested against real brokers in the integration suite, not under the operator.
