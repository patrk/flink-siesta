# 4. Go with controller-runtime

**Context.** The Flink Kubernetes Operator is Java on Java Operator SDK, so a Java implementation would share its types and be the natural shape for an upstream contribution. Kubernetes controllers at large are Go on controller-runtime.

**Decision.** Go. controller-runtime for the manager, cache, leader election and envtest, franz-go for Kafka.

**Consequences.** approx. 20 MB image, sub-second start. An upstream contribution would be a rewrite. 
That would carry over is the annotation contract, the decision table and its tests.