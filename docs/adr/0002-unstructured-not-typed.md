# 2. FlinkDeployment is handled as an unstructured object

**Context.** The FlinkDeployment CRD is external (apache/flink-kubernetes-operator) and has no official Go client. We can generate types from the CRD with controller-gen, write a partial typed struct with deep-copy generation, or use `unstructured`.

**Decision.** `unstructured.Unstructured` with a single package, `internal/flink`, that maps the six paths we read and the three we write.

**Consequences.** No generated code, no drift when the operator adds fields, one file to change if a path moves. The cost is string paths instead of fields checked at compile time. Unit tests cannot catch a mistyped path, `unstructured` just returns empty, but the envtest suite can, because it runs against the real CRD schema and the API server prunes a field that does not exist.
