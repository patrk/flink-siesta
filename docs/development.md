# Development

Every design decision is recorded in [`adr/`](adr/), one file per decision. Read them in order. The later ones amend the earlier ones where the code moved on.

## Built with

Go, controller-runtime and franz-go. The image is about 20 MB and starts in under a second. The FlinkDeployment is handled as an unstructured object, since the CRD is external and only a handful of fields are read.

## Testing

    make test      the pure decision table, the state codec and the store, no Docker needed
    make it        the Kafka probe against Confluent's image, and against Redpanda with SASL_SSL, SCRAM and TLS
    make envtest   the reconciler on a real kube-apiserver with the FlinkDeployment CRD
    make e2e       KinD with the Flink operator, Kafka and a Kafka-reading job built from e2e/job. Five
                   scenarios, each standing on its own: suspend, savepoint restore and resume with a
                   controller restart in between, sources verified against the job, drift reported and a
                   burst held back by the job gate, a Kafka outage and a recreated topic, the admission
                   policy and garbage collection, the restart budget, sources learned from the job. Each runs in its own namespace,
                   E2E_PARALLEL at a time (default 2, 1 for a small machine), or one alone:
                   make e2e E2E_SCENARIO=outage. CI runs them on separate clusters.
    make bench     one worker over 200 deployments on envtest, reports reconciles per minute
    make soak      a simulated soak: sources flapping, probes failing, writes dropped, a fake operator
                   reacting late, and the controller crashed every 400 ticks. Asserts consistency and
                   flat memory. 30 deployments over 20 simulated hours by default, which takes minutes.
                   The nightly runs SOAK_DEPLOYMENTS=100 SOAK_TICKS=3000.
    make e2e-chaos the leader killed mid-transition, and the operator away while we act
    make e2e-soak  45 real minutes on KinD: three jobs, random traffic, the controller killed every
                   10 minutes, one Kafka outage. Asserts consistency and flat RSS.

Every push runs the first four on Flink 2.2 and operator 1.15. A tag runs the compatibility grid first, operator 1.13 to 1.16 by Flink 1.20, 2.0 and 2.2, plus Flink 2.3 on operator 1.16, one box per operator in the run, and publishes only when all of it is green. The same grid runs weekly on main, and by hand from the Actions tab with the tag field left empty. The nightly runs the chaos and soak scenarios and the benchmark, and gates nothing.

CI compiles once per run: the controller image and the job jar per Flink version are built by `prepare.yml` and handed to every e2e job as artifacts, so eighty clusters do not each ask Maven Central for the same dependencies. Locally `make e2e` still builds both itself.

Changes go to main through a pull request, squash merged once ci is green. A branch ruleset enforces it and names the checks. Tags are pushed from main after the grid.

To get going:

    make deps         # once, pins the dependencies
    make test         # seconds, no Docker and no cluster
    make kind-up e2e  # a KinD cluster with the operator and Kafka, then the five scenarios

## Scale

One reconcile is one cached read of the deployment, one uncached read of its ConfigMap, one Kafka admin call for the end offsets plus one for the group's committed offsets when `consumer-group` is set, and a write only when something changed. The save compares against the ConfigMap the tick loaded instead of reading it again. Every deployment is visited once per `--poll-interval`.

A tick mostly waits: on the broker, on the job's REST API with `idle: job` or `sources: auto`, and on the API server. `--max-concurrent-reconciles` (default 4) sets how many deployments wait at once; one deployment is never reconciled twice at once. Size it so a pass fits in the poll interval: with ticks of 50 ms, 4 workers visit about 4,800 deployments a minute. The worst case is a broker that swallows connections, where every tick waits out `--probe-timeout` on Kafka: 1,300 deployments, 10 s each and 4 workers take about 54 minutes a pass, against 3.6 hours on one worker. Nothing acts on an unknown reading, so a slow pass delays decisions and never makes a wrong one. For a large fleet, raise the workers to 16 or 32 and lower `--probe-timeout`.

The Kubernetes client is rate limited to `--kube-api-qps` 20 and `--kube-api-burst` 30. A quiet tick costs one request and a transition three or four, so the default carries about a thousand quiet deployments a minute with room for transitions. Raise both with the workers. The Kafka calls are not batched across deployments: at 1,300 deployments with a consumer group that is about 45 admin requests a second, well inside what a broker serves.

`make bench` measures the controller's own overhead, not capacity. It comes to about 2 ms per reconcile on an Apple M4 Pro, against a local envtest API server with a fake probe on the quiet path. Add your Kafka and API server round trips to that.

Horizontal scale is per namespace: one instance, one Kafka cluster, one credential. Replicas exist for failover, not throughput. Leader election keeps one active.

## The README illustration

`docs/siesta-light.svg` and `docs/siesta-dark.svg` are drawn by `docs/illustration/draw.js`, so the sketch can be changed in code rather than in a drawing tool. rough.js draws the shapes and opentype.js turns the labels into outlines of Patrick Hand, a handwriting face under the SIL Open Font License, because GitHub shows the image through an `<img>` tag that cannot load fonts. GitHub picks the variant that matches the reader's colour scheme.

    npm install --no-save roughjs opentype.js
    node docs/illustration/draw.js
