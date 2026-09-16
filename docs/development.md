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

One reconcile is one cached read of the deployment, one read of its ConfigMap, one or two Kafka admin calls, and a write only when something changed. Every deployment is visited once a minute. Tens to low hundreds of deployments per namespace run on the default single worker with plenty of headroom. Around a thousand, raise `MaxConcurrentReconciles` and the client rate limit, or batch the Kafka calls per tick.

`make bench` measures the controller's own overhead, not capacity. It comes to about 2 ms per reconcile on an Apple M4 Pro, against a local envtest API server with a fake probe on the quiet path. Add your Kafka and API server round trips to that. With 10 ms to Kafka and 5 ms to the API server, one worker handles a few thousand deployments per minute, which is far beyond the population this is built for.

Horizontal scale is per namespace: one instance, one Kafka cluster, one credential. Replicas exist for failover, not throughput. Leader election keeps one active.

## The README illustration

`docs/siesta-light.svg` and `docs/siesta-dark.svg` are drawn by `docs/illustration/draw.js`, so the sketch can be changed in code rather than in a drawing tool. rough.js draws the shapes and opentype.js turns the labels into outlines of Patrick Hand, a handwriting face under the SIL Open Font License, because GitHub shows the image through an `<img>` tag that cannot load fonts. GitHub picks the variant that matches the reader's colour scheme.

    npm install --no-save roughjs opentype.js
    node docs/illustration/draw.js
