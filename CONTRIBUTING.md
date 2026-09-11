# Contributing

This repository doubles as a teaching project. Decisions are recorded in
`docs/adr/` at the time they are made.

The code is pair-written with an AI assistant. Every design decision, every line, and every test must be reviewed and understood by the contributor before a PR is created. Contributions are welcome on the same terms: use whatever tools you like,
and be able to explain what you submit.

Before opening a PR:

    make lint test        # golangci-lint, gofmt, vet, pure tests
    make envtest          # controller against a local kube-apiserver

Conventions:

- Packages are nouns and small. `decide` is pure and never imports Kubernetes or Kafka or any other vendor specific packages.
- Errors are returned, wrapped with `%w`, and checked with `errors.Is`.
- No interface with a single implementation unless a test needs to fake it.
- A new source is a new probe in `internal/probe`.
