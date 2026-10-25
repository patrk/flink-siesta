# Security

## Reporting a vulnerability

Report it privately through GitHub: **Security → Report a vulnerability** on this repository. Please do not open a public issue. You get an answer within a week, and a fix or a mitigation is released before the details are published.

## Supported versions

The latest minor release gets fixes. Until 1.0 that is one line, for example 0.5.x.

## What the controller can do

The controller's identity can patch FlinkDeployments and write ConfigMaps in its namespace. Two ValidatingAdmissionPolicies in the chart cap that: on a FlinkDeployment it may change only `spec.job.state`, `spec.restartNonce` and annotations under its prefix, and it may write only its own `siesta-<deployment>` ConfigMaps. A compromised controller can therefore suspend, resume or restart the jobs it watches, and nothing else. See [Least privilege](docs/install.md#least-privilege).

## Releases

Images are signed with cosign keyless signing in the release workflow and carry SBOM and provenance attestations. Verify an image before you run it:

    cosign verify ghcr.io/patrk/flink-siesta:v0.5.0 \
      --certificate-identity-regexp '^https://github.com/patrk/flink-siesta/.github/workflows/release.yml@refs/' \
      --certificate-oidc-issuer https://token.actions.githubusercontent.com

Base images are pinned by digest, the release job's actions by commit, and `govulncheck` runs on every pull request and every night.
