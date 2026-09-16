# ADR 006: Package the validation Lambda as a container image

- **Status:** Accepted
- **Date:** 2026-09-15

## Context

The validation adapter uses PyArrow to create explicit-schema Snappy Parquet. PyArrow
ships native Linux binaries and is too large to treat like a small pure-Python
dependency. Packaging must be reproducible from Windows while producing a Linux amd64
artifact compatible with Lambda.

## Options considered

| Option | Benefit | Cost or risk |
|---|---|---|
| ZIP plus a custom layer | Small application ZIP; the team owns the layer | Requires a separate Linux-native build, layer versioning, and coordinated application/layer deployment limits |
| Public or prebuilt layer | Little packaging work | Adds an external binary publisher and weakens provenance and version control |
| Lambda container image | One inspectable artifact with application and native dependency; reliable size budget | Adds ECR, image scanning/lifecycle, and explicit build/push/digest steps |

## Decision

Use the official AWS Lambda Python 3.12 base image for Linux amd64, pinned by image
manifest digest. Install the CPython 3.12 Linux amd64 PyArrow wheel with an exact version
and SHA-256 hash, copy only the `metropulse` production package, and configure
`metropulse.aws.lambda_handler.lambda_handler` as the image command. The AWS SDK remains
the version supplied by the digest-pinned Lambda base image.

Terraform accepts only an ECR image URI ending in `@sha256:<digest>`. ECR and Lambda must
be in the same Region. Tags provide build discovery, but deployment uses the digest so a
tag cannot change an existing release. Lambda supplies its default least-privileged
runtime user because the Dockerfile does not override `USER`; application temporary
writes, if later needed, are restricted to `/tmp`.

## Consequences

- The binary environment can be built and inspected consistently from any machine with
  Linux amd64 Docker support.
- ECR must be bootstrapped before the platform: create repository, build, push, obtain
  digest, then pass the digest-pinned URI to the platform stack.
- Each dependency or application change produces a new image and digest. Updating the
  platform variable creates a Lambda image revision; mutable tag-only deployment is
  rejected.
- Phase 4 defines packaging and infrastructure. The user-run Linux amd64 container
  verification passed before the Phase 4 commit; no AWS deployment evidence exists.
