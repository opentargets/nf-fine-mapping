# Contributing

## Setup

```bash
make dev
```

## Local checks

Run these before opening a pull request:

```bash
make lint
make test
```

Changes should include tests for new behavior and preserve the Gentropy-compatible
schemas and output contracts. For performance-sensitive changes, record a
before-and-after runtime on the same input dataset.

The collector is part of the parent Nextflow repository, so pipeline and
container changes should also be verified from the repository root.
