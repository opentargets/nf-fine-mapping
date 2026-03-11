# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Structure

This is a monorepo with two distinct components:

1. **`pipeline/`** — An nf-core-based Nextflow pipeline (`opentargets/finemapping`) for fine-mapping GWAS loci. Follows nf-core DSL2 conventions. Requires Nextflow `>=24.10.5`.

2. **`modules/`** — Standalone Nextflow modules developed independently:
   - `modules/opentargets/gentropy/` — Module wrapping the [gentropy](https://github.com/opentargets/gentropy) tool for LD preparation (`GENTROPY_LD_PREPARE` process). Includes its own `nextflow.config`, `conf/`, and `tests/`. The `gentropy/` subdirectory is a git submodule pointing to `git@github.com:opentargets/gentropy.git`.
   - `modules/sushie/` — Module wrapping [SuShiE](https://github.com/mancusolab/sushie) for multi-ancestry fine-mapping.

The root `.nf-core.yml` sets `repository_type: modules` with `org_path: opentargets`, which means nf-core tooling treats this repo as a modules repository.

## Running the Pipeline

From `pipeline/`:

```bash
# Run with test profile (requires docker or singularity)
nextflow run main.nf -profile test,docker --outdir results/

# Run with custom samplesheet
nextflow run main.nf -profile docker --input samplesheet.csv --outdir results/
```

Use `-profile arm` on Apple Silicon alongside `docker` to force `linux/amd64` emulation.

## Running Module Tests

From `pipeline/`, using [nf-test](https://www.nf-test.com/):

```bash
# Run all pipeline tests
nf-test test

# Run a specific test file
nf-test test tests/default.nf.test
```

From `modules/opentargets/gentropy/`, using nf-test:

```bash
# Run the gentropy module tests
nf-test test tests/main.nf.test

# Run a single test by name
nf-test test tests/main.nf.test --filter "sumstats - tsv"

# Run stub tests (no container needed)
nf-test test tests/main.nf.test --filter "sumstats - tsv - stub"
```

## Building Containers

The sushie container is defined in `modules/sushie/Dockerfile` (installs from GitHub via pip).

The gentropy container is built from the gentropy submodule:

```bash
# From modules/opentargets/gentropy/
make build-gentropy-image
# This runs: (cd gentropy && make build-docker)
```

The gentropy module currently pins to `docker.io/library/gentropy:000`. Update the version string in `modules/opentargets/gentropy/main.nf` when bumping the container.

## Architecture Notes

### Pipeline (`pipeline/`)

- `main.nf` → entry point, calls `FINEMAPPING` workflow and nf-core init/completion subworkflows
- `workflows/finemapping.nf` → main workflow (currently scaffolded with FastQC + MultiQC as placeholders)
- `conf/modules.config` → per-process `ext.args` and `publishDir` overrides
- `pipeline/modules/nf-core/` → vendored nf-core modules (fastqc, multiqc)
- `pipeline/subworkflows/` — nf-core utility subworkflows + local pipeline subworkflow

### Gentropy Module (`modules/opentargets/gentropy/`)

- `main.nf` — defines `GENTROPY_LD_PREPARE` process and a test workflow. The process runs `gentropy step=sushie_ld_input` to slice LD matrices for SuShiE input. Metadata (`leadVariantId`, `ldPopulation`) is parsed from the input path pattern `leadvariantId=<id>/ldPopulation=<pop>`.
- `conf/google-batch.config` — profile for running on Google Cloud Batch
- `tests/data/GCST90573121.h.tsv` — test GWAS summary statistics file

### Module Patterns

- Processes follow nf-core conventions: `meta` map in input tuples, `versions.yml` output, `task.ext.args` for extra args, `task.ext.prefix` for output naming
- Version strings are manually maintained in process scripts (no CLI version flag available from gentropy)
