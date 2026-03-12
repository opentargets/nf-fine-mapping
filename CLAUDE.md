# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Structure

This is a monorepo for multi-ancestry GWAS fine-mapping using SuShiE. Two components:

1. **Root-level Nextflow pipeline** — `main.nf` + `conf/` + `tests/`. Runs `FINE_MAPPING` workflow orchestrating the full pipeline. Requires Nextflow `>=24.10.5`.

2. **`modules/`** — Standalone Nextflow process modules:
   - `collect/` — Merges per-trait/ancestry parquet directory into a single parquet file
   - `intersect/` — Finds variants present across ALL ancestries for a trait (SQL `GROUP BY chrom, pos, ref, alt HAVING COUNT(*) = n`)
   - `transform/` — Reshapes parquet to SuShiE-compatible gzip TSV (extracts alleles via regex, computes zScore = beta/se)
   - `ld/` — Subsets LD matrices using `subset_ld` for relevant variants
   - `sushie/` — Runs multi-ancestry fine-mapping

3. **`tools/collector/`** — Python CLI tool (Python 3.12+, DuckDB, Typer) providing `collect`, `intersect`, and `transform` subcommands. This is built into the `collector:latest` container image used by the first three modules.

The root `.nf-core.yml` sets `repository_type: modules` with `org_path: opentargets`.

## Running the Pipeline

```bash
# Run with test profile (uses testdata/ directory)
nextflow run main.nf -profile test

# Google Cloud profile
nextflow run main.nf -profile googleCloud

# Use -profile arm on Apple Silicon alongside docker
nextflow run main.nf -profile test,arm
```

## Running Tests

```bash
# Run pipeline nf-test (from repo root)
nf-test test

# Run a specific test file
nf-test test tests/default.nf.test

# Run stub tests (no container needed)
nf-test test tests/default.nf.test --stub
```

For the Python collector tool:

```bash
cd tools/collector
uv run pytest
uv run pytest tests/collector/test_cli.py::test_collect  # single test
```

## Building Containers

The `collector` container (used by collect/intersect/transform modules) is built via GitHub Actions (`collector-docker.yml`). Version is read from `tools/collector/pyproject.toml`. Multi-arch: linux/amd64 and linux/arm64.

The `sushie` container is defined in `modules/sushie/Dockerfile` (Python 3.10 + SuShiE v0.19).

## Workflow Architecture

The `FINE_MAPPING` workflow in `main.nf` orchestrates channels through these stages:

```
manifest.tsv (trait, ancestry, sampleSize, summaryStatisticsPath)
  → Collect      (per trait+ancestry: merge parquet dir → single parquet)
  → group_by_trait()
  → Intersect    (per trait: find common variants across all ancestries)
  → mix_with_intersection()
  → Transform    (per trait+ancestry: parquet → gzip TSV for SuShiE)
  → mix_with_ld()
  → SubsetLD     (per ancestry: extract relevant LD variants)
  → annotate_with_ld()
  → SuShiE       (per trait: multi-ancestry fine-mapping)
```

Channel manipulation helpers (`group_by_trait`, `mix_with_intersection`, `mix_with_ld`, `annotate_with_ld`) are defined at the bottom of `main.nf`. They use `combine()` and `groupTuple()` to join channels on trait or ancestry keys.

The **`meta` map** carries `trait`, `sampleSize`, `ancestry` (and sometimes `leadVariantId`, `ldPopulation`) through all process input/output tuples.

## Configuration

- `conf/base.config` — Docker enabled by default; sets Python env vars
- `conf/test.config` — Local executor, resource limits (4 CPUs, 15GB, 1h), paths to `testdata/`
- `conf/google-batch.config` — Google Cloud Batch profile

Key pipeline parameters: `params.manifest`, `params.ld_reference`, `params.output_dir`, `params.chain`, `params.liftover`, `params.r2`.

## Test Data

`testdata/manifest.tsv` — 4 rows: traits A & B × NFE & AFR ancestries, pointing to parquet directories under `testdata/eur_gwas/` and `testdata/afr_gwas/`.

`testdata/ld_reference.tsv` — LD matrix paths for AFR, AMR, EAS, FIN, NFE ancestries (gnomAD v2.1.1).

## Data Flow Formats

- **Input**: Parquet summary statistics (`variantId`, `beta`, `standardError`, `chromosome`, `position`)
- **Intermediate**: Parquet (after collect/intersect steps)
- **SuShiE input**: Gzip TSV — columns: `chromosome variantId position referenceAllele alternateAllele zScore`
- **SuShiE output**: Published to `params.output_dir/sushie/` (correlation, credible sets, weights, logs)

## Module Conventions

All processes follow nf-core DSL2 patterns:
- Input tuples: `tuple val(meta), path(...)`
- Always emit a `versions` channel with `versions.yml`
- Use `task.ext.args` for extra CLI arguments
- Include `stub:` block for dry-run testing
- Process labels map to resource configs in `conf/test.config`
