# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Structure

This is a monorepo for multi-ancestry GWAS fine-mapping using SuShiE. Two components:

1. **Root-level Nextflow pipeline** — `main.nf` + `conf/` + `tests/`. Runs `FINE_MAPPING` workflow orchestrating the full pipeline. Requires Nextflow `>=25.10.0` (the `nf-schema` plugin's minimum).

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
# Run the canonical local integration profile (uses testdata/manifest.full.tsv)
make integration-test

# Google Cloud profile
nextflow run main.nf -profile googleCloud

# Run the canonical profile directly
nextflow run main.nf -profile testFullCollectorHailingDucks
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

- `nextflow.config` — shared Docker, shell, environment, plugin, validation, resource, and reporting defaults
- `conf/test-full-collector-hailing-ducks.config` — Local full-data integration profile for the collector locus breaker and Hailing Ducks LD annotation
- `conf/google-cloud.config` — Google Cloud Batch production profile using Collector and Hailing Ducks
- `conf/google-cloud-test.config` — Google Cloud Batch staging/test profile using Collector and Hailing Ducks

Key pipeline parameters: `params.manifest`, `params.ld_registry`, `params.output_dir`. See `nextflow_schema.json` for the full, authoritative parameter list — `validateParameters()` (nf-schema) rejects any parameter not declared there.

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
- Process labels map to resource configs in `conf/test-full-collector-hailing-ducks.config`
