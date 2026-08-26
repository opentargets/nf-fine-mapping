# Copilot Instructions

## What this repo does

Multi-ancestry GWAS fine-mapping pipeline using [SuShiE](https://github.com/mancusolab/sushie). Takes summary statistics (parquet) across ancestries and produces credible sets via Bayesian fine-mapping.

## Build & test commands

```bash
# Pipeline tests (from repo root, requires nf-test)
nf-test test                                        # all tests
nf-test test tests/default.nf.test                 # specific file
nf-test test tests/default.nf.test --stub           # dry-run, no containers

# Collector Python tool (from tools/collector/)
make test                                           # uv sync + pytest
uv run pytest tests/collector/test_cli.py::test_collect   # single test
uv run ruff check src/                              # lint

# Build collector container
make build                                          # docker build -t collector:latest

# Run the pipeline
nextflow run main.nf -profile test                  # local, testdata
nextflow run main.nf -profile test,arm              # Apple Silicon + docker
nextflow run main.nf -profile googleCloud           # Google Batch
```

## Architecture

Two components are tightly coupled:

1. **Python CLI tool** (`tools/collector/`) — Typer + DuckDB CLI built into the `collector:latest` container. Has three subcommands: `collect`, `intersection`, `transform`.

2. **Nextflow pipeline** (`main.nf` + `modules/`) — DSL2 workflow. The first three process modules each call a `collector` subcommand. The last two (`ld/`, `sushie/`) use separate containers.

```
manifest.tsv (trait, ancestry, sampleSize, summaryStatisticsPath)
  → Collect      parquet dir → single parquet          [collector collect]
  → Intersect    variants common across all ancestries  [collector intersection]
  → Transform    parquet → gzip TSV + zScore            [collector transform]
  → SubsetLD     extract LD matrix slice                [subset_ld binary]
  → SuShiE       multi-ancestry fine-mapping            [sushie]
```

Output lands in `params.output_dir/sushie/` as `.corr.tsv`, `.cs.tsv`, `.weights.tsv`, `.log`.

## Channel manipulation

Four helper functions at the bottom of `main.nf` manage data routing between processes. All use `combine()` or `groupTuple()` on `trait` or `ancestry` keys extracted from the `meta` map:

| Function | Joins on | Purpose |
|---|---|---|
| `group_by_trait(ch)` | trait | Pack all ancestries for a trait together for intersection |
| `mix_with_intersection(inter_ch, collected_ch)` | trait | Fan intersection result back to each ancestry's collected file |
| `mix_with_ld(transformed_ch, ld_ch)` | ancestry | Pair each transformed file with its LD matrix |
| `annotate_with_ld(transformed_ch, ld_ch)` | trait | Final grouping into per-trait tuples for SuShiE |

## Process module conventions

All modules follow nf-core DSL2 patterns:

- Input/output tuples always start with `tuple val(meta), path(...)`
- `meta` map carries: `trait`, `ancestry`, `sampleSize` (and `leadVariantId`, `ldPopulation` in later stages)
- Every process emits a `versions` channel writing `versions.yml`
- `task.ext.args` used for extra CLI flags (set in `conf/`)
- Every process has a `stub:` block for dry-run (`--stub`) testing

Example skeleton:
```groovy
process MyProcess {
    input:  tuple val(meta), path(input_file)
    output: tuple val(meta), path("output.*"), emit: results
            path "versions.yml",               emit: versions
    stub:   // create empty output files
    script:
    def args = task.ext.args ?: ''
    """
    some-tool $args $input_file
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        some-tool: \$(some-tool --version)
    END_VERSIONS
    """
}
```

## Container versioning

- **collector**: version from `tools/collector/pyproject.toml` → `ghcr.io/<org>/collector:<version>`. Built by `collector-docker.yml` on push to `main` when `tools/collector/` changes.
- **sushie**: version from `modules/sushie/VERSION` → `ghcr.io/<org>/sushie:<version>`. Built by `sushie-docker.yml`.
- Both are multi-arch (linux/amd64, linux/arm64).
- Local test profile uses `container = 'collector:latest'` (local build).

## Collector tool internals

- DuckDB executes all parquet operations as SQL queries
- `intersection` subcommand uses `GROUP BY chrom, pos, ref, alt HAVING COUNT(*) = n` to find variants present in all ancestries
- `transform` extracts `referenceAllele`/`alternateAllele` via regex from `variantId`, then computes `zScore = beta / standardError`

## Key pipeline parameters

| Parameter | Description |
|---|---|
| `params.manifest` | TSV: `trait, ancestry, sampleSize, summaryStatisticsPath` |
| `params.ld_reference` | TSV: `ancestry, ldMatrix, ldIndex` |
| `params.output_dir` | Results destination |
| `params.chain` | Liftover chain file (GRCh37→GRCh38) |
| `params.liftover` | Liftover reference (gnomAD) |
| `params.r2` | LD correlation threshold (default 0.5) |

## Nextflow requirement

`>=24.10.5` (DSL2, set in `nextflow.config`).
