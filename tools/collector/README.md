# collector

This is a tool to collect parquet files for summary statistics. It is used to collect parquet files from the input manifest and store them in a structured way for downstream analysis. The tool is implemented in Python and uses DuckDB for data processing. The tool is designed to be used in a Nextflow pipeline, but can also be used standalone.

## Development

The package uses `uv` for dependency management and supports the following
local checks:

| Command | Description |
| --- | --- |
| `make dev` | Install development dependencies. |
| `make lint` | Run Ruff formatting/linting and `ty` type checks. |
| `make test` | Run the Python test suite. |
| `make build` | Build the collector container image. |
| `make hailing-s3-smoke` | Opt in to real Pan-UKBB HT/BM, indel, and signed-LD validation. |
| `make clean` | Remove local development artifacts. |

See [CONTRIBUTING.md](CONTRIBUTING.md) for the contribution workflow.
The full pipeline documentation can be built from the repository root with
`make docs`.

## Compare LocusBreaker outputs

After running the pipeline with both collector and Gentropy LocusBreaker outputs enabled, compare logical rows with:

```bash
uv run python scripts/compare_locus_breaker_outputs.py \
  --collector-dir ../../testdata/output/locus_breaker_clumped_study_locus \
  --gentropy-dir ../../testdata/output/gentropy_locus_breaker_clumped_study_locus \
  --manifest ../../testdata/manifest.tsv \
  --json ../../testdata/output/locus_breaker_comparison.json
```

If the Gentropy process completed but the reference dataset was not published, compare against the Nextflow work directory instead:

```bash
uv run python scripts/compare_locus_breaker_outputs.py \
  --collector-dir ../../testdata/output/locus_breaker_clumped_study_locus \
  --gentropy-work-dir ../../testdata/work \
  --manifest ../../testdata/manifest.tsv \
  --json ../../testdata/output/locus_breaker_comparison.json
```

The comparison normalizes Gentropy hive partitions into a real `studyLocusId` column, sorts top-level rows, sorts nested `locus` arrays, and compares logical values instead of Parquet metadata. It is strict by default; use `--float-abs-tolerance 1e-6` to ignore tiny FLOAT representation differences while still checking row membership and non-float values exactly.

## Hailing Ducks LD annotation

The production collector image includes the native Hailing Ducks v1.1.0
DuckDB CLI. ``collector hailing_ld`` accepts one fine-mapping locus-set
Parquet dataset, study metadata JSONL, and aligned ancestry/HT/BM registry
options. It emits a flat Gentropy-compatible MultiAncestryPairwiseLD Parquet
file and per-ancestry JSONL statistics.

Use ``collector ld_parity`` to compare that output with Gentropy. The report
separates shared, backend-only, diagonal, and value-mismatch counts and reports
the maximum absolute LD difference by ancestry and overall.
