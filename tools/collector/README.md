# Collector

This is a tool to collect parquet files for summary statistics. It is used to collect parquet files from the input manifest and store them in a structured way for downstream analysis. The tool is implemented in Python and uses DuckDB for data processing. The tool is designed to be used in a Nextflow pipeline, but can also be used standalone.

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
