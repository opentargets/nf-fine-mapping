# Canonical collection speed benchmark

Benchmark date: 2026-08-24

## Fixture

- Three studies/ancestries.
- 1,000 canonical regions.
- 99,999 summary-statistics rows per study.
- Complete EAF and no duplicate `(studyId, variantId)` rows.
- Same CLI arguments and output contract for both runs.

## Results

| Implementation | Wall time | Published rows | Stats rows |
|---|---:|---:|---:|
| `main` baseline | 25.76 s | 2,997 | 1,000 |
| optimized branch | 2.59 s (2.77 s after sweep-only refinement) | 2,997 | 1,000 |

The optimized path was approximately **9.3–9.9× faster** on this fixture, depending on the run.

Logical Parquet comparison reported **0 differing rows** for the locus-set outputs. The optimized `stats.json` phase timings were:

- input validation: 0.054 s
- region discovery: 0.021 s
- locus materialization: 2.329 s
- statistics: 0.034 s

This is a synthetic benchmark, not a production-data guarantee. The final PR should include the fixture construction and machine/runtime details when the benchmark is rerun in CI or on representative data.

## Testdata correctness and performance check

The optimized command was compared with the legacy per-locus reader using the
first three summary-statistics Parquet files under ``testdata/sumstats``. The
benchmark creates 60 locus-breaker windows per study, runs the complete
``collect_canonical_regions`` command through both readers, and compares every
published Parquet row plus the stats JSON (excluding phase timings).

Run it from ``tools/collector`` with:

```bash
.venv/bin/python scripts/benchmark_canonical_regions.py --loci-per-study 60
```

Observed on 2026-08-26:

| Reader | Wall time |
|---|---:|
| Legacy per-locus reader | 8.31 s |
| Set-based reader | 0.808 s |

The run produced 79 locus-set files. Logical outputs were equal and stats JSON
was equal apart from ``timingsSeconds`` (10.29× speedup).
