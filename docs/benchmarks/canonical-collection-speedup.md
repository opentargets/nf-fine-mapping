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
