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

The optimized command was compared with the legacy per-locus reader using all
four three-study runs in ``testdata/manifest.full.tsv`` and their summary-
statistics Parquet files under ``testdata/sumstats``. The benchmark creates 60
locus-breaker windows per study, runs the complete
``collect_canonical_regions`` command through both readers for every run, and
compares every published Parquet row plus the stats JSON (excluding phase
timings).

Run it from ``tools/collector`` with:

```bash
.venv/bin/python scripts/benchmark_canonical_regions.py --loci-per-study 60
```

Observed on 2026-08-26:

| Run | Legacy | Set-based | Speedup | Output files | Equal |
|---|---:|---:|---:|---:|:---:|
| ``GCST90002351,GCST90018748,GCST90475531`` | 8.277 s | 0.817 s | 10.14× | 92 | yes |
| ``GCST90002357,GCST90278666,GCST90476301`` | 7.523 s | 0.866 s | 8.69× | 95 | yes |
| ``GCST90278661,GCST90475090,GCST90692780`` | 7.976 s | 0.871 s | 9.15× | 136 | yes |
| ``GCST90278665,GCST90475419,GCST90501104`` | 7.425 s | 0.882 s | 8.42× | 144 | yes |
| **Total** | **31.201 s** | **3.436 s** | **9.08×** | **467** | **yes** |

Every case produced logically identical Parquet outputs and stats JSON equal
apart from ``timingsSeconds``.
