# PRD — `MetaCollapse`: comparator arms for the multi-ancestry resolution benchmark

Status: **proposed**, not implemented.
Owner: Szymon Szyszkowski.
Related: [`docs/benchmarks/multi-ancestry-resolution.md`](../benchmarks/multi-ancestry-resolution.md) (statistical analysis plan; this PRD implements its section 17).

---

## 1. Problem

The pipeline produces multi-ancestry credible sets, but there is no comparator,
so no claim about improved resolution can currently be supported. The 26.09
`manifest.test3.tsv` run gives 237 locus sets and 423 reportable credible sets
in the joint arm alone — a description, not a benchmark.

The comparator required by the analysis plan is a **single-effect-set arm on the
same data**: inverse-variance-weighted meta-analysis across the ancestry arms,
fine-mapped over identical regions with identical variants and identical LD
provenance. That contrast holds total sample size fixed and varies only LD
structure, which is the mechanism the project claims.

Two constraints make this a pipeline change rather than an analysis script:

1. **The per-ancestry LD is not recoverable from published outputs.**
   `HAILING_DUCKS_LD_ANNOTATION` publishes only `stats.jsonl`
   (`modules/local/collector/hailing_ld/main.nf`, `publishDir` pattern
   `hailing_ducks_ld_annotation/**/stats.jsonl`). The
   `multi_ancestry_pairwise_ld.parquet` payload stays in the work directory. For
   the 26.09 run that payload is 1,333,618,165 pairs — 5.33 GB as float32,
   roughly 17× the entire 309 MB published tree. The comparator cannot be built
   post hoc at any level of effort.
2. **The comparator must reuse the joint arm's LD**, not fetch its own, or the
   arms differ in LD provenance and the contrast is invalid.

Both point to the same place: between LD annotation and the fine-mapping
method, where per-ancestry LD is in scope exactly once.

## 2. Why now — the existing run is the leverage

The work directory for the 26.09 run still holds every LD payload. Nextflow
`-resume` will cache-hit `COLLECTOR_LOCUS_BREAKER`,
`COLLECT_CANONICAL_REGIONS` and `HAILING_DUCKS_LD_ANNOTATION`, because adding a
process *downstream* does not alter their task hashes. Measured from
`output/pipeline_info/execution_trace/execution_trace_2026-09-03_19-08-34.txt`:

| process | tasks | median s | p90 s | max s | total h | median peak RSS |
|---|---:|---:|---:|---:|---:|---:|
| `HAILING_DUCKS_LD_ANNOTATION` | 251 | 50.7 | 161.0 | 910.0 | **6.01** | 1.30 GB |
| `MULTISUSIE_FINE_MAPPING` | 242 | 32.3 | 81.0 | 971.0 | **3.84** | 0.83 GB |
| `COLLECTOR_LOCUS_BREAKER` | 47 | 51.0 | 96.0 | 98.0 | 0.79 | 3.90 GB |
| `COLLECT_CANONICAL_REGIONS` | 19 | 46.9 | 124.0 | 125.0 | 0.37 | 2.10 GB |
| `COLLECTOR_CHECK_LD_PAIR_STATS` | 237 | 0.7 | 0.8 | 0.9 | 0.05 | — |
| **total** | **797** | | | | **11.06** | |

Cost of the three arms, two ways:

- **Three independent runs:** 3 × 11.06 = **33.2 h** of task time.
- **`-resume` plus this module:** +2 × 3.84 h of MultiSuSiE (an upper bound —
  both new arms present a single population, so each is cheaper than the joint
  arm) + `MetaCollapse` ≈ **+7.7 h**, giving 18.8 h total.

The saving is the 12.0 h of LD re-retrieval and 2.3 h of locus preparation that
never happen. That is the entire justification for the placement in §4.

> **Hard operational requirement.** The change must not touch
> `modules/local/collector/hailing_ld/main.nf`, `params.hailing_ducks_container`,
> `params.hailing_ducks_max_cached_blocks`, `params.ld_registry`, the locus-breaker
> or canonical-region modules and params, or `tools/collector` version pinning
> **in the same commit as the run**. Any of those invalidates the upstream cache
> and re-incurs the 6.01 h LD retrieval. Ship `MetaCollapse` as strictly
> additive, run with `-resume` against the original `-work-dir`, and only then
> refactor upstream if needed.

## 3. Goals and non-goals

**Goals**

- G1. Produce a `meta` arm: one IVW-collapsed effect-set plus its correct
  collapsed LD, per canonical locus set.
- G2. Produce a `single` arm: the largest-effective-sample-size ancestry alone.
- G3. Run all arms through the **unchanged** `MULTISUSIE_FINE_MAPPING` process,
  so `L`, purity threshold, convergence criteria and output contract are
  identical by construction rather than by discipline.
- G4. Emit results partitioned by arm, joinable on `(runId, fineMappingLocusSetId)`.
- G5. Default behaviour of the pipeline is unchanged: production runs produce
  exactly today's outputs at today's paths.

**Non-goals**

- N1. The genome-wide meta-analysis pass for the *discovery* endpoint (plan
  §17.4). Separate change.
- N2. The simulation arm (plan §9).
- N3. Comparator arms for SuSiEx or SuShiE.
- N4. The statistical analysis itself. This PRD delivers the paired table only.
- N5. Any change to LD retrieval, locus definition, or QC.

## 4. Placement

```
COLLECTOR_LOCUS_BREAKER → COLLECT_CANONICAL_REGIONS → HAILING_DUCKS_LD_ANNOTATION
                                    ↓  (cache-hit on -resume)
              ch_locus_annotation : Channel<Map>
                 runId, fineMappingLocusSetId, metas,
                 fine_mapping_locus_set_path, multi_ancestry_pairwise_ld_path
                                    ↓
                            fan_out_arms()
        ┌───────────────────────────┼───────────────────────────┐
   arm = joint                 arm = meta                  arm = single
   pass through          COLLECTOR_META_COLLAPSE      COLLECTOR_META_COLLAPSE
                            (--mode meta)               (--mode single)
        └───────────────────────────┼───────────────────────────┘
                                    ↓
                     MULTISUSIE_FINE_MAPPING  (unchanged)
```

Every arm enters the method as the same record shape, so the method process
requires no modification whatsoever. This is the property that makes plan §14
items 4–7 (identical `L`, coverage, purity, convergence) true by construction.

## 5. Input contracts (existing, do not change)

**`fine_mapping_locus_set_path`** — `COLLECTED_LOCUS_SCHEMA`
(`tools/collector/src/collector/schema.py`): one row per
`(fineMappingLocusSetId, studyLocusId, studyId)` — i.e. one row per ancestry arm
— with `chromosome`, `locusStart`, `locusEnd`, `qualityControls`, and
`locus : LIST<STRUCT<variantId VARCHAR, pValueMantissa FLOAT, pValueExponent
INTEGER, beta DOUBLE, standardError DOUBLE>>`.

**`multi_ancestry_pairwise_ld_path`** — written by `_adapt_reference_output` in
`hailing_ld.py`, long format:

| column | type | note |
|---|---|---|
| `ancestry` | VARCHAR | |
| `variantIdI` | VARCHAR | |
| `variantIdJ` | VARCHAR | |
| `r` | DOUBLE | correlation, not r² |

Each unordered pair appears once, and the diagonal is appended with `r = 1.0`
via `UNION ALL`. Confirmed arithmetically: a 911-variant arm reports 415,416
pairs = C(911,2) + 911.

**`metas`** — list of `{studyId, ancestry, sampleSize}`, passed to the method as
`metadata.jsonl`.

## 6. The transformation — exact specification

### 6.1 Weights

For variant *i*, over the set `O(i)` of ancestry arms in which *i* is observed:

```
u[a,i] = (1 / se[a,i]) / sqrt( Σ_{b ∈ O(i)} 1 / se[b,i]² )     for a ∈ O(i)
u[a,i] = 0                                                      otherwise
```

By construction `Σ_a u[a,i]² = 1` for every variant. `se[a,i]` is
`locus.standardError` for that ancestry's row.

### 6.2 Collapsed statistics

```
beta_meta[i] = Σ_a (beta[a,i] / se[a,i]²) / Σ_a (1 / se[a,i]²)
se_meta[i]   = 1 / sqrt( Σ_a 1 / se[a,i]² )
z_meta[i]    = beta_meta[i] / se_meta[i] = Σ_a u[a,i] · z[a,i]
```

The collapsed row carries a synthetic `studyId = "${runId}__meta"` and
`sampleSize = Σ_a sampleSize[a]`. Ancestry label `meta`.

### 6.3 Collapsed LD — a group-by, not linear algebra

```
R_meta[i,j] = Σ_a u[a,i] · u[a,j] · R[a,i,j]
```

Because the LD is stored long, this is a single aggregation and **no dense p×p
matrix is ever materialised**:

```sql
WITH w AS (  -- one row per (ancestry, variantId), u as in 6.1
    SELECT ancestry, variantId,
           (1.0 / standardError)
             / sqrt(SUM(1.0 / (standardError * standardError))
                    OVER (PARTITION BY variantId)) AS u
    FROM locus_variants
),
canon AS (   -- canonicalise pair order before grouping (see R3)
    SELECT ancestry,
           LEAST(variantIdI, variantIdJ)    AS vi,
           GREATEST(variantIdI, variantIdJ) AS vj,
           r
    FROM read_parquet($ld)
)
SELECT 'meta' AS ancestry, c.vi AS variantIdI, c.vj AS variantIdJ,
       SUM(wi.u * wj.u * c.r) AS r
FROM canon c
JOIN w wi ON wi.ancestry = c.ancestry AND wi.variantId = c.vi
JOIN w wj ON wj.ancestry = c.ancestry AND wj.variantId = c.vj
GROUP BY 1, 2, 3
```

Median input size is 2.45M rows per locus set (p90 13.4M, max 96.3M), so this
is seconds of DuckDB work, and memory scales with the group-by output, not with
p².

### 6.4 Three properties that follow, and are therefore tests

1. **Unit diagonal.** `R_meta[i,i] = Σ_a u[a,i]² · 1 = 1` exactly. Assert
   `|r − 1| < 1e-12` on every diagonal row. This is a free end-to-end check of
   the weight normalisation *and* the join.
2. **Positive semi-definiteness is preserved.** `R_meta = Σ_a D_a R_a D_a` is a
   sum of PSD matrices when each `R_a` is PSD. Reference-panel matrices often
   are not, so `R_meta` inherits that; do not "fix" it here — record it (R5).
3. **Partial overlap is correct, not attenuated.** If variant *j* is absent from
   ancestry *a*, then `u[a,j] = 0` and that term drops out. The resulting lower
   `|r_meta[i,j]|` is the *true* covariance of two statistics computed on
   partially different samples, not an artefact. No renormalisation over pairs.

### 6.5 Verification already performed

`docs/benchmarks/verify_meta_ld.py` checks §6.3 by Monte Carlo — 3 ancestries at
N = 400,000 / 80,000 / 4,000, distinct AR(1)-plus-noise LD per ancestry,
distinct per-ancestry MAF, 400,000 draws:

| assumed LD | mean abs. error, off-diagonal | max abs. error |
|---|---|---|
| `Σ_a D_a R_a D_a` (this spec) | 0.00124 = **0.8 × Monte Carlo SE** | 0.0062 |
| `Σ_a (N_a/N) R_a` (√N form) | 0.01351 = **8.5 × Monte Carlo SE** | 0.1012 |

The per-variant form is correct to within simulation noise. The
sample-size-weighted form — the one usually written down — is wrong by up to
0.10 in correlation, because `se[a,i]` depends on MAF and MAF differs across
ancestries. **That is the mechanism this pipeline exploits, so the
approximation fails precisely where it matters. Implement §6.1, not the √N
shortcut.**

### 6.6 `--mode single`

Select the ancestry arm with the largest `sampleSize` from `metas`; emit its
locus row unchanged and filter the LD parquet to `ancestry = <that arm>`. No
arithmetic. Present for symmetry so all three arms traverse identical code
downstream.

## 7. Functional requirements

| id | requirement |
|---|---|
| **R1** | New collector subcommand `collector meta_collapse` with `--input`, `--multi_ancestry_pairwise_ld`, `--study_metadata`, `--output`, `--ld_output`, `--metadata_output`, `--stats_output`, `--mode {meta,single}`. |
| **R2** | Output locus set validates against `COLLECTED_LOCUS_SCHEMA` with exactly one row per `fineMappingLocusSetId`. |
| **R3** | Pair order is canonicalised with `LEAST`/`GREATEST` before grouping. If two ancestries store the same unordered pair in opposite orders, a naive `GROUP BY` splits it and halves the LD — silent and severe. |
| **R4** | Emit `metadata_output` JSONL for the collapsed arm, consumable unchanged by `MULTISUSIE_FINE_MAPPING`'s `--study-metadata`. |
| **R5** | `stats_output` JSONL records, per locus set: `mode`, `nAncestryArms`, `nVariantsUnion`, `nVariantsInAllArms`, `nVariantsSingleArmOnly`, `nPairsIn`, `nPairsOut`, `maxAbsDiagonalDeviation`, `nPairsMissingWithBothVariantsPresent`, `sampleSizeTotal`, and `pairOrderCanonicalisationApplied`. Provenance sufficient to audit without re-running. |
| **R6** | Fail loudly, with the count, if any diagonal deviates from 1.0 by more than 1e-9, or if `nPairsMissingWithBothVariantsPresent` exceeds `--max_missing_pair_fraction` (default 0.001). Silently zeroing absent LD understates correlation and makes SuSiE over-split. |
| **R7** | New process `COLLECTOR_META_COLLAPSE` in `modules/local/collector/meta_collapse/main.nf`, `label "collector"`, typed input/output records matching the surrounding style, `topic` version emission, and a `stub` block. |
| **R8** | `fan_out_arms()` channel helper in `main.nf`, adding `arm : String` to the record. |
| **R9** | `params.benchmark_arms = ['joint']` in `nextflow.config`, declared in `nextflow_schema.json` (`validateParameters()` rejects undeclared params). Validate against `['joint','meta','single']`, reject duplicates and empties, mirroring the existing `fine_mapping_methods` validation in `workflows/fine_mapping/main.nf`. |
| **R10** | Publishing: `joint` keeps today's path `multisusie/${runId}/${fineMappingLocusSetId}/` so existing consumers keep working; other arms publish to `multisusie_${arm}/${runId}/${fineMappingLocusSetId}/`. Also publish `meta_collapse/${runId}/${fineMappingLocusSetId}/stats.jsonl`. |
| **R11** | `MULTISUSIE_FINE_MAPPING` is not modified. If it must be, the PRD has failed its purpose. |
| **R12** | Verify that MultiSuSiE with one population reduces to standard SuSiE — its cross-population effect-size prior must degenerate correctly at K = 1. If it does not, record the residual as a limitation (plan §14 item 10) rather than papering over it. **Blocking on the first arm run, not on merge.** |

## 8. Non-functional requirements

- **N-R1.** Peak RSS per `COLLECTOR_META_COLLAPSE` task ≤ 4 GB at the 96.3M-pair
  worst-case locus. Set a DuckDB `memory_limit` and rely on its spill-to-disk.
- **N-R2.** Median task time ≤ 60 s, so the module is a rounding error against
  the 3.84 h MultiSuSiE cost.
- **N-R3.** Deterministic output — identical bytes for identical input. Sort
  before `COPY`.
- **N-R4.** No network access. Pure local parquet work.

## 9. Testing

**Collector unit tests** (`tools/collector/tests/`, `uv run pytest`):

| test | assertion |
|---|---|
| unit diagonal | every diagonal `r` equals 1.0 within 1e-12, on a fixture with three arms and distinct per-variant `se` |
| K = 1 idempotence | `--mode meta` on a single-arm input returns the input locus and LD unchanged |
| two identical arms | with equal `se` in both arms, `u = 1/√2` each, so `z_meta = √2 · z` and `R_meta = R`. Closed form, no tolerance fudging |
| pair-order robustness | reversing `(variantIdI, variantIdJ)` in one arm's fixture changes nothing (R3) |
| partial overlap | a variant present in one arm only gets `u = 1` there, its diagonal stays 1.0, and its off-diagonals to other-arm-only variants are 0 |
| missing pair guard | exceeding `--max_missing_pair_fraction` exits non-zero with the count in `stats_output` (R6) |
| Monte Carlo (slow-marked) | port `docs/benchmarks/verify_meta_ld.py`; empirical covariance within 2 × MC SE of `Σ_a D_a R_a D_a` |
| schema conformance | output validates against `COLLECTED_LOCUS_SCHEMA` |

**Pipeline tests** (`nf-test`):

- `--stub` test for `params.benchmark_arms = ['joint','meta','single']`, asserting three `MULTISUSIE_FINE_MAPPING` invocations per locus set and correct publish paths.
- Regression: default params produce byte-identical publish paths to today (G5).
- `testdata/manifest.test.minimal.tsv` end-to-end for the `meta` arm.

## 10. Acceptance criteria

1. `nextflow run main.nf -profile <profile> -resume -work-dir <original> --benchmark_arms joint,meta,single` completes on the 26.09 run with **zero** `HAILING_DUCKS_LD_ANNOTATION`, `COLLECTOR_LOCUS_BREAKER` or `COLLECT_CANONICAL_REGIONS` tasks executed — all cached. Verified from the new execution trace.
2. Three arms present for each of the 237 locus sets that reached LD annotation, or a recorded reason per absence.
3. `maxAbsDiagonalDeviation` < 1e-9 across every locus set.
4. Default-params run reproduces today's output paths exactly.
5. A joined table keyed on `(runId, fineMappingLocusSetId, arm)` carrying credible-set membership, PIPs, convergence status and purity counts — the direct input to plan §4–§6.
6. Incremental task time ≤ 9 h (against the 7.7 h estimate in §2).

## 11. Risks

| risk | severity | mitigation |
|---|---|---|
| Upstream cache invalidated → 6.01 h LD re-retrieval | high | strictly additive change; no upstream edits in the same commit; verify with `-resume` dry inspection before the full run |
| Pair-order inconsistency across ancestries silently halves LD | high | R3 canonicalisation + the dedicated fixture test |
| √N weights used instead of per-variant IVW | high | §6.5 quantifies the error; the closed-form and Monte Carlo tests both fail if taken |
| `R_meta` not PSD → MultiSuSiE numerics failure | medium | already observed once in the joint arm (`stats.json` reason: *"a population's residual-variance estimate going negative (LD/summary-statistics mismatch)"*). Record clipped spectral mass in `stats_output`; expect and report failures rather than suppressing them |
| MultiSuSiE at K = 1 ≠ SuSiE | medium | R12; report as a limitation if confirmed |
| Purity filter differs across arms | high | prevented by construction — one process, `params.multisusie_purity_min_r2`. Note the filter removes 82.1% of modelled components (1,937 of 2,360) in the joint arm, so any drift here would dominate the result |
| Work directory deleted before the run | high | confirm the original `-work-dir` exists **before** starting; otherwise the LD is gone and the plan reverts to a full re-run |

## 12. Phasing

- **P1** — `collector meta_collapse` with `--mode meta`, unit tests, schema conformance. No pipeline changes. Independently reviewable.
- **P2** — `--mode single`, `COLLECTOR_META_COLLAPSE`, `fan_out_arms()`, params + schema, stub tests, publish paths.
- **P3** — the `-resume` run on 26.09, R12 verification, the joined paired table.
- **P4** — the analysis in `docs/benchmarks/multi-ancestry-resolution.md` §4–§6, and the benchmark-fairness caption for the poster.

## 13. Open questions

1. Does `MULTISUSIE_FINE_MAPPING` accept a single population without special-casing, and does its prior degenerate correctly? (R12)
2. Is the original 26.09 `-work-dir` still intact?
3. `qualityControls` on the collapsed row: union across arms, or a new
   `META_COLLAPSED` marker? Affects whether downstream QC filters behave
   identically across arms.
4. `pValueMantissa` / `pValueExponent` on the collapsed locus struct — recompute
   from `z_meta`, or leave null? The method consumes `beta`/`standardError`, but
   the schema declares them non-null-ish and downstream Gentropy consumers may
   read them.
5. Run `26.09` labelling: the results directory is `26.09-nf-fine-mapper-results`
   while the poster figures are captioned release 26.06. Which is correct?
