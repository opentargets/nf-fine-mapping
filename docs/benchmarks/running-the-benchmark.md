# Running the multi-ancestry resolution benchmark

Runbook for producing the `meta` and `single` comparator arms against the
existing 26.09 `manifest.test3.tsv` run, reusing its LD rather than re-fetching
it.

Companion documents: [`multi-ancestry-resolution.md`](multi-ancestry-resolution.md)
(what is being measured and why) and [`../prd/meta-collapse.md`](../prd/meta-collapse.md)
(what was built).

---

## What the previous run was

From `.nextflow/history` in the repository root:

```
2026-09-03 19:08:34  1h 8m 20s  magical_gilbert  OK
  session 4b8fdfc9-b04a-457b-be99-f4b4555a8992
  nextflow run main.nf -profile googleCloudTest -resume
```

- Executor: **Google Cloud Batch**, project `open-targets-eu-dev`, `europe-west1`
- Work directory: `gs://gwas_catalog_multi_ancestry_fine_mapping/work_gcloud_test`
- Output: `gs://gwas_catalog_multi_ancestry_fine_mapping/output_gcloud_test`
- Manifest: `gs://gwas_catalog_multi_ancestry_fine_mapping/manifest.test3.tsv`

The per-ancestry LD lives in that GCS work directory and nowhere else — it is
never published (see the PRD). Reusing it is the whole point of the exercise:
`HAILING_DUCKS_LD_ANNOTATION` was 6.01 h of the 11.06 h run.

## Two things that will silently cost you that 6 h

### 1. The resume cache lives in the launch directory, not in the branch

Nextflow keeps its resume cache in `$launchDir/.nextflow/cache/<session>`. The
26.09 session is in the **repository root**:

```
/Users/ss60/Projects/nf-fine-mapping/.nextflow/cache/4b8fdfc9-b04a-457b-be99-f4b4555a8992
```

A worktree is a different directory and has no `.nextflow` of its own. Running
`-resume` from `.claude/worktrees/meta-collapse` therefore finds **no cache**,
treats every task as new, and re-runs the full LD annotation on Google Batch.

Two ways to avoid it, in order of preference:

**(a) Copy the cache into the worktree.** Leaves the original untouched.

```bash
REPO=/Users/ss60/Projects/nf-fine-mapping
WT=$REPO/.claude/worktrees/meta-collapse
SESSION=4b8fdfc9-b04a-457b-be99-f4b4555a8992

mkdir -p "$WT/.nextflow/cache"
cp -R "$REPO/.nextflow/cache/$SESSION" "$WT/.nextflow/cache/"
cp    "$REPO/.nextflow/history"        "$WT/.nextflow/"
```

**(b) Launch from the repository root with the branch checked out.** Simplest,
but gives up the worktree separation, and the cache then mixes runs from both
branches.

Either way, resume the session **explicitly** rather than relying on "the last
run", because the history has 88 sessions in it:

```bash
-resume 4b8fdfc9-b04a-457b-be99-f4b4555a8992
```

The `projectDir` does not enter the task hashes for this profile — every path in
`conf/google-cloud-test.config` is an absolute `gs://` URI — so a run launched
from the worktree resumes the same tasks.

### 2. Confirm the work directory still holds the LD

```bash
gcloud storage ls gs://gwas_catalog_multi_ancestry_fine_mapping/work_gcloud_test/ | head
gcloud storage du -s gs://gwas_catalog_multi_ancestry_fine_mapping/work_gcloud_test/
```

Expect something on the order of several GB; the LD alone is 1,333,618,165
pairs. If a lifecycle rule or a manual clean has removed it, the resume
strategy is void and the run costs the full 33 h instead of 7.7 h.

---

## Step 1 — unit tests (local, free, and never yet executed)

```bash
cd /Users/ss60/Projects/nf-fine-mapping/.claude/worktrees/meta-collapse
uv run --project tools/collector pytest tests/collector/test_meta_collapse.py -m "not slow" -x -q
uv run --project tools/collector pytest tests/collector/test_meta_collapse.py -m slow -q
make collector-check
```

The SQL in `meta_collapse.py` has never run. The algebra it encodes was
verified independently, but the statements themselves have not been executed
against DuckDB even once. Expect to fix something here; do not proceed until
this is green.

## Step 2 — pipeline wiring, stub only (local, free)

```bash
nextflow run main.nf -profile testFullCollectorHailingDucks -stub-run \
    --benchmark_arms joint,meta,single
```

Confirms the channel wiring, the aliased process invocations and the publish
paths without running anything real. Check the summary reports
`COLLECTOR_META_COLLAPSE_META`, `COLLECTOR_META_COLLAPSE_SINGLE`,
`MULTISUSIE_FINE_MAPPING_META` and `MULTISUSIE_FINE_MAPPING_SINGLE`.

Then confirm the default is untouched:

```bash
nextflow run main.nf -profile testFullCollectorHailingDucks -stub-run
```

No `COLLECTOR_META_COLLAPSE*` or `*_META` / `*_SINGLE` task should appear.

## Step 3 — prove the cache is intact BEFORE spending anything

This is the step that protects the 6 h. Resume with the joint arm only, which
adds no new work at all:

```bash
nextflow run main.nf -profile googleCloudTest \
    -resume 4b8fdfc9-b04a-457b-be99-f4b4555a8992 \
    --benchmark_arms joint
```

**Expected: every task cached, zero tasks submitted.** The summary should read
along the lines of `Succeeded: 0, Cached: 797`.

> If it starts executing `HAILING_DUCKS_LD_ANNOTATION`, **cancel immediately**
> (Ctrl-C). The cache is not being matched, and continuing re-fetches
> 1.33 billion LD pairs on Google Batch. Diagnose with `-dump-hashes` before
> trying again: something upstream changed, or the cache was not found in the
> launch directory.

## Step 4 — the real run

```bash
nextflow run main.nf -profile googleCloudTest \
    -resume 4b8fdfc9-b04a-457b-be99-f4b4555a8992 \
    --benchmark_arms joint,meta,single \
    -with-report  logs/benchmark-report.html \
    -with-trace   logs/benchmark-trace.txt \
    -with-timeline logs/benchmark-timeline.html
```

Expected work: 237 × `COLLECTOR_META_COLLAPSE` per collapsed arm, plus 237 ×
`MULTISUSIE_FINE_MAPPING` per arm. Everything upstream cached.

Estimated additional task time **≤ 7.7 h**, against 33.2 h for three
independent runs. The estimate is an upper bound: both new arms present a
single population to MultiSuSiE, so each should be cheaper than the joint arm
that took 3.84 h.

## Step 5 — check the guard rails fired correctly

```bash
gcloud storage cat \
  "gs://gwas_catalog_multi_ancestry_fine_mapping/output_gcloud_test/meta_collapse/**/stats.json" \
  | jq -s '{
      n: length,
      max_diag_deviation: (map(.maxAbsDiagonalDeviation) | max),
      max_missing_pair_fraction: (map(.missingPairFraction) | max),
      arms: (map(.nAncestryArms) | add / length),
      variants_union_median: (map(.nVariantsUnion) | sort | .[length/2 | floor])
    }'
```

`max_diag_deviation` must be below `1e-9`. It is 1 by construction, so any
deviation means the weights or one of the two joins is wrong, and every
downstream number is void.

## Step 6 — the outputs to analyse

| arm | path |
|---|---|
| joint | `output_gcloud_test/multisusie/${runId}/${locusSetId}/` |
| meta | `output_gcloud_test/multisusie_meta/${runId}/${locusSetId}/` |
| single | `output_gcloud_test/multisusie_single/${runId}/${locusSetId}/` |
| collapse stats | `output_gcloud_test/meta_collapse/${runId}/${locusSetId}/${arm}/stats.json` |

Join on `(runId, fineMappingLocusSetId, arm)` and hand the result to sections
4–6 of the analysis plan. The joint arm's paths are unchanged from 26.09, so
existing consumers keep working.

---

## Known unknowns before the numbers mean anything

1. **Does MultiSuSiE at K = 1 reduce to SuSiE?** (PRD R12.) If its
   cross-population effect-size prior does not degenerate correctly with a
   single population, the `meta` and `single` arms are not clean single-effect
   analyses and the contrast carries a residual that must be reported rather
   than hidden. Answerable by reading the MultiSuSiE source; do it before
   interpreting anything.
2. **Are Locus Breaker regions arm-blind?** (Plan §6.) If regions were defined
   from one arm's statistics, the discovery comparison is circular. Resolution
   is unaffected — the collapse reuses the joint arm's regions verbatim — but
   the discovery endpoint would be void.
3. **One trait produced no output at all.** 237 locus sets came from **18 of
   19** manifest runs; `GCST90013925,GCST90692138` is absent. Explain it before
   quoting any denominator.
4. **`pValueMantissa` / `pValueExponent` are written as NULL** on the collapsed
   locus struct. MultiSuSiE consumes `beta`/`standardError`, but downstream
   Gentropy consumers may not tolerate nulls.
