# nf-schema Parameter and Manifest Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire up the `nf-schema` Nextflow plugin so pipeline parameters and the manifest TSV are validated fail-fast, replacing silent acceptance of bad input with clear, immediate errors.

**Architecture:** Two independent, additive checks, both fail-fast (halt before any process runs): (1) `validateParameters()` + `paramsSummaryLog(workflow)` against the existing `nextflow_schema.json`; (2) a new samplesheet schema (`assets/schema_manifest.json`) validated via `samplesheetToList()`, replacing the hand-rolled manifest TSV parser in `main.nf`'s `read_manifest()`.

**Tech Stack:** Nextflow DSL2 (strict/typed syntax, `nextflow.enable.types = true`), the `nf-schema` plugin (pinned `2.8.0`), nf-test for testing.

**Spec:** `/home/misio-pysio/Knowledgebase/Wiki/Projects/multi-ancestry-fine-mapping/Plan/2026-08-24-nf-schema-manifest-validation-prd.md` (mirrored as GitHub issues [opentargets/nf-fine-mapping#29](https://github.com/opentargets/nf-fine-mapping/issues/29) and [#30](https://github.com/opentargets/nf-fine-mapping/issues/30))

## Global Constraints

- Pin the plugin to exactly `nf-schema@2.8.0` — this is the latest published release (verified via `gh api repos/nextflow-io/nf-schema/releases`), not the `2.7.2` nf-schema's own docs site shows, nor the unreleased `3.0.0` sitting on `master`.
- `nf-schema` 2.8.0 requires Nextflow ≥25.10.0 and Java 17+. Local Nextflow is 26.04.6, so there is no local/CI impact — this is a floor correction to `CLAUDE.md`'s documented minimum (`>=24.10.5`), not a real upgrade requirement.
- `nextflow_schema.json`'s `"$schema"` **must** be `"https://json-schema.org/draft/2020-12/schema"`. 2.8.0 refuses to load a draft-07 meta schema at all — confirmed empirically (`Failed to load the meta schema`). The repo currently has `"http://json-schema.org/draft-07/schema#"`.
- `paramsSummaryLog` requires the `workflow` argument in 2.8.0 (`paramsSummaryLog(workflow)`) — confirmed empirically against the installed plugin. Do not use the no-arg form shown in nf-schema's current doc examples; those examples target the unreleased `3.0.0`.
- There is no `validation.failUnrecognisedParams` config key in 2.8.0 (confirmed empirically: `No such variable: failUnrecognisedParams`). Do not add it. Unrecognized params already fail once `validateParameters()` runs, because `nextflow_schema.json`'s existing top-level `"additionalProperties": false` triggers a hard validation failure on its own — confirmed empirically.
- `format: file-path` / `format: directory-path` in nf-schema do **not** check filesystem existence unless `exists: true` is also set — confirmed empirically. Do not add `exists: true` to `summarystatsLocation` in the new manifest schema: paths there are relative to `params.manifest_base_dir`, not the schema validator's resolution root, so an existence check would false-fail legitimate manifests.
- All nf-test suites run via `nf-test test` (root `nf-test.config` sets `testsDir "."`, default `profile "testFullCollectorHailingDucks"`, and loads `tests/nextflow.config` which sets `docker.enabled = false`). Full-pipeline tests use `nextflow_pipeline { script "../main.nf" options "-stub-run" }`, matching `tests/default.nf.test`.
- Do not touch the existing `MANIFEST_VALIDATION` workflow (`main.nf`, the `UNREGISTERED_ANCESTRY` soft per-run cascade) — it is a distinct, unrelated failure mode from this feature and must keep passing its existing tests in `tests/workflows/manifest_validation.nf.test` unchanged.

---

## File Structure

- `nextflow.config` — add `nf-schema` to the existing `plugins {}` block.
- `nextflow_schema.json` — bump `$schema` to the 2020-12 draft URI (no other content changes).
- `main.nf` — add the `plugin/nf-schema` include; call `validateParameters()`/`paramsSummaryLog(workflow)` in the entry `workflow {}`; replace `read_manifest()`'s body to use `samplesheetToList()`; rename `manifest_row_to_record()` to `manifest_entry_to_record()` with a new signature matching the samplesheet's `[meta, summarystatsLocation, traitFromSourceMappedIds]` tuple shape.
- `assets/schema_manifest.json` — new file: the manifest TSV's samplesheet schema.
- `CLAUDE.md` — bump the documented Nextflow minimum.
- `testdata/manifest.missing_column.tsv`, `testdata/manifest.invalid_sample_size.tsv` — new malformed fixtures.
- `tests/params_validation.nf.test` — new nf-test file for Task 1.
- `tests/manifest_schema_validation.nf.test` — new nf-test file for Task 2.
- `docs/user-guide/qc-validation.rst` — cross-reference addition, **only if the file exists** (it ships in the still-unmerged PR #25; see Task 2, Step 7).

---

### Task 1: Wire up nf-schema parameter validation

**Files:**
- Modify: `nextflow.config` (plugins block)
- Modify: `nextflow_schema.json` (`$schema` line)
- Modify: `main.nf` (include + entry workflow)
- Modify: `CLAUDE.md` (Nextflow minimum version line)
- Create: `tests/params_validation.nf.test`

**Interfaces:**
- Consumes: nothing from other tasks (first task).
- Produces: the `include { validateParameters; paramsSummaryLog; samplesheetToList } from 'plugin/nf-schema'` line in `main.nf` — Task 2 extends this same include with no other symbols to add (all three are already listed here), and calls `samplesheetToList` from within `read_manifest()`.

- [ ] **Step 1: Write the failing tests**

Create `tests/params_validation.nf.test`:

```groovy
nextflow_pipeline {

    name "Test nf-schema parameter validation"
    script "../main.nf"
    tag "pipeline"
    tag "stub"
    tag "validation"
    options "-stub-run"

    test("fails when a known parameter has the wrong type") {
        when {
            params {
                canonical_region_min_maf = "notanumber"
            }
        }

        then {
            def output = [workflow.stdout, workflow.stderr]
                .findAll { stream -> stream != null && stream.toString() }
                .collect { stream -> stream.toString() }
                .join("\n")

            assertAll(
                { assert workflow.failed },
                { assert workflow.exitStatus != 0 },
                { assert output.contains("canonical_region_min_maf") },
                { assert output.contains("should be") },
                { assert workflow.trace.tasks().size() == 0 }
            )
        }
    }

    test("fails when a known parameter violates its minimum") {
        when {
            params {
                hailing_ducks_max_cached_blocks = -5
            }
        }

        then {
            def output = [workflow.stdout, workflow.stderr]
                .findAll { stream -> stream != null && stream.toString() }
                .collect { stream -> stream.toString() }
                .join("\n")

            assertAll(
                { assert workflow.failed },
                { assert workflow.exitStatus != 0 },
                { assert output.contains("hailing_ducks_max_cached_blocks") },
                { assert workflow.trace.tasks().size() == 0 }
            )
        }
    }

    test("succeeds and logs a parameter summary when parameters are valid") {
        then {
            def output = [workflow.stdout, workflow.stderr]
                .findAll { stream -> stream != null && stream.toString() }
                .collect { stream -> stream.toString() }
                .join("\n")

            assertAll(
                { assert workflow.success },
                { assert workflow.trace.failed().size() == 0 },
                { assert output.contains("manifest") }
            )
        }
    }
}
```

Note: the third test (`"succeeds and logs..."`) is a regression guard, not a red-first test — the pipeline already succeeds with valid parameters today (verified: `nf-test test tests/default.nf.test` passes on the current `main` branch). It exists to confirm wiring up `validateParameters()`/`paramsSummaryLog()` doesn't break the happy path, and that a summary is actually logged. The first two tests are the real red-first cases.

- [ ] **Step 2: Run the tests to verify the first two fail and the third already passes**

Run: `nf-test test tests/params_validation.nf.test`

Expected: the first two tests **FAIL** — the pipeline currently completes successfully (`completed=57`) even with `canonical_region_min_maf=notanumber` or `hailing_ducks_max_cached_blocks=-5`, because neither param is in `main.nf`'s native typed `params {}` block, so nothing today validates their type or range. (Verified directly: `nextflow run main.nf -stub-run -profile testFullCollectorHailingDucks --canonical_region_min_maf notanumber` currently exits with `[SUCCESS] completed=57 failed=0`.) The third test **PASSES** already (expected — it's a regression guard, see the note in Step 1).

- [ ] **Step 3: Add the plugin declaration**

In `nextflow.config`, find:

```groovy
plugins {
    id 'nf-google'
}
```

Replace with:

```groovy
plugins {
    id 'nf-google'
    id 'nf-schema@2.8.0'
}
```

- [ ] **Step 4: Fix the schema draft version**

In `nextflow_schema.json`, find:

```json
  "$schema": "http://json-schema.org/draft-07/schema#",
```

Replace with:

```json
  "$schema": "https://json-schema.org/draft/2020-12/schema",
```

- [ ] **Step 5: Wire validateParameters() and paramsSummaryLog() into main.nf**

In `main.nf`, find the include block at the top:

```groovy
include { LOCUS_BREAKER    } from './workflows/locus_breaker/main.nf'
include { LOCUS_COLLECTION } from './workflows/locus_collection/main.nf'
include { LOCUS_ANNOTATION } from './workflows/locus_annotation/main.nf'
include { FINE_MAPPING     } from './workflows/fine_mapping/main.nf'
```

Replace with:

```groovy
include { LOCUS_BREAKER    } from './workflows/locus_breaker/main.nf'
include { LOCUS_COLLECTION } from './workflows/locus_collection/main.nf'
include { LOCUS_ANNOTATION } from './workflows/locus_annotation/main.nf'
include { FINE_MAPPING     } from './workflows/fine_mapping/main.nf'
include { validateParameters; paramsSummaryLog; samplesheetToList } from 'plugin/nf-schema'
```

Then find the typed `params {}` block:

```groovy
params {
    manifest: String
    manifest_base_dir: String
    output_dir: String
    route: String
    ld_registry: List = []
    ld_annotation_method: String = 'gentropy'
    hailing_ducks_container: String = 'ghcr.io/project-defiant/hailing-ducks:v1.1.0'
    hailing_ducks_max_cached_blocks: Integer = 8
    fine_mapping_methods: List = ['multisusie']
}
```

Add the `validate_params` flag (it already exists in `nextflow_schema.json`, hidden, default `true`, but has no entry in this typed block — add it so it can be overridden on the CLI):

```groovy
params {
    manifest: String
    manifest_base_dir: String
    output_dir: String
    route: String
    ld_registry: List = []
    ld_annotation_method: String = 'gentropy'
    hailing_ducks_container: String = 'ghcr.io/project-defiant/hailing-ducks:v1.1.0'
    hailing_ducks_max_cached_blocks: Integer = 8
    fine_mapping_methods: List = ['multisusie']
    validate_params: Boolean = true
}
```

Then find the entry workflow's start:

```groovy
workflow {

    main:
    intro()
    manifest_ch = read_manifest(params.manifest)
```

Replace with:

```groovy
workflow {

    main:
    intro()
    if (params.validate_params) {
        validateParameters()
    }
    log.info paramsSummaryLog(workflow)
    manifest_ch = read_manifest(params.manifest)
```

- [ ] **Step 6: Run the tests to verify all three pass**

Run: `nf-test test tests/params_validation.nf.test`

Expected: all three tests **PASS**.

- [ ] **Step 7: Run the full existing suite to check for regressions**

Run: `nf-test test`

Expected: all existing tests still pass (`tests/default.nf.test`, `tests/workflows/*.nf.test`). If any test fails because it passes a parameter not declared in `nextflow_schema.json`, that is a real gap the schema needs to cover — do not work around it by disabling validation.

- [ ] **Step 8: Bump the documented Nextflow minimum**

In `CLAUDE.md`, find:

```
Requires Nextflow `>=24.10.5`.
```

Replace with:

```
Requires Nextflow `>=25.10.0` (the `nf-schema` plugin's minimum).
```

- [ ] **Step 9: Commit**

```bash
git add nextflow.config nextflow_schema.json main.nf CLAUDE.md tests/params_validation.nf.test
git commit -m "feat: validate pipeline parameters with nf-schema"
```

---

### Task 2: Add manifest samplesheet schema validation

**Files:**
- Create: `assets/schema_manifest.json`
- Create: `testdata/manifest.missing_column.tsv`
- Create: `testdata/manifest.invalid_sample_size.tsv`
- Create: `tests/manifest_schema_validation.nf.test`
- Modify: `main.nf` (`read_manifest()`, `manifest_row_to_record()`)
- Modify: `docs/user-guide/qc-validation.rst` (only if it exists — see Step 7)

**Interfaces:**
- Consumes: the `include { validateParameters; paramsSummaryLog; samplesheetToList } from 'plugin/nf-schema'` line added in Task 1 (already imports `samplesheetToList` — no new include needed).
- Produces: nothing consumed by a later task (last task).

- [ ] **Step 1: Write the malformed manifest fixtures**

Create `testdata/manifest.missing_column.tsv` (drops the `traitFromSourceMappedIds` column):

```
runId	studyId	route	summarystatsLocation	majorAncestry	effectiveSampleSize
RUN_A	GCST90002351	multi_susie_route	testdata/sumstats/GCST90002351	nfe	519288
```

Create `testdata/manifest.invalid_sample_size.tsv` (non-numeric `effectiveSampleSize`):

```
runId	studyId	route	summarystatsLocation	majorAncestry	traitFromSourceMappedIds	effectiveSampleSize
RUN_A	GCST90002351	multi_susie_route	testdata/sumstats/GCST90002351	nfe	['EFO_0004833']	notanumber
```

(Both use tab separators, matching every other `testdata/manifest*.tsv` fixture.)

- [ ] **Step 2: Write the failing tests**

Create `tests/manifest_schema_validation.nf.test`:

```groovy
nextflow_pipeline {

    name "Test manifest samplesheet schema validation"
    script "../main.nf"
    tag "pipeline"
    tag "stub"
    tag "validation"
    options "-stub-run"

    test("fails fast when the manifest is missing a required column") {
        when {
            params {
                manifest = new File("testdata/manifest.missing_column.tsv").canonicalPath
                manifest_base_dir = new File(".").canonicalPath
            }
        }

        then {
            def output = [workflow.stdout, workflow.stderr]
                .findAll { stream -> stream != null && stream.toString() }
                .collect { stream -> stream.toString() }
                .join("\n")

            assertAll(
                { assert workflow.failed },
                { assert workflow.exitStatus != 0 },
                { assert output.contains("Missing required field") },
                { assert output.contains("traitFromSourceMappedIds") },
                { assert workflow.trace.tasks().size() == 0 }
            )
        }
    }

    test("fails fast when effectiveSampleSize is not numeric") {
        when {
            params {
                manifest = new File("testdata/manifest.invalid_sample_size.tsv").canonicalPath
                manifest_base_dir = new File(".").canonicalPath
            }
        }

        then {
            def output = [workflow.stdout, workflow.stderr]
                .findAll { stream -> stream != null && stream.toString() }
                .collect { stream -> stream.toString() }
                .join("\n")

            assertAll(
                { assert workflow.failed },
                { assert workflow.exitStatus != 0 },
                { assert output.contains("effectiveSampleSize") },
                { assert output.contains("should be") },
                { assert workflow.trace.tasks().size() == 0 }
            )
        }
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `nf-test test tests/manifest_schema_validation.nf.test`

Expected: both tests **FAIL**. Today, `read_manifest()` parses positionally with no schema, so:
- The missing-column fixture shifts every column left by one and throws `IndexOutOfBoundsException` reading `row[6]` — not the `"Missing required field"` message the test expects.
- The invalid-sample-size fixture throws `NumberFormatException: For input string: "notanumber"` from `row[6].toInteger()` — not the `"should be"` message the test expects.

- [ ] **Step 4: Write the manifest samplesheet schema**

Create `assets/schema_manifest.json`:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://raw.githubusercontent.com/opentargets/nf-fine-mapping/main/assets/schema_manifest.json",
  "title": "nf-fine-mapping manifest schema",
  "description": "Schema for the tab-separated manifest describing studies and summary-statistics inputs.",
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "runId": {"type": "string", "minLength": 1, "meta": "runId"},
      "studyId": {"type": "string", "minLength": 1, "meta": "studyId"},
      "route": {"type": "string", "minLength": 1, "meta": "route"},
      "summarystatsLocation": {"type": "string", "minLength": 1},
      "majorAncestry": {"type": "string", "minLength": 1, "meta": "ancestry"},
      "traitFromSourceMappedIds": {"type": "string", "minLength": 1},
      "effectiveSampleSize": {"type": "integer", "minimum": 1, "meta": "sampleSize"}
    },
    "required": ["runId", "studyId", "route", "summarystatsLocation", "majorAncestry", "traitFromSourceMappedIds", "effectiveSampleSize"]
  }
}
```

(Property order matters to nf-schema — it determines the order of the non-meta fields in the returned tuple. `summarystatsLocation` then `traitFromSourceMappedIds` here, matching the order `manifest_entry_to_record` reads them in Step 5.)

- [ ] **Step 5: Rewrite read_manifest() to use samplesheetToList()**

In `main.nf`, find:

```groovy
def manifest_row_to_record(row: List<String>, manifest_base_dir: String) -> Map {
    def traitSet: List<String> = row[5].tokenize(',')
    def summary_statistics_path = row[3].startsWith('/') ? row[3] : "${manifest_base_dir}/${row[3]}"

    def meta = [
        runId: row[0],
        studyId: row[1],
        route: row[2],
        ancestry: row[4],
        traitSet: traitSet,
        sampleSize: row[6].toInteger(),
    ]

    return [
        summary_statistics_path: file(summary_statistics_path),
        meta: meta,
    ]
}


def read_manifest(path: String) {
    def manifest_channel = channel.fromPath(path)
        .flatMap { manifest ->
            manifest.splitCsv(
                sep: '\t',
                skip: 1,
            )
        }
        .map { row ->
            manifest_row_to_record(row as List<String>, params.manifest_base_dir)
        }

    log.info("Manifest file read successfully: ${path}")

    return manifest_channel
}
```

Replace with:

```groovy
def manifest_entry_to_record(entry: List, manifest_base_dir: String) -> Map {
    def meta = entry[0] as Map
    def summarystats_location = entry[1] as String
    def trait_from_source_mapped_ids = entry[2] as String

    def summary_statistics_path = summarystats_location.startsWith('/')
        ? summarystats_location
        : "${manifest_base_dir}/${summarystats_location}"

    return [
        summary_statistics_path: file(summary_statistics_path),
        meta: meta + [traitSet: trait_from_source_mapped_ids.tokenize(',')],
    ]
}


def read_manifest(path: String) {
    def manifest_channel = channel.fromList(
        samplesheetToList(path, "assets/schema_manifest.json")
    ).map { entry ->
        manifest_entry_to_record(entry as List, params.manifest_base_dir)
    }

    log.info("Manifest file read successfully: ${path}")

    return manifest_channel
}
```

`samplesheetToList()` returns each row as `[metaMap, summarystatsLocation, traitFromSourceMappedIds]` — the `meta` map already carries `runId`, `studyId`, `route`, `ancestry`, `sampleSize` (typed as `Integer`) as declared by the schema's `meta` keys in Step 4, matching the shape `MANIFEST_VALIDATION` and every downstream workflow already expect (verified against `tests/workflows/manifest_validation.nf.test`'s fixtures, which construct this exact meta shape by hand).

- [ ] **Step 6: Run the tests to verify they pass, then run the full suite**

Run: `nf-test test tests/manifest_schema_validation.nf.test`

Expected: both tests **PASS**.

Run: `nf-test test`

Expected: all existing tests still pass, in particular `tests/default.nf.test`'s tests that exercise `testdata/manifest.tsv`, `testdata/manifest.full.tsv`, and `testdata/manifest.invalid_run.tsv` end-to-end through `read_manifest()` (these are the regression check for the 4 pre-existing manifest fixtures — no fixture changes are needed since all 4 already conform to the new schema), and `tests/workflows/manifest_validation.nf.test` (unaffected — it feeds `MANIFEST_VALIDATION` directly with hand-built channel input, bypassing `read_manifest()` entirely).

- [ ] **Step 7: Cross-reference the QC docs, if present**

Check whether `docs/user-guide/qc-validation.rst` exists in this worktree:

```bash
test -f docs/user-guide/qc-validation.rst && echo exists || echo missing
```

If `missing`: skip this step. That file ships in the still-unmerged PR #25 (`worktree-qc-validation-docs` branch); once it merges to `main`, revisit this step as a small follow-up.

If `exists`: add a short cross-reference. Find the paragraph starting `The dashed *Validation & QC* line above is exactly this mechanism:` and insert a new paragraph immediately after it:

```rst
Manifest *structural* problems — a missing column, a non-numeric sample size — are checked
separately and earlier, by an `nf-schema <https://nextflow-io.github.io/nf-schema/>`_ samplesheet
schema (``assets/schema_manifest.json``). That check is fail-fast: it halts the whole run before
any process starts, reporting every row's errors together. It is deliberately not part of the
``validationStage``/``reason`` status-record system described below — a malformed manifest file is
a broken input, not a per-run semantic condition to cascade through the pipeline.
```

- [ ] **Step 8: Commit**

```bash
git add assets/schema_manifest.json testdata/manifest.missing_column.tsv testdata/manifest.invalid_sample_size.tsv tests/manifest_schema_validation.nf.test main.nf
git add docs/user-guide/qc-validation.rst 2>/dev/null || true
git commit -m "feat: validate the manifest TSV with an nf-schema samplesheet schema"
```

---

## Final Verification

- [ ] Run `make unit-test` (equivalent to `nf-test test`) from the repo root — all tests pass.
- [ ] Run `nextflow config -show` (or `nextflow run main.nf -stub-run -profile testFullCollectorHailingDucks`) once by hand to confirm the plugin downloads and the pipeline still completes successfully end-to-end.
- [ ] Confirm `git log --oneline -2` shows exactly the two commits from Task 1 and Task 2.
