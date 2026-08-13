---
marp: true
theme: default
paginate: true
size: 16:9
title: "Multi-ancestry fine-mapping: pipeline, native LD, and method applications"
description: "Open Targets project update for 13 August 2026"
style: |
  section {
    background: #07111f;
    color: #e2e8f0;
    font-family: Inter, Aptos, Arial, sans-serif;
    padding: 54px 68px;
  }
  h1 { color: #f8fafc; font-size: 48px; line-height: 1.06; letter-spacing: -0.025em; }
  h2 { color: #67e8f9; font-size: 31px; margin-bottom: 18px; }
  h3 { color: #f8fafc; font-size: 23px; }
  p, li { font-size: 22px; line-height: 1.38; }
  strong { color: #f8fafc; }
  a { color: #67e8f9; }
  table { width: 100%; font-size: 18px; }
  th { background: #172033; color: #67e8f9; }
  td { background: #0f172a; }
  code { background: #172033; color: #f8fafc; }
  footer { color: #94a3b8; font-size: 12px; }
  section.title h1 { font-size: 60px; max-width: 980px; }
  section.title h2 { color: #cbd5e1; font-weight: 500; }
  section.visual { padding: 28px 40px; }
  section.visual h1 { margin: 0 0 14px 20px; font-size: 40px; }
  section.references p, section.references li { font-size: 16px; line-height: 1.3; }
  section.references h1 { font-size: 38px; }
  .eyebrow { color: #2dd4bf; font-weight: 800; letter-spacing: 0.12em; text-transform: uppercase; }
  .accent { color: #67e8f9; }
  .muted { color: #94a3b8; }
  .kicker { font-size: 28px; color: #cbd5e1; max-width: 960px; }
  .metric { display: inline-block; min-width: 175px; margin: 8px 18px 8px 0; padding: 18px; border-radius: 16px; background: #0f172a; border: 1px solid #334155; }
  .metric b { display: block; color: #67e8f9; font-size: 31px; }
  .metric span { color: #cbd5e1; font-size: 15px; }
  .callout { margin-top: 22px; padding: 18px 22px; border-left: 5px solid #2dd4bf; background: #0f172a; }
  .warning { border-left-color: #f59e0b; }
  .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 34px; }
  .card { padding: 20px 24px; border-radius: 18px; background: #0f172a; border: 1px solid #334155; }
  .source { color: #94a3b8; font-size: 13px; }
---

<!-- _class: title -->
<!-- _paginate: false -->

<p class="eyebrow">Open Targets · project update · 13 August 2026</p>

# Multi-ancestry fine-mapping at scale

## Pipeline orchestration · native LD access · method application refactors

<p class="kicker">A reproducible route from GWAS summary statistics to comparable, Gentropy-compatible fine-mapping outputs.</p>

---

# Why this work exists

Today, Open Targets fine-maps studies mostly **one study and one ancestry at a time**. The opportunity is to group studies of the same trait and use population differences in LD to separate causal variants from correlated tags.

<div>
  <div class="metric"><b>7,490</b><span>multi-study trait sets in release 26.06</span></div>
  <div class="metric"><b>Summary stats</b><span>no individual-level genotypes required</span></div>
  <div class="metric"><b>Out-of-sample LD</b><span>gnomAD and Pan-UKBB references</span></div>
</div>

<div class="callout">
<strong>What changed:</strong> we now have a scalable Nextflow route that groups studies, constructs cross-study loci, retrieves ancestry-matched LD, and fans the same validated inputs into multiple fine-mapping engines.
</div>

<p class="source">Sources: [P1], [P2], [P3]</p>

---

<!-- _class: visual -->

# The pipeline, as a metro map

![Metro-style pipeline map showing selectable locus, LD, and fine-mapping routes](assets/pipeline-metro.svg)

<p class="source">Sources: [P2], [P3], [P4]</p>

---

# 1 · nf-fine-mapping: the orchestration layer

<div class="two-col">
<div class="card">

### Stable stages

- Manifest validation and route selection
- Per-study locus breaking
- Per-`runId` overlap collection
- Per-locus-set LD annotation and validation
- Configurable fine-mapping fan-out
- Gentropy-compatible publication

</div>
<div class="card">

### Engineering properties

- Typed Nextflow channels preserve metadata
- Task boundaries support scatter, caching and `-resume`
- Collector/Gentropy backends remain comparable
- Quality failures emit status instead of misleading outputs
- Local and Google Cloud profiles share the same contracts

</div>
</div>

<div class="callout">
<strong>Current verification:</strong> 23/23 nf-test checks pass; Collector CI is green at commit <code>4d9db06</code>. MultiSuSiE is the default fine-mapping route.
</div>

<p class="source">Sources: [P2], [P3], [P5], [P6]</p>

---

# 2 · Hailing Ducks + Collector

![Architecture showing Pan-UKBB, Hailing Ducks, Collector, and stable Nextflow contracts](assets/hailing-collector-stack.svg)

<p class="source">Sources: [H1], [H2], [H3], [H4]</p>

---

<!-- _class: visual -->

# Native LD retrieval: measured impact

![Benchmark comparing Hailing Ducks and Gentropy on wall time, memory, output size, and correctness](assets/hailing-benchmark.svg)

<p class="source">Benchmark run 29 July 2026 against public Pan-UKBB resources. Source: [H5]</p>

---

# 3 · Refactoring the SuSiE-family applications

![Shared application contract around MultiSuSiE, SuSiEx, and SuShiE](assets/susie-contract.svg)

<p class="source">Sources: [M1], [M2], [M3], [M4]</p>

---

# Same lifecycle, deliberately different science

| Method | Variant semantics | Native engine | Pipeline maturity |
|---|---|---|---|
| **MultiSuSiE** | Union across ancestries; missingness masks | Python `multisusie_rss` | **Default**, synthetic container fit in CI |
| **SuSiEx** | Union plus explicit native indicator mask | C++ via pybind11 | Packaged **PoC route** |
| **SuShiE** | Strict shared GWAS ∩ shared LD intersection | Python/JAX | Packaged **PoC route** |

The refactor standardises **inputs, validation, CLI/container execution, outputs and status handling**. It does not merge or rewrite the scientific models.

<div class="callout">
Every method can emit <strong>StudyLocus Parquet</strong> for Gentropy, <strong>AnnData H5AD</strong> for full posterior detail, and <strong>stats.json</strong> for machine-readable quality outcomes.
</div>

<p class="source">Sources: [M1]–[M7]</p>

---

# What is ready — and what comes next

<div class="two-col">
<div class="card">

### Ready to showcase

- End-to-end typed workflow topology
- Collector locus processing and validation
- Selectable, parity-tested native LD backend
- Shared application contract across three methods
- Green Collector CI and pipeline stub suite

</div>
<div class="card">

### Promotion work

- Resolve same-position multi-variant coverage in Hailing Ducks
- Run the switched Hailing profile end to end
- Pin released method images in pipeline configuration
- Add real-container pipeline evidence for SuSiEx and SuShiE
- Benchmark methods on the same representative locus sets

</div>
</div>

<div class="callout warning">
<strong>Decision boundary:</strong> Gentropy remains the production LD default; Hailing Ducks is opt-in. MultiSuSiE is the method default; SuSiEx and SuShiE remain PoC pipeline routes until real-container integration evidence is complete.
</div>

<p class="source">Sources: [H5], [P3], [P5], [M1], [M8]</p>

---

<!-- _class: references -->

# References · pipeline and LD

- **[P1]** [Project abstract: rationale and release 26.06 scale](../abstract/abstract.qmd)
- **[P2]** [Pipeline overview](../user-guide/overview.rst)
- **[P3]** [Workflow contracts and selectable backends](../user-guide/workflows.rst)
- **[P4]** [Top-level Nextflow workflow](../../main.nf)
- **[P5]** [Execution and verification guide](../user-guide/execution.rst)
- **[P6]** [Green Collector CI after contract-test repair](https://github.com/opentargets/nf-fine-mapping/actions/runs/31699948286)
- **[H1]** [Hailing Ducks overview](https://github.com/project-defiant/hailing-ducks/blob/main/README.md)
- **[H2]** [Batch-optimised LD query design and status contract](https://github.com/project-defiant/hailing-ducks/blob/main/docs/LD-QUERY.md)
- **[H3]** [Collector Hailing Ducks adapter](../../tools/collector/src/collector/hailing_ld.py)
- **[H4]** [Selectable locus-annotation workflow](../../workflows/locus_annotation/main.nf)
- **[H5]** [Hailing Ducks LD benchmark and rollout gates](../benchmarks/hailing-ducks-ld-annotation.rst)
- **[H6]** [Pan-UK Biobank GWAS resource](https://doi.org/10.1038/s41588-025-02335-7)
- **[P7]** [Nextflow](https://doi.org/10.1038/nbt.3820)

---

<!-- _class: references -->

# References · method applications

- **[M1]** [Fine-mapping routes and common I/O contract](../user-guide/fine-mapping-routes.rst)
- **[M2]** [MultiSuSiE Open Targets application fork](https://github.com/project-defiant/MultiSuSiE)
- **[M3]** [SuSiEx Open Targets application fork](https://github.com/project-defiant/susiex)
- **[M4]** [SuShiE Open Targets application fork](https://github.com/project-defiant/sushie)
- **[M5]** Wang et al. [SuSiE](https://doi.org/10.1111/rssb.12388), *JRSS B* (2020)
- **[M6]** Yuan et al. [SuSiEx](https://doi.org/10.1038/s41588-024-01870-z), *Nature Genetics* (2024)
- **[M7]** Lu et al. [SuShiE](https://doi.org/10.1038/s41588-025-02262-7), *Nature Genetics* (2025)
- **[M8]** Rossen et al. [MultiSuSiE](https://doi.org/10.1038/s41588-025-02450-5), *Nature Genetics* (2026)

<div class="callout">
<strong>Repository state used for this deck:</strong> nf-fine-mapping <code>main@4d9db06</code>; Hailing Ducks <code>v1.1.0</code>; MultiSuSiE <code>v1.0.0</code>; SuSiEx <code>v0.1.0</code>; SuShiE <code>v1.0.0</code>. Status checked 13 August 2026.
</div>
