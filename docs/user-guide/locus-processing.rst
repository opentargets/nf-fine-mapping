Locus processing
================

The locus-processing layer contains two steps.

Locus breaker
-------------

The collector reads one flat summary-statistics Parquet dataset and writes one
flat ``StudyLocus`` Parquet file. The output schema is compatible with
Gentropy, including mantissa/exponent p-values, nullable quality-control
fields, and nested locus variants.

The implementation uses DuckDB SQL for windowing, interval construction,
filtering, projection, and Parquet output. WBC lead selection retains the
algorithmic semantics needed for parity with Gentropy.

Locus collection
----------------

Locus collection turns each study's separate loci into shared, multi-study
locus sets, ready for fine-mapping.

**How a locus set is built:**

- **Each locus gets its own "lead" position first** — its most significant
  qualifying variant, found only within that locus's own boundaries. A
  locus with no qualifying variant is dropped before any comparison.
- **Overlapping loci are compared pairwise**, based on where each one's
  lead sits relative to the overlap:

  - Both leads inside the overlap → same signal, both tighten to the
    shared window.
  - Only one lead inside → that locus keeps the shared part; the other
    keeps only its own remainder.
  - Neither lead inside → both keep only their own remainders; the
    disputed middle is dropped, not given to either side.
  - One locus fully contains another → the larger locus wins.

- **Any disagreement starts a new locus set** — even with no position gap
  between the results. This is what stops locus sets from growing without
  limit in densely packed regions.

**Example:** study A's locus (1–300, lead 120) fully contains study B's
locus (150–200, lead 170), so they merge into one locus set spanning
1–300. Study C's locus (250–400, lead 280) then overlaps that merged span
in 250–300; since C's own lead falls in that overlap but neither A's nor
B's does, C keeps the disputed zone and the A/B locus set trims back to
249 — the two locus sets end up touching, never overlapping.

Because growth only ever happens through agreement, and every input locus
already has a bounded size from locus breaking
(``params.locus_breaker_large_loci_size``), a locus set can't grow without
limit — there's no separate size cap or rejection step. When using the
collector locus breaker, the pipeline checks at startup that
``params.canonical_region_max_region_span_bp`` is at least as large as
``params.locus_breaker_large_loci_size``. A locus set's contributing loci
are only those studies whose own lead still falls inside its final
boundaries.

The command writes one locus-set Parquet file per published
``fineMappingLocusSetId`` (published as *Collected Loci*, see
:doc:`overview`), one stats Parquet file, and one JSON statistics document
per run. The published output is the input for downstream LD annotation
and fine-mapping.
