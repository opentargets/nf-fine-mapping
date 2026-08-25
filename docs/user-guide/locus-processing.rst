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

Locus collection turns each study's separate loci into shared,
multi-study locus sets, ready for fine-mapping.

**How a locus set is built:**

- **Find each locus's own strongest signal first.** Before comparing
  loci to each other, each study's own locus gets one fixed "lead"
  position — its most significant qualifying variant, found only
  within that locus's own boundaries. A locus with no qualifying
  variant is dropped before it's ever compared to anything else.
- **Compare overlapping loci pairwise.** Loci are lined up by genomic
  position. When two neighboring loci overlap, they're resolved based
  on where each one's own lead sits:

  - If both leads fall inside the overlap, they're treated as the
    same signal and both loci are tightened down to just that shared
    window.
  - If only one lead falls inside the overlap, that locus keeps the
    shared part; the other locus keeps only its own separate
    remainder.
  - If neither lead falls inside the overlap, both loci keep only
    their own separate remainders, and the disputed middle ground is
    dropped rather than handed to either side.
  - If one locus's boundaries fully contain another's, the larger
    locus wins and both are treated as the wider span.

- **Draw a new locus set wherever two loci disagree.** Even if the
  resulting boundaries end up touching with no gap between them,
  disagreement always starts a new locus set — this is what keeps
  locus sets from silently growing without limit in densely packed
  regions.

**Example:** study A reports a locus at 1–300 with its own strongest
signal at 120, and study B reports a locus at 150–200 (fully inside
A) with its strongest signal at 170. Since B sits entirely inside A,
they're treated as one shared locus set spanning 1–300. If a third
study, C, then reports a locus at 250–400 with its own strongest
signal at 280, C's locus overlaps that shared span in 250–300. C's
own lead falls inside that disputed overlap, but neither A's nor B's
lead does, so C keeps the whole shared stretch and the A/B locus
set's own end trims back to 249 — the two locus sets end up touching
with no gap, but never overlapping each other.

Because a locus set only ever grows through *agreement*, and every
input locus already has a known, bounded size from locus breaking
(``params.locus_breaker_large_loci_size``), a locus set can't grow
into an arbitrarily large region the way blind merging could — there
is no separate size cap or rejection step during construction. The
pipeline checks at startup that the region-size limit
(``params.canonical_region_max_region_span_bp``) is at least as large
as the upstream locus size, so a locus set can never be asked to be
smaller than a single input locus. A locus set's contributing loci
are drawn only from studies whose own lead still falls inside its
final boundaries.

The command writes one locus-set Parquet file per published
``fineMappingLocusSetId`` (published as *Collected Loci*, see
:doc:`overview`), one stats Parquet file, and one JSON statistics
document per run. The published output is the input for downstream
LD annotation and fine-mapping.
