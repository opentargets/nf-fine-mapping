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

The collection step performs a bounded sweep across all input studies to
assemble canonical multi-ancestry locus sets, rather than classifying loci
by how many studies they overlap: loci from every study are pooled, sorted
by genomic position, and merged whenever they overlap into non-overlapping
*canonical regions*; a genuine gap between loci starts a new region. Two
regions the sweep emits never share genomic coordinates — this is a
structural guarantee, not a best-effort heuristic, since a shared position
would let two regions independently pick the same variant as a study's
"lead" and publish it twice.

Within each region, every study's most significant variant above the
configured MAF cutoff (``params.canonical_region_min_maf``) becomes that
study's component of the region's ``fineMappingLocusSetId``. A region only
publishes if every input study has a qualifying variant in it; a region where
at least one study has none is excluded and counted under
``NO_VARIANTS_IN_LOCUS`` in the run's ``notPromotedReasons``.

Region size is soft-capped by ``params.canonical_region_max_region_span_bp``
(default 3,000,000 bp): merging never stops at the cap if the alternative
would be an overlapping split, so a region can legitimately grow past the cap
when the underlying loci are densely clustered. Such regions are tagged
``SOURCE_LOCUS_EXCEEDS_MAX_REGION_SPAN`` (a single input locus is individually
oversized) and/or ``MERGED_REGION_EXCEEDS_MAX_REGION_SPAN`` (the merged group
is oversized), and counted in the run's ``nRegionsExceedingSpanCap``
statistic. A region that grows past ``canonical_region_max_region_span_bp``
times 5 is rejected outright instead of published — at that scale, downstream
LD annotation (quadratic in variant count) becomes intractable — and counted
under ``REGION_SPAN_EXCEEDS_REJECT_THRESHOLD`` in ``notPromotedReasons``
instead.

Two regions that independently select the identical set of lead variants
across every study collapse into one published row, with bounds taken as the
intersection of the colliding regions, tagged
``MULTIPLE_FINE_MAPPING_LOCUS_SETS_OVERLAP_THE_SAME_SIGNAL``.

The command writes one locus-set Parquet file per published
``fineMappingLocusSetId`` (published as *Collected Loci*, see
:doc:`overview`), one stats Parquet file (one row per published set,
including its QC tags), and one JSON statistics document per run
(candidate/published/rejected counts, size distributions, and
``notPromotedReasons``). The published output is the input for downstream
LD annotation and fine-mapping.
