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

- **Merge overlapping loci.** Loci from every study are lined up by
  genomic position. Any loci that overlap are merged into one region.
  A real gap between loci starts a new region.
- **Pick one lead variant per study.** Each study contributes its most
  significant variant above the MAF cutoff
  (``params.canonical_region_min_maf``). If any study has no qualifying
  variant in a region, that region is dropped (``NO_VARIANTS_IN_LOCUS``).
- **Cap region size.** Regions have a soft size limit
  (``params.canonical_region_max_region_span_bp``, 3,000,000 bp by
  default). Densely packed loci can push a region past this limit —
  that's allowed and flagged (``SOURCE_LOCUS_EXCEEDS_MAX_REGION_SPAN`` or
  ``MERGED_REGION_EXCEEDS_MAX_REGION_SPAN``), rather than split, because
  splitting could let two regions overlap. A region that grows more than
  5x past the limit is dropped instead of published
  (``REGION_SPAN_EXCEEDS_REJECT_THRESHOLD``), since it would be too large
  for LD annotation to process.

**Example:** study A reports a locus at 1-200, study B at 150-300, and
study C at 250-400. Since each pair overlaps, all three merge into one
region spanning 1-400. The pipeline then picks each study's best variant
inside that combined range.

Regions never overlap each other. This means the same variant can never
end up published as the "lead" for two different locus sets.

Each published region becomes one locus set with a unique
``fineMappingLocusSetId``, written as one Parquet file (shown as
*Collected Loci* in :doc:`overview`). A stats file alongside it records
how many candidate regions were published, dropped, or capped.
