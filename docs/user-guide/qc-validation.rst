QC and validation
==================

Every processing stage in the pipeline emits its own quality-control status
records. A status record marks one run, or one locus set, as invalid; the
pipeline filters that record's inputs out before they reach the next stage,
so a known-bad input is never silently carried forward into fine-mapping.

.. image:: ../architecture/pipeline-metro-map.svg
   :alt: Metro map of the nf-fine-mapping pipeline, highlighting the dashed
         Validation & QC line that runs alongside manifest validation, both
         locus-breaker routes, locus collection, and both LD-annotation
         routes, converging on the Reports output.

The dashed *Validation & QC* line above is exactly this mechanism: every
stage that can invalidate an input feeds it, and all four converge on the
same published ``Reports`` output.

Validation stages
------------------

Four stages can emit a status record:

.. list-table::
   :header-rows: 1

   * - ``validationStage``
     - Reason
     - Trigger
     - Implementation
   * - ``MANIFEST``
     - ``UNREGISTERED_ANCESTRY``
     - A manifest row's ancestry has no matching entry in ``params.ld_registry``.
     - Inline Groovy check in ``main.nf`` (``MANIFEST_VALIDATION``).
   * - ``LOCUS_BREAKER``
     - ``EMPTY_DATASET``
     - The per-study locus-breaker output has zero logical rows.
     - ``collector empty_status`` (both the ``collector`` and ``gentropy``
       locus-breaker routes call it).
   * - ``LOCUS_COLLECTION``
     - ``EMPTY_DATASET``
     - The per-run collected-loci output has zero logical rows.
     - ``collector empty_status``.
   * - ``LD_ANNOTATION``
     - ``EMPTY_LD_PAIRS``
     - Any ancestry in a locus set's pairwise LD statistics has
       ``n_ld_pairs == 0``.
     - ``collector check_ld_pair_stats`` (both the ``gentropy`` and
       ``hailing_ducks`` LD-annotation routes call it).

``MANIFEST``, ``LOCUS_BREAKER``, and ``LOCUS_COLLECTION`` records are keyed
by ``runId``. ``LD_ANNOTATION`` records are keyed by
``fineMappingLocusSetId`` instead — see `Why the filtering key changes`_.

Status record schema
---------------------

Each stage writes newline-delimited JSON. A typical record looks like:

.. code-block:: json

   {"runId": "run-1", "path": "collected_loci/fine_mapping_locus_sets/run-1", "validationStage": "LOCUS_COLLECTION", "reason": "EMPTY_DATASET"}

The ``LD_ANNOTATION`` variant adds ``fineMappingLocusSetId`` in place of a
bare ``runId``-scoped path. ``fineMappingLocusSetId`` is an MD5 digest of
the set's sorted ``studyLocusId`` values, not a coordinate-derived string:

.. code-block:: json

   {"runId": "run-1", "fineMappingLocusSetId": "3f2a9c7e1b8d4f6a0c5e9b2d7a1f4c8e", "path": "stats.jsonl", "validationStage": "LD_ANNOTATION", "reason": "EMPTY_LD_PAIRS"}

Status records are published alongside the pipeline's other outputs:

.. list-table::
   :header-rows: 1

   * - Stage
     - Published path
   * - ``MANIFEST``
     - ``validation/manifest``
   * - ``LOCUS_BREAKER``
     - ``status/locus_breaker``
   * - ``LOCUS_COLLECTION``
     - ``status/locus_collection``
   * - ``LD_ANNOTATION``
     - ``status/locus_annotation``

Why the filtering key changes
------------------------------

The first three stages operate one-study-per-``runId``, so invalidation
accumulates forward by ``runId``: a run marked invalid at ``MANIFEST`` is
excluded from ``LOCUS_BREAKER``'s input, and a run marked invalid at either
``MANIFEST`` or ``LOCUS_BREAKER`` is excluded from ``LOCUS_COLLECTION``'s
input.

``LOCUS_COLLECTION`` merges loci from multiple studies into shared
``fineMappingLocusSetId`` groups, so a single ``runId`` no longer identifies
one output row from that point on. ``LD_ANNOTATION`` therefore filters its
own output by ``fineMappingLocusSetId`` instead of by ``runId``, removing
any locus set with zero LD pairs in any ancestry before it reaches
``FINE_MAPPING``. No further validation stage runs after LD annotation.

Within ``LOCUS_COLLECTION`` itself, candidate-level QC is recorded in
``stats.parquet`` before this stage emits any run-level ``EMPTY_DATASET``
status. ``INSUFFICIENT_VARIANT_OVERLAP`` marks candidates whose exact
post-MAF multi-study Jaccard score falls below
``canonical_region_min_variant_overlap_proportion``. ``NO_VARIANTS_IN_LOCUS``
takes precedence whenever any component has zero post-MAF variants; in that
case the overlap fields are null and the candidate is not also labelled as an
insufficient-overlap failure.

Because ``stats.parquet`` now retains unpublished candidates, a null
``fineMappingLocusSetId`` in that file means the candidate failed a blocking
collection QC and was not materialized under
``fine_mapping_locus_sets/``. The stage-level ``LOCUS_COLLECTION /
EMPTY_DATASET`` status still means the published locus-set directory ended up
empty after applying those candidate-level blockers.

.. _duplicate-summary-statistics-limitation:

Duplicate summary-statistics limitation
----------------------------------------

Summary statistics must contain at most one row for each ``(studyId,
variantId)`` pair before LocusBreaker runs. Duplicate rows are not a harmless
formatting detail: they can contain different p-values, effect sizes, or
alleles, so there is no generally safe row to keep.

The two available LocusBreaker backends do not currently handle this case in
the same way:

* the collector removes every row for a duplicated variant before clumping;
* Gentropy can rank duplicate rows during clumping before later processing
  removes ambiguous variants.

Consequently, a collector-versus-Gentropy comparison is not meaningful when
the input contains duplicated variant IDs. The lead variant, locus boundary,
and downstream fine-mapping input can differ even when both tasks complete
successfully. This is a known limitation, not evidence that one backend is
numerically wrong.

Until a shared preflight validator is available, reject or repair duplicated
summary statistics before starting the pipeline. Do not silently keep the
most significant duplicate. A simple DuckDB check is:

.. code-block:: sql

   SELECT studyId, COUNT(*) AS n_rows,
          COUNT(DISTINCT variantId) AS n_variant_ids,
          COUNT(*) - COUNT(DISTINCT variantId) AS n_duplicate_rows
   FROM read_parquet('summary_statistics.parquet')
   GROUP BY studyId
   HAVING COUNT(*) > COUNT(DISTINCT variantId);

The collector canonical-region command silently drops all rows for any
duplicated ``variantId`` before processing — both copies are removed, not
just the weaker one. This is consistent with the collector LocusBreaker's
``QUALIFY count(*) OVER (PARTITION BY studyId, variantId) = 1`` filter.
Duplicate counts in the run report and a shared Gentropy preflight are
tracked in GitHub issue #11.

Triggering validation paths in stub tests
-------------------------------------------

The ``collector empty_status`` and ``collector check_ld_pair_stats`` stub
blocks are silent by default, since most stub fixtures are not meant to
exercise the invalid path. Force them on with:

.. code-block:: groovy

   params {
       empty_status_stub_emit = true
       ld_pair_stats_stub_emit = true
       ld_pair_stats_stub_empty_locus_set_ids = ['3f2a9c7e1b8d4f6a0c5e9b2d7a1f4c8e']
   }

``empty_status_stub_emit`` covers both the ``LOCUS_BREAKER`` and
``LOCUS_COLLECTION`` stages, since they share the same
``COLLECTOR_EMPTY_STATUS`` process. ``MANIFEST`` validation needs no stub
flag: its ``UNREGISTERED_ANCESTRY`` check is a plain Groovy comparison
against ``params.ld_registry``, so it behaves identically in stub and
non-stub runs.
