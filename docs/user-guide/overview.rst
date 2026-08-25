Pipeline overview
=================

.. image:: ../architecture/pipeline-metro-map.svg
   :alt: Metro map of the nf-fine-mapping pipeline, from an OpenTargets study
         through fine-mapping planning, manifest generation, locus breaking,
         locus collection, LD annotation and fine-mapping to published
         outputs.

Regenerate this diagram with ``make docs-metro-map`` after changing
``main.nf`` or its subworkflows; the source lives at
``docs/architecture/pipeline-metro-map.mmd``.

Outputs
-------

The workflow's ``output {}`` block (in ``main.nf``) is the authoritative
source for what gets published. All paths below are relative to
``params.output_dir``.

.. list-table::
   :header-rows: 1
   :widths: 22 30 15 33

   * - Output
     - Path
     - Produced by
     - Contents
   * - Loci
     - ``locus_breaker_clumped_study_locus/``
     - LocusBreaker
     - Per-study clumped-locus Parquet, filtered to studies that passed
       manifest validation.
   * - Collected Loci
     - ``collected_loci/full_overlaps/``
     - Locus Collection
     - Canonical multi-ancestry locus sets assembled from all input studies
       for a run; the sole candidate-set output carried forward into LD
       annotation and fine-mapping.
   * - Collection Stats
     - ``collected_loci/stats/``
     - Locus Collection
     - Size and count statistics for the canonical-region collection.
   * - Annotated Locus Sets
     - ``locus_annotation/``
     - LD Annotation
     - LD-annotated locus-set Parquet. The pairwise LD matrix itself is a
       transient work artifact and is not published.
   * - LD-pair Stats
     - ``locus_annotation/stats/``
     - LD Annotation
     - Per-locus-set statistics on LD-pair coverage.
   * - MultiSuSiE Results
     - ``multisusie/<runId>/<fineMappingLocusSetId>/``
     - Fine Mapping
     - Gentropy-compatible StudyLocus Parquet, extended AnnData (``.h5ad``)
       results, and a JSON status record.
   * - Manifest Validation Report
     - ``validation/manifest/``
     - Manifest Validation
     - JSONL records of manifest rows rejected during validation.
   * - LocusBreaker Status
     - ``status/locus_breaker/``
     - LocusBreaker
     - JSONL records flagging runs with empty LocusBreaker output.
   * - Locus Collection Status
     - ``status/locus_collection/``
     - Locus Collection
     - JSONL records flagging runs with empty collection output.
   * - Locus Annotation Status
     - ``status/locus_annotation/``
     - LD Annotation
     - JSONL records flagging locus sets with no valid LD pairs.

IDIC performs summary-statistics fine-mapping using out-of-sample LD. The
workflow is designed for multi-study and multi-ancestry analyses and emits
Gentropy-compatible datasets for downstream Open Targets processing.

The major stages are:

1. Read and filter study metadata from a manifest.
2. Break summary statistics into study loci.
3. Collect canonical multi-ancestry locus sets across studies in a shared
   ``runId``.
4. Prepare LD and fine-mapping inputs.
5. Run a configured fine-mapping route, such as MultiSuSiE.

The pipeline can run locally for development or on Google Cloud through the
provided Nextflow profiles.

Data resolution
---------------

The workflow operates at summary-statistics resolution. It does not require
individual-level genotypes. LD is supplied through external reference panels,
which makes the workflow suitable for large-scale GWAS Catalog analyses while
introducing the usual out-of-sample LD limitations.
