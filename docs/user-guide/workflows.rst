Workflows
=========

Locus breaker
-------------

``LOCUS_BREAKER`` accepts manifest-derived study metadata and summary-statistics
paths. The backend is selected with ``params.locus_breaker_method``:

* ``collector`` — DuckDB-based collector implementation, the default.
* ``gentropy`` — Gentropy Spark implementation for parity and comparison.

The workflow passes native Nextflow tuples containing a metadata ``Map`` and a
``Path``. This keeps task hashing stable and preserves ``-resume`` behavior.

Locus collection
----------------

``LOCUS_COLLECTION`` groups locus outputs by ``runId`` and runs one collection
task per run. It emits full-overlap, partial-overlap, non-overlap, and JSON
statistics channels. Full-overlap loci are the candidate sets passed to later
fine-mapping stages.

Locus annotation and MultiSuSiE
-------------------------------

``LOCUS_ANNOTATION`` runs LD annotation and then invokes the MultiSuSiE CLI
once per ``fineMappingLocusSetId``. The process receives the locus-set Parquet
file, the MultiAncestryPairwiseLD dataset, and the study metadata carried by
the channel. Metadata is serialized as JSONL inside the task, so no separate
metadata process is required.

The MultiSuSiE process emits a metadata-preserving record containing:

* ``study_locus_path`` — the Gentropy-compatible StudyLocus Parquet output;
* ``extended_results_path`` — the AnnData H5AD output with component-level
  posterior results and provenance;
* ``stats_path`` — the JSON status record for the locus-set fit;
* ``runId``, ``fine_mapping_locus_set_id``, and the input ``metas``.

The image is configured with ``params.multisusie_container`` and defaults to
``multisusie:latest``. All method options are passed through the process
``task.ext.args`` interface. For example:

.. code-block:: text

   withName: MULTISUSIE_FINE_MAPPING {
       ext.args = '--L 10 --max-iter 100 --low-memory-mode'
   }

Non-converged or non-reportable fits write ``stats.json`` with a failure status
and exit successfully so the workflow can continue. The result files are
omitted for those fits. Input validation and output-writing errors still fail
the process.

Fine-mapping routes
-------------------

Fine-mapping methods are organized as execution routes. A route is responsible
for assembling study loci, LD inputs, sample sizes, and method-specific
parameters before invoking its fine-mapping tool.
