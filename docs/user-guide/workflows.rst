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

``LOCUS_COLLECTION`` groups locus outputs by ``runId`` and runs one canonical
multi-ancestry region collection task per run. It emits the collected locus
sets alongside a JSON/Parquet statistics channel. The collected locus sets
are the sole candidate sets passed to later fine-mapping stages.

Locus annotation
----------------

``LOCUS_ANNOTATION`` runs LD annotation once per
``fineMappingLocusSetId``. It emits the locus-set Parquet file, the
MultiAncestryPairwiseLD dataset, and the study metadata carried by the channel.

The LD backend is selected with ``params.ld_annotation_method``:

* ``gentropy`` uses the Spark/Hail implementation and each registry entry's
  ``vi_path`` and ``bm_path``;
* ``hailing_ducks`` uses the collector image with Hailing Ducks v1.1.0 and
  each registry entry's native hg38 ``ht_path`` and ``bm_path``.

Both backends emit the same flat ``MultiAncestryPairwiseLD`` contract. The
Hailing Ducks adapter converts pipeline variant IDs such as ``1_100_A_AT`` to
the native ``chr1_100_A_AT`` lookup convention, maps them back on output, and
uses the study metadata to query each study-locus only against its registered
ancestry.

Fine-mapping
------------

``FINE_MAPPING`` consumes valid records from ``LOCUS_ANNOTATION`` after empty
LD results have been filtered. It can run any combination of:

* ``multisusie``;
* ``susiex``;
* ``sushie``.

Select methods with ``params.fine_mapping_methods``. The default is
``['multisusie']`` so existing runs do not unexpectedly multiply their compute
cost. To compare all methods:

.. code-block:: groovy

   params.fine_mapping_methods = ['multisusie', 'susiex', 'sushie']

Each method runs once per ``fineMappingLocusSetId`` with the same annotated
locus, pairwise LD, and study metadata. Metadata is serialized as JSONL inside
each task, so no separate metadata process is required. Every method emits a
metadata-preserving record containing:

* ``study_locus_path`` — the Gentropy-compatible StudyLocus Parquet output;
* ``extended_results_path`` — the AnnData H5AD output with component-level
  posterior results and provenance;
* ``stats_path`` — the JSON status record for the locus-set fit;
* ``runId``, ``fine_mapping_locus_set_id``, and the input ``metas``.

Images are configured independently with ``params.multisusie_container``,
``params.susiex_container``, and ``params.sushie_container``. Their local
defaults are ``multisusie:latest``, ``susiex:latest``, and ``sushie:latest``.
Method options use the process ``task.ext.args`` interface. MultiSuSiE purity
and low-memory settings are managed by the pipeline and must not be overridden
through ``task.ext.args``. For example:

.. code-block:: text

   withName: MULTISUSIE_FINE_MAPPING {
       ext.args = '--L 10 --max-iter 100'
   }

   withName: SUSIEX_FINE_MAPPING {
       ext.args = '--n-sig 10 --max-iter 200'
   }

   withName: SUSHIE_FINE_MAPPING {
       ext.args = '--L 10 --rho 0.1 --max-iter 500'
   }

Non-converged or otherwise non-reportable fits write ``stats.json`` and omit
the result files according to each method's application contract. Hard input
or output errors still fail the corresponding process.
