Collector API
=============

The collector package provides the DuckDB-backed implementation used by the
default Nextflow locus-breaker and locus-collection workflows.

Command surface
---------------

.. automodule:: collector
   :members: app

Locus breaker
-------------

.. automodule:: collector.locus_breaker
   :members: LocusBreakerConfig, run_locus_breaker, split_pvalue

Canonical regions
-----------------

.. automodule:: collector.canonical_regions
   :members: CollectCanonicalRegionsConfig, run_collect_canonical_regions

Schema contracts
----------------

.. automodule:: collector.schema
   :members: DatasetSchema, DatasetField, ListSchema, StructSchema
