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

Fine-mapping routes
-------------------

Fine-mapping methods are organized as execution routes. A route is responsible
for assembling study loci, LD inputs, sample sizes, and method-specific
parameters before invoking its fine-mapping tool.
