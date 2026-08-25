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

The collection step unions the study-locus inputs and performs a bounded,
inclusive sweep across all input studies to assemble canonical
multi-ancestry locus sets, rather than classifying loci by how many studies
they overlap. Candidate-set generation is performed in DuckDB and supports
an arbitrary number of input studies.

The command writes a directory of per-locus-set Parquet files plus a Parquet
and JSON statistics document. The locus-set output contains deterministic
``fineMappingLocusSetId`` values and is the input for downstream LD
annotation and fine-mapping.
