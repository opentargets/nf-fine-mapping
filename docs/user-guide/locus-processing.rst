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

The collection step unions the study-locus inputs, joins loci by genomic-range
overlap, and classifies each locus according to the number of distinct studies
it overlaps. Full candidate-set generation is performed in DuckDB and supports
an arbitrary number of input studies.

The command writes three Parquet result sets and one JSON statistics document.
The full-overlap output contains deterministic ``fineMappingLocusSetId`` values
and is the input for downstream fine-mapping.
