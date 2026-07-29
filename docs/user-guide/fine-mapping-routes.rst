Fine-mapping routes
===================

Available methods
-----------------

The ``FINE_MAPPING`` workflow currently supports three Open Targets method
forks:

* MultiSuSiE retains the union of variants across ancestries;
* SuSiEx runs the native C++ cross-ancestry model through its Python adapter;
* SuShiE restricts inference to variants shared across every ancestry with
  complete LD.

All methods consume the same per-locus Parquet, MultiAncestryPairwiseLD, and
JSONL metadata contract. They each publish a Gentropy-compatible StudyLocus
Parquet file, an extended AnnData result, and machine-readable statistics.

The workflow consumes only fully overlapping locus sets from
``LOCUS_COLLECTION`` that pass LD annotation validation. Partial and
non-overlap outputs remain available for quality-control analysis.

LD interface
------------

LD preparation is kept separate from locus processing. This allows the same
candidate loci to be evaluated with different ancestry panels and lets all
three method modules share the same upstream data preparation.
