Fine-mapping routes
===================

Available methods
-----------------

The ``FINE_MAPPING`` workflow contains three Open Targets method forks, but
only MultiSuSiE is currently fully integrated and supported:

* MultiSuSiE retains the union of variants across ancestries and is the
  supported default;
* SuSiEx runs the native C++ cross-ancestry model through its Python adapter;
  full pipeline integration is pending;
* SuShiE restricts inference to variants shared across every ancestry with
  complete LD; full pipeline integration is pending.

The method modules are being aligned to a shared per-locus Parquet,
MultiAncestryPairwiseLD, and JSONL metadata contract. MultiSuSiE publishes the
supported Gentropy-compatible StudyLocus Parquet file, extended AnnData
result, and machine-readable statistics. SuSiEx and SuShiE remain pending
integration work.

The workflow consumes the canonical multi-ancestry locus sets emitted by
``LOCUS_COLLECTION`` that pass LD annotation validation.

LD interface
------------

LD preparation is kept separate from locus processing. This allows the same
candidate loci to be evaluated with different ancestry panels and is intended
to let all three method modules share the same upstream data preparation once
SuSiEx and SuShiE integration is complete.
