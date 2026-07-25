Fine-mapping routes
===================

MultiSuSiE
----------

MultiSuSiE is the first planned multi-study fine-mapping route. It combines
candidate study-locus sets with ancestry-specific LD matrices and summary
statistics before running the method-specific model.

The route is designed to consume only fully overlapping locus sets from
``LOCUS_COLLECTION``. Partial and non-overlap outputs remain available for
quality-control analysis and future workflows.

LD interface
------------

LD preparation is kept separate from locus processing. This allows the same
candidate loci to be evaluated with different ancestry panels and keeps route
modules independent from the upstream summary-statistics clumping backend.
