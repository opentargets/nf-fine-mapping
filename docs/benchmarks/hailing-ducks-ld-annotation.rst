Hailing Ducks LD annotation benchmark
=====================================

Decision
--------

Hailing Ducks remains an opt-in LD annotation backend. It is roughly twice as
fast as Gentropy and uses less memory on the measured loci, but Hailing Ducks
v1.1.0 intentionally rejects multiple distinct requested variants at the same
genomic position. The large fixture therefore has a small pair-coverage gap.
Gentropy remains the default until that gap is resolved.

The promotion gates are:

* zero shared-value mismatches at absolute tolerance ``1e-8``;
* zero backend-only pairs in both directions on the large regression locus;
* successful SNP, indel, allele-flip, native hg38 HT, and BM smoke checks;
* peak task memory below the configured 16 GiB local-test allocation; and
* wall time no worse than Gentropy on representative small and large loci.

Environment and method
----------------------

Measurements were taken on 29 July 2026 against the public Pan-UKBB EUR, CSA,
and AFR resources in ``us-east-1``. Gentropy used the existing Nextflow task
traces. Hailing Ducks used the pinned
``ghcr.io/project-defiant/hailing-ducks:v1.1.0`` native CLI inside the
collector image.

Hailing timings separate the extension call—which combines remote HT/BM query
and native LD/status Parquet materialisation—from collector adaptation and the
final combined output write. Hailing Ducks v1.1.0 does not expose network
lookup and native output-write timings separately.

Results
-------

.. list-table::
   :header-rows: 1

   * - Metric
     - Small chr1 locus
     - Large chr1 locus
   * - Input locus variants
     - 4,951
     - 19,432
   * - Hailing output pairs
     - 1,058,236
     - 24,249,143
   * - Gentropy output pairs
     - 1,058,236
     - 24,279,447
   * - Shared-value mismatches
     - 0
     - 0
   * - Maximum absolute shared difference
     - 0.0
     - 0.0
   * - Hailing wall time
     - 1m50s
     - 7m48s
   * - Gentropy wall time
     - 3m22s
     - 15m15s–16m53s
   * - Hailing native query/materialise
     - 1m35s
     - 7m08s
   * - Hailing collector adaptation
     - 0.93s
     - 22.09s
   * - Hailing final output write
     - 0.18s
     - 4.94s
   * - Hailing peak RSS
     - 0.71 GiB
     - 7.84 GiB
   * - Gentropy peak RSS
     - 10.47 GiB
     - 13.41–13.71 GiB
   * - Hailing output size
     - 8.57 MiB
     - 214.24 MiB
   * - Gentropy output size
     - 18.02 MiB
     - 432.82 MiB

Correctness findings
--------------------

The small locus has exact row and value parity across AFR, EAS, and NFE.

For the large locus, every one of the 24,249,143 Hailing pairs exists in the
Gentropy output with exactly the same LD value. Gentropy has 30,304 additional
pairs, including seven additional diagonals. Those seven variants occur at
positions with two distinct requested variant IDs in the same study-locus.
Hailing Ducks reports this request shape as
``multiple_variants_at_position``; it is not a rounding, contig, indel, or
allele-sign error. The missing share is approximately 0.125% of the Gentropy
pair set.

Operational safeguards
----------------------

* ``params.ld_annotation_method`` defaults to ``gentropy``.
* Hailing Ducks uses ancestry-specific requests derived from study metadata.
* ``params.hailing_ducks_max_cached_blocks`` defaults to 8.
* Adapted ancestry outputs are written to temporary Parquet files before the
  final union, avoiding an in-memory cross-ancestry accumulator.
* The process continues to run per ``fineMappingLocusSetId``.
* Zero-pair ancestry statistics feed the existing locus-level invalidation.
* ``collector ld_parity`` fails when shared LD values exceed the configured
  tolerance and reports backend-only and diagonal differences explicitly.
* ``make -C tools/collector hailing-s3-smoke`` is opt-in and network-dependent;
  ordinary unit and nf-test suites remain offline.
