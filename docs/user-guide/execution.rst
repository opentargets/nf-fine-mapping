Execution
=========

Local execution
---------------

The repository Makefile provides the main development and test entry points:

.. code-block:: bash

   make help
   make integration-test
   make unit-test-all

The canonical local integration entry point runs the full-data Collector +
Hailing Ducks profile.

Nextflow profiles
-----------------

The principal profiles are:

``testFullCollectorHailingDucks``
   Full summary-statistics test data using the collector locus breaker and
   Hailing Ducks LD annotation.
``googleCloud`` / ``googleCloudTest``
   Google Cloud Batch execution profiles using Collector and Hailing Ducks.

To run a profile directly:

.. code-block:: bash

   nextflow run main.nf -profile testFullCollectorHailingDucks -resume

Gentropy alternatives
---------------------

The pipeline still contains Gentropy-based locus-breaking and LD-annotation
modules for comparison and specialized runs. They are not selected by any
shipped profile and are discouraged for routine execution because they are
slower than the Collector and Hailing Ducks path.

To opt into a Gentropy module, override the relevant method and provide the
required Gentropy container and Spark settings in a separate user config. For
example:

.. code-block:: bash

   nextflow run main.nf -c gentropy.local.config \
     --locus_breaker_method gentropy \
     --ld_annotation_method gentropy

The user config must provide the Gentropy process container and, for LD
annotation, the appropriate ``gentropy_spark_uri`` and Spark configuration.

The ``-resume`` flag reuses successful tasks when their process inputs and
commands have not changed.

Fine-mapping containers
-----------------------

The fine-mapping workflow currently supports MultiSuSiE. SuSiEx and SuShiE
containers are pinned for pending integration work and should not be selected
for production runs yet. The schema defaults use pinned images. Override them
only when running locally built development images, for example:

.. code-block:: groovy

   params {
       fine_mapping_methods = ['multisusie']
       multisusie_container = 'multisusie:local'
       multisusie_purity_min_r2 = 0.01
       susiex_container = 'susiex:local'
       sushie_container = 'sushie:local'
   }

Build and smoke-test the local images before running the non-stub pipeline:

.. code-block:: bash

   docker build --tag multisusie:local /path/to/MultiSuSiE
   docker build --tag susiex:local /path/to/susiex
   docker build --tag sushie:local /path/to/sushie
   docker run --rm multisusie:local --help
   docker run --rm susiex:local --help
   docker run --rm sushie:local --help
   make integration-test

The nf-test suite uses process stubs and therefore does not require Docker or
the method images. CI separately runs Nextflow lint and the complete nf-test
suite on pull requests.

``multisusie_purity_min_r2`` must be strictly between 0 and 1. It defaults to
0.01. MultiSuSiE publishes only credible sets at or above this R-squared
threshold; the process always runs with low-memory mode disabled so purity is
available for filtering.

Hailing Ducks LD validation
---------------------------

The canonical local integration profile already selects the native DuckDB
backend:

.. code-block:: bash

   make integration-test

To run the same profile directly:

.. code-block:: bash

   nextflow run main.nf -profile testFullCollectorHailingDucks -resume

The integration target builds and runs the local ``collector:1.1.0`` image for
collector-labelled processes and keeps the ordinary stub test suite
network-independent. Pairwise LD files remain transient work artifacts for
fine-mapping and are not published; only the compact LD statistics are retained.
An explicit smoke target still checks the public Pan-UKBB hg38 HT, BlockMatrix,
indel resolution, and signed LD extraction:

.. code-block:: bash

   make -C tools/collector build REGISTRY=local VERSION=hailing-ducks-dev
   make -C tools/collector hailing-s3-smoke \
     HAILING_DUCKS_IMAGE=local/collector:hailing-ducks-dev

To compare a Hailing Ducks result with a Gentropy result independent of row
order and ``chr`` prefixes:

.. code-block:: bash

   collector ld_parity \
     --hailing hailing.parquet \
     --gentropy gentropy_ld_dataset \
     --hailing_stats hailing.stats.jsonl \
     --gentropy_stats gentropy.stats.jsonl \
     --report parity.json
