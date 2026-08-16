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
   Google Cloud execution profiles.

To run a profile directly:

.. code-block:: bash

   nextflow run main.nf -profile testFullCollectorHailingDucks -resume

The ``-resume`` flag reuses successful tasks when their process inputs and
commands have not changed.

Fine-mapping containers
-----------------------

The fine-mapping workflow supports MultiSuSiE, SuSiEx, and SuShiE containers.
Select methods in configuration and set their images explicitly for local or
production execution:

.. code-block:: groovy

   params {
       fine_mapping_methods = ['multisusie', 'susiex', 'sushie']
       multisusie_container = 'multisusie:local'
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
