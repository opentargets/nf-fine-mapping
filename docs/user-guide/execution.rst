Execution
=========

Local execution
---------------

The repository Makefile provides the main development and test entry points:

.. code-block:: bash

   make help
   make integration-test
   make integration-test-gentropy-local
   make unit-test-all
   make integration-test-full

The default local profile uses the collector locus-breaker implementation.
The ``testGentropyLocal`` profile runs the Gentropy implementation against the
same chr1 test data for comparison.

Nextflow profiles
-----------------

The principal profiles are:

``test``
   Local chr1 test data using the collector implementation.
``testGentropyLocal``
   Local chr1 test data using Gentropy locus breaking.
``fullTest``
   Full summary-statistics data for performance and parity checks.
``googleCloud`` / ``googleCloudTest``
   Google Cloud execution profiles.

To run a profile directly:

.. code-block:: bash

   nextflow run main.nf -profile test -resume

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

Select the native DuckDB backend with:

.. code-block:: bash

   nextflow run main.nf \
     -profile test \
     --ld_annotation_method hailing_ducks \
     --collector_container collector:hailing-ducks-dev \
     -resume

The collector image pins ``ghcr.io/project-defiant/hailing-ducks:v1.1.0`` as
its native DuckDB base. The ordinary test suite remains network-independent.
An explicit smoke target checks the public Pan-UKBB hg38 HT, BlockMatrix,
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
