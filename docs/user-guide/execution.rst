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

MultiSuSiE container
--------------------

The locus-annotation workflow expects a container exposing the ``multisusie``
command. Set it explicitly for local or production execution:

.. code-block:: bash

   nextflow run main.nf \
     -profile test \
     --multisusie_container multisusie:local \
     -resume

Build and smoke-test the local image from the MultiSuSiE repository before
running the non-stub pipeline route:

.. code-block:: bash

   docker build --tag multisusie:local /path/to/MultiSuSiE
   docker run --rm multisusie:local --help
   make integration-test

The nf-test suite uses process stubs and therefore does not require Docker or
the MultiSuSiE image. CI separately runs Nextflow lint and the complete
nf-test suite on pull requests.
