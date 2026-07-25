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
