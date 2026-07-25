
.PHONY: help \
	run-test run-test-gentropy-local run-test-full \
	integration-test integration-test-gentropy-local integration-test-full integration-test-all \
	unit-test unit-test-pipeline unit-test-workflows unit-test-all \
	collector-dev collector-lint collector-test collector-check docs docs-clean collector-docs collector-docs-clean collector-build

NEXTFLOW ?= nextflow
NF_TEST ?= nf-test

help: ## Show available development and test targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-32s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

integration-test: ## Run the collector Nextflow integration test profile
	@echo "Running pipeline locally..."
	@NXF_LOG_FILE=logs/.nextflow.log $(NEXTFLOW) run main.nf -resume -profile test

integration-test-gentropy-local: ## Run the Gentropy Nextflow integration test profile
	@echo "Running chr1 Gentropy pipeline locally..."
	@NXF_LOG_FILE=logs/.nextflow.gentropy-local.log $(NEXTFLOW) run main.nf -resume -profile testGentropyLocal

integration-test-full: ## Run the full-data Nextflow integration test profile
	@echo "Running full-data pipeline locally..."
	@NXF_LOG_FILE=logs/.nextflow.full.log $(NEXTFLOW) run main.nf -resume -profile fullTest

integration-test-all: ## Run all Nextflow integration test profiles
integration-test-all: integration-test integration-test-gentropy-local integration-test-full

run-test: integration-test ## Backward-compatible alias for integration-test

run-test-gentropy-local: integration-test-gentropy-local ## Backward-compatible alias for Gentropy integration test

run-test-full: integration-test-full ## Backward-compatible alias for full-data integration test

unit-test: ## Run all nf-test unit and workflow tests
	$(NF_TEST) test

unit-test-pipeline: ## Run the top-level pipeline nf-test suite
	$(NF_TEST) test tests/default.nf.test

unit-test-workflows: ## Run the workflow-level nf-test suites
	$(NF_TEST) test tests/workflows/locus_breaker.nf.test tests/workflows/locus_collection.nf.test

unit-test-all: ## Run all nf-test suites
unit-test-all: unit-test

collector-dev:
	$(MAKE) -C tools/collector dev

collector-lint:
	$(MAKE) -C tools/collector lint

collector-test:
	$(MAKE) -C tools/collector test

collector-check: collector-lint collector-test

docs: ## Build the full pipeline documentation
	$(MAKE) -C tools/collector docs

docs-clean: ## Remove generated pipeline documentation
	$(MAKE) -C tools/collector docs-clean

collector-docs:
	$(MAKE) docs

collector-docs-clean:
	$(MAKE) docs-clean

collector-build:
	$(MAKE) -C tools/collector build
