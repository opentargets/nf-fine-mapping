
.PHONY: help \
	dev \
	reference-data-filters reference-data-variant-index reference-data-download-panukbb-tables reference-data-panukbb-indexes reference-data-panukbb-chr1 reference-data-all-indexes \
	run-test run-test-gentropy-local run-test-full \
	integration-test integration-test-gentropy-local integration-test-full integration-test-all \
	unit-test unit-test-pipeline unit-test-workflows unit-test-all \
	collector-dev collector-lint collector-test collector-check docs docs-clean collector-docs collector-docs-clean collector-build

NEXTFLOW ?= nextflow
NF_TEST ?= nf-test
JAVA_MIN_VERSION ?= 17

help: ## Show available development and test targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z0-9_-]+:.*?## / {printf "\033[36m%-32s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

dev: ## Install and validate the local pipeline development toolchain
	@set -eu; \
	local_bin="$${XDG_BIN_HOME:-$$HOME/.local/bin}"; \
	export PATH="$$local_bin:$$PATH"; \
	if ! command -v java >/dev/null 2>&1; then \
		echo "Java is required by Nextflow but was not found on PATH." >&2; \
		exit 1; \
	fi; \
	echo "Java version:"; \
	java_version="$$(java -version 2>&1 | sed -n '1p')"; \
	java_major="$$(printf '%s\n' "$$java_version" | sed -n 's/.*version "\([0-9][0-9]*\).*/\1/p')"; \
	printf '%s\n' "$$java_version"; \
	if [ -z "$$java_major" ] || [ "$$java_major" -lt "$(JAVA_MIN_VERSION)" ]; then \
		echo "Java $(JAVA_MIN_VERSION)+ is required by Nextflow; detected: $${java_major:-unknown}." >&2; \
		exit 1; \
	fi; \
	if ! command -v $(NEXTFLOW) >/dev/null 2>&1; then \
		echo "Installing Nextflow..."; \
		mkdir -p "$$local_bin"; \
		tmp_dir="$$(mktemp -d)"; \
		trap 'rm -rf "$$tmp_dir"' EXIT; \
		curl -fsSL https://get.nextflow.io -o "$$tmp_dir/nextflow"; \
		install -m 0755 "$$tmp_dir/nextflow" "$$local_bin/nextflow"; \
		echo "Installed Nextflow at $$local_bin/nextflow"; \
	fi; \
	if ! command -v $(NF_TEST) >/dev/null 2>&1; then \
		echo "Installing nf-test..."; \
		mkdir -p "$$local_bin"; \
		tmp_dir="$$(mktemp -d)"; \
		trap 'rm -rf "$$tmp_dir"' EXIT; \
		( cd "$$tmp_dir" && curl -fsSL https://get.nf-test.com | bash ); \
		install -m 0755 "$$tmp_dir/nf-test" "$$local_bin/nf-test"; \
		echo "Installed nf-test at $$local_bin/nf-test"; \
	fi; \
	echo "Nextflow: $$(command -v $(NEXTFLOW))"; \
	echo "nf-test: $$(command -v $(NF_TEST))"; \
	$(MAKE) -C tools/collector dev; \
	if ! command -v prek >/dev/null 2>&1; then \
		echo "Installing prek..."; \
		uv tool install prek; \
	fi; \
	echo "prek: $$(command -v prek || echo 'installed by uv; restart the shell if it is not on PATH')"

reference-data-filters: ## Build PanUKBB chr1 and full-test variant filter parquet files
	scripts/reference_data/build_panukbb_variant_filters.sh

reference-data-variant-index: reference-data-filters ## Build minimal local VariantIndex from exact StudyLocus variants
	scripts/reference_data/build_study_locus_variant_index.sh

reference-data-download-panukbb-tables: ## Download PanUKBB .variant.b38.ht Hail Tables from public S3
	scripts/reference_data/download_panukbb_variant_tables.sh

reference-data-panukbb-indexes: reference-data-filters reference-data-variant-index ## Prepare local PanUKBB filtered LD indexes via Gentropy
	FILTER_SCOPE=chr1,full_test scripts/reference_data/prepare_panukbb_ld_reference.sh

reference-data-panukbb-chr1: reference-data-filters reference-data-variant-index ## Prepare local PanUKBB chr1 filtered LD index via Gentropy
	FILTER_SCOPE=chr1 scripts/reference_data/prepare_panukbb_ld_reference.sh

reference-data-all-indexes: reference-data-filters reference-data-variant-index reference-data-download-panukbb-tables reference-data-panukbb-indexes ## Build all local PanUKBB LD-index reference data

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
	$(NF_TEST) test tests/workflows/locus_breaker.nf.test tests/workflows/locus_collection.nf.test tests/workflows/locus_annotation.nf.test

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
