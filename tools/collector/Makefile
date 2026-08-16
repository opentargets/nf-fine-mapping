.PHONY: help dev lint test hailing-s3-smoke docs docs-clean build clean

VERSION ?= 1.1.0
REGISTRY ?= ghcr.io/opentargets
HAILING_DUCKS_IMAGE ?= collector:hailing-ducks-dev
HAILING_DUCKS_S3_HT_PATH ?= s3://pan-ukb-us-east-1/ld_release/UKBB.EUR.ldadj.variant.b38.ht
HAILING_DUCKS_S3_BM_PATH ?= s3://pan-ukb-us-east-1/ld_release/UKBB.EUR.ldadj.bm

help: ## Show available development targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-9s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

dev: ## Install development dependencies
	uv sync --all-groups

lint: ## Run Ruff and ty
	uv run --frozen ruff check .
	uv run --frozen ruff format --check .
	uv run --frozen ty check src

test: ## Run the test suite
	uv run --frozen pytest

hailing-s3-smoke: ## Run the opt-in real Pan-UKBB Hailing Ducks smoke test
	docker run --rm --entrypoint '' \
		$(HAILING_DUCKS_IMAGE) \
		python /app/scripts/smoke_hailing_ducks_s3.py \
		--ht-path $(HAILING_DUCKS_S3_HT_PATH) \
		--bm-path $(HAILING_DUCKS_S3_BM_PATH)

docs: ## Build HTML documentation with Sphinx
	uv run --frozen sphinx-build -M html ../../docs ../../docs/_build

docs-clean: ## Remove generated Sphinx documentation
	rm -rf ../../docs/_build

build: ## Build the collector container image
	docker build -t $(REGISTRY)/collector:$(VERSION) -f Dockerfile .

clean: ## Remove local development artifacts
	 rm -rf .venv .pytest_cache .ruff_cache *.egg-info .coverage coverage.xml
