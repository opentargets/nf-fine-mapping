.PHONY: help dev lint test docs docs-clean build clean

VERSION ?= 1.0.0
REGISTRY ?= ghcr.io/opentargets

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

docs: ## Build HTML documentation with Sphinx
	uv run --frozen sphinx-build -M html ../../docs ../../docs/_build

docs-clean: ## Remove generated Sphinx documentation
	rm -rf ../../docs/_build

build: ## Build the collector container image
	docker build -t $(REGISTRY)/collector:$(VERSION) -f Dockerfile .

clean: ## Remove local development artifacts
	 rm -rf .venv .pytest_cache .ruff_cache *.egg-info .coverage coverage.xml
