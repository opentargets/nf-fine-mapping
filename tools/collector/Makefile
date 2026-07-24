VERSION = 1.0.0
REGISTRY = ghcr.io/opentargets


test:
	@echo "Running collector tests..."
	(uv sync && uv run pytest)

build:
	@echo "Building collector image..."
	(uv sync && docker build -t $(REGISTRY)/collector:$(VERSION) -f Dockerfile .)

