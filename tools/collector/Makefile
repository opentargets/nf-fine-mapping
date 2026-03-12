test:
	@echo "Running collector tests..."
	(uv sync && uv run pytest)

build:
	@echo "Building collector image..."
	(uv sync && docker build -t collector:latest -f Dockerfile .)

