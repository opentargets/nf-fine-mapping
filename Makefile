
data-generation:
	@echo "Starting data generation notebooks..."
	(cd data_generation && uv sync && uv run jupyter lab)


