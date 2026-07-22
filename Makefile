
data-generation:
	@echo "Starting data generation notebooks..."
	(cd data_generation && uv sync && uv run jupyter lab)


run-test:
	@echo "Running pipeline locally..."
	@NXF_LOG_FILE=logs/.nextflow.log nextflow run main.nf -profile test