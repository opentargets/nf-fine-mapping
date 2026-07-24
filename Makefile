
run-test:
	@echo "Running pipeline locally..."
	@NXF_LOG_FILE=logs/.nextflow.log nextflow run main.nf -resume -profile test
