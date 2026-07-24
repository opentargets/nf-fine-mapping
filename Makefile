
run-test:
	@echo "Running pipeline locally..."
	@NXF_LOG_FILE=logs/.nextflow.log nextflow run main.nf -resume -profile test

run-test-gentropy-local:
	@echo "Running chr1 Gentropy pipeline locally..."
	@NXF_LOG_FILE=logs/.nextflow.gentropy-local.log nextflow run main.nf -resume -profile testGentropyLocal

run-test-full:
	@echo "Running full-data pipeline locally..."
	@NXF_LOG_FILE=logs/.nextflow.full.log nextflow run main.nf -resume -profile fullTest
