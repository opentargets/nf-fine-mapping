nextflow.enable.dsl = 2
nextflow.enable.types = true


process COLLECTOR_EMPTY_STATUS {
    tag "${runId}"
    tag "${logical_path}"

    label "collector"
    label "status"
    publishDir "${params.output_dir}", mode: 'copy', pattern: 'status/*.jsonl'

    input:
    tuple(runId: String, logical_path: String, validation_stage: String, dataset_path: Path)

    output:
    file(("status/*.jsonl"), optional: true)

    topic:
    tuple("${task.process}", "collector", "1.1.0") >> "versions"

    script:
    def safe_logical_path = logical_path.replaceAll(/[^A-Za-z0-9._-]+/, "__")
    def status_filename = "${runId}--${safe_logical_path}.jsonl"
    """
    mkdir -p status

    collector empty_status \
        --run_id ${runId} \
        --path ${dataset_path} \
        --logical_path ${logical_path} \
        --validation_stage ${validation_stage} \
        > status/${status_filename}

    if [[ ! -s status/${status_filename} ]]; then
        rm status/${status_filename}
    fi
    """

    stub:
    def safe_logical_path = logical_path.replaceAll(/[^A-Za-z0-9._-]+/, "__")
    def status_filename = "${runId}--${safe_logical_path}.jsonl"
    def emit_status = task.ext.emit_status ?: params.empty_status_stub_emit ?: false
    """
    mkdir -p status

    if [[ "${emit_status}" == "true" ]]; then
        printf '%s\n' '{"runId":"${runId}","path":"${logical_path}","validationStage":"${validation_stage}","reason":"EMPTY_DATASET"}' > status/${status_filename}
    fi
    """
}
