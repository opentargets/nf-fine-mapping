nextflow.enable.dsl = 2
nextflow.enable.types = true


process COLLECTOR_CHECK_LD_PAIR_STATS {
    tag "${runId}"

    label "collector"
    label "status"

    input:
    tuple(runId: String, stats_path: Path)

    output:
    file("status/*.jsonl"), optional: true

    topic:
    tuple("${task.process}", "collector", "1.0.0") >> "versions"

    script:
    """
    mkdir -p status

    collector check_ld_pair_stats \\
        --run_id ${runId} \\
        --path ${stats_path} \\
        > status/${runId}.jsonl

    if [[ ! -s status/${runId}.jsonl ]]; then
        rm status/${runId}.jsonl
    fi
    """

    stub:
    def emit_status = params.ld_pair_stats_stub_emit ?: false
    """
    mkdir -p status

    if [[ "${emit_status}" == "true" ]]; then
        printf '%s\\n' '{"runId":"${runId}","path":"${stats_path}","validationStage":"LD_ANNOTATION","reason":"EMPTY_LD_PAIRS"}' > status/${runId}.jsonl
    fi
    """
}
