nextflow.enable.dsl = 2
nextflow.enable.types = true


process COLLECTOR_CHECK_LD_PAIR_STATS {
    tag "${runId}:${fine_mapping_locus_set_id}"

    label "collector"
    label "status"

    publishDir "${params.output_dir}", mode: 'copy', pattern: 'status/*.jsonl'

    input:
    tuple(runId: String, fine_mapping_locus_set_id: String, stats_path: Path)

    output:
    file(("status/*.jsonl"), optional: true)

    topic:
    tuple("${task.process}", "collector", "1.1.0") >> "versions"

    script:
    """
    mkdir -p status

    collector check_ld_pair_stats \\
        --run_id ${runId} \\
        --fine_mapping_locus_set_id ${fine_mapping_locus_set_id} \\
        --path ${stats_path} \\
        > status/${runId}--${fine_mapping_locus_set_id}.jsonl

    if [[ ! -s status/${runId}--${fine_mapping_locus_set_id}.jsonl ]]; then
        rm status/${runId}--${fine_mapping_locus_set_id}.jsonl
    fi
    """

    stub:
    def emit_status = params.ld_pair_stats_stub_emit ?: false
    def invalid_locus_set_ids = (params.ld_pair_stats_stub_empty_locus_set_ids ?: []).collect { id -> id.toString() }
    def emit_for_locus = emit_status && (invalid_locus_set_ids.isEmpty() || invalid_locus_set_ids.contains(fine_mapping_locus_set_id))
    """
    mkdir -p status

    if [[ "${emit_for_locus}" == "true" ]]; then
        printf '%s\\n' '{"runId":"${runId}","fineMappingLocusSetId":"${fine_mapping_locus_set_id}","path":"${stats_path}","validationStage":"LD_ANNOTATION","reason":"EMPTY_LD_PAIRS"}' > status/${runId}--${fine_mapping_locus_set_id}.jsonl
    fi
    """
}
