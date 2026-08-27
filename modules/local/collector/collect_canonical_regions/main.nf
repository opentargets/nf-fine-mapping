nextflow.enable.dsl = 2
nextflow.enable.types = true

process COLLECT_CANONICAL_REGIONS {
    tag "${runId}"

    label "collector"
    label "locus_collection"

    publishDir "${params.output_dir}", mode: 'copy', pattern: 'status/*.jsonl'
    publishDir "${params.output_dir}", mode: 'copy', pattern: 'stats.*', saveAs: { filename -> "collected_loci/stats/${filename}" }

    input:
    tuple(runId: String, metas: List, locus_breaker_paths: List<Path>, ancestries: List<String>, summary_statistics_paths: List<Path>)

    output:
    loci = tuple(runId, metas, file("fine_mapping_locus_sets", type: 'dir'))
    stats = tuple(runId, metas, file("stats.parquet"), file("stats.json"))
    status = file("status/*.jsonl", optional: true)

    topic:
    tuple("${task.process}", "collector", "1.1.0") >> "versions"

    script:
    def args = task.ext.args ?: ''
    def lb_args = locus_breaker_paths.collect { "--locus_breaker ${it}" }.join(' ')
    def ancestry_args = ancestries.collect { "--ancestry '${it}'" }.join(' ')
    def ss_args = summary_statistics_paths.collect { "--summary_statistics ${it}" }.join(' ')
    def logical_path = "collected_loci/fine_mapping_locus_sets/${runId}"
    def safe_logical_path = logical_path.replaceAll(/[^A-Za-z0-9._-]+/, "__")
    def status_filename = "${runId}--${safe_logical_path}.jsonl"
    """
    export DUCKDB_TMPDIR="\$PWD/duckdb_tmp"
    export TMPDIR="\$DUCKDB_TMPDIR"
    mkdir -p "\$DUCKDB_TMPDIR"
    mkdir -p fine_mapping_locus_sets
    mkdir -p status

    collector collect_canonical_regions \\
        --run_id '${runId}' \\
        ${lb_args} \\
        ${ancestry_args} \\
        ${ss_args} \\
        --fine_mapping_locus_set_output_dir fine_mapping_locus_sets \\
        --stats_parquet_output stats.parquet \\
        --stats_json_output stats.json \\
        --canonical_region_min_maf '${params.canonical_region_min_maf}' \\
        --canonical_region_max_region_span_bp ${params.canonical_region_max_region_span_bp} \\
        ${args}

    collector empty_status \\
        --run_id '${runId}' \\
        --path fine_mapping_locus_sets \\
        --logical_path '${logical_path}' \\
        --validation_stage 'LOCUS_COLLECTION' \\
        > status/${status_filename}

    if [[ -s status/${status_filename} ]]; then
        rm -f fine_mapping_locus_sets/*.parquet
    else
        rm -f status/${status_filename}
    fi
    """

    stub:
    def logical_path = "collected_loci/fine_mapping_locus_sets/${runId}"
    def safe_logical_path = logical_path.replaceAll(/[^A-Za-z0-9._-]+/, "__")
    def status_filename = "${runId}--${safe_logical_path}.jsonl"
    def emit_status = task.ext.emit_status ?: params.empty_status_stub_emit ?: false
    """
    mkdir -p fine_mapping_locus_sets
    mkdir -p status
    touch stats.parquet stats.json

    if [[ "${emit_status}" == "true" ]]; then
        printf '%s\\n' '{"runId":"${runId}","path":"fine_mapping_locus_sets","validationStage":"LOCUS_COLLECTION","reason":"EMPTY_DATASET"}' > status/${status_filename}
    else
        touch fine_mapping_locus_sets/set-a.parquet
        touch fine_mapping_locus_sets/set-b.parquet
    fi
    """
}
