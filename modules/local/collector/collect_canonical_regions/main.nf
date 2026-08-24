nextflow.enable.dsl = 2
nextflow.enable.types = true

process COLLECT_CANONICAL_REGIONS {
    tag "${runId}"

    label "collector"
    label "locus_collection"

    input:
    tuple(runId: String, metas: List, locus_breaker_paths: List<Path>, ancestries: List<String>, summary_statistics_paths: List<Path>)

    output:
    loci = tuple(runId, metas, file("fine_mapping_locus_sets", type: 'dir'))
    stats = tuple(runId, metas, file("stats.parquet"), file("stats.json"))

    topic:
    tuple("${task.process}", "collector", "1.1.0") >> "versions"

    script:
    def args = task.ext.args ?: ''
    def lb_args = locus_breaker_paths.collect { "--locus_breaker ${it}" }.join(' ')
    def ancestry_args = ancestries.collect { "--ancestry '${it}'" }.join(' ')
    def ss_args = summary_statistics_paths.collect { "--summary_statistics ${it}" }.join(' ')
    """
    export DUCKDB_TMPDIR="\$PWD/duckdb_tmp"
    export TMPDIR="\$DUCKDB_TMPDIR"
    mkdir -p "\$DUCKDB_TMPDIR"
    mkdir -p fine_mapping_locus_sets

    collector collect_canonical_regions \\
        --run_id '${runId}' \\
        ${lb_args} \\
        ${ancestry_args} \\
        ${ss_args} \\
        --fine_mapping_locus_set_output_dir fine_mapping_locus_sets \\
        --stats_parquet_output stats.parquet \\
        --stats_json_output stats.json \\
        ${args}
    """

    stub:
    """
    mkdir -p fine_mapping_locus_sets
    touch fine_mapping_locus_sets/set-a.parquet
    touch fine_mapping_locus_sets/set-b.parquet
    touch stats.parquet stats.json
    """
}
