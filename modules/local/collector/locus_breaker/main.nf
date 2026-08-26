nextflow.enable.dsl = 2
nextflow.enable.types = true


process COLLECTOR_LOCUS_BREAKER {
    tag "${meta.route}"
    tag "${meta.traitSet}"
    tag "${meta.ancestry}"
    tag "${meta.runId}"
    tag "${meta.studyId}"

    label "collector"
    label "locus_breaker"

    publishDir "${params.output_dir}", mode: 'copy', pattern: 'locus_breaker_clumped_study_locus/*.parquet'
    publishDir "${params.output_dir}", mode: 'copy', pattern: 'status/*.jsonl'

    input:
    tuple(meta: Map, summary_statistics_path: Path)

    output:
    record(
        meta: meta,
        summary_statistics_path: summary_statistics_path,
        study_locus_path: file("locus_breaker_clumped_study_locus/*.parquet", optional: true),
        status_path: file("status/*.jsonl", optional: true),
    )

    topic:
    tuple("${task.process}", "collector", "1.1.0") >> "versions"

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: meta.studyId
    def logical_path = "locus_breaker_clumped_study_locus/${meta.studyId}.parquet"
    def safe_logical_path = logical_path.replaceAll(/[^A-Za-z0-9._-]+/, "__")
    def status_filename = "${meta.runId}--${safe_logical_path}.jsonl"
    """
    mkdir -p locus_breaker_clumped_study_locus
    mkdir -p status

    collector locus_breaker \\
        --input ${summary_statistics_path} \\
        --output locus_breaker_clumped_study_locus/${prefix}.parquet \\
        --lbc_baseline_pvalue 1.0e-05 \\
        --lbc_distance_cutoff 250000 \\
        --lbc_pvalue_threshold 1.0e-08 \\
        --lbc_flanking_distance 100000 \\
        --large_loci_size ${params.locus_breaker_large_loci_size} \\
        --wbc_clump_distance 500000 \\
        --wbc_pvalue_threshold 1.0e-05 \\
        --collect_locus \\
        --remove_mhc \\
        ${args}

    collector empty_status \\
        --run_id '${meta.runId}' \\
        --path locus_breaker_clumped_study_locus/${prefix}.parquet \\
        --logical_path '${logical_path}' \\
        --validation_stage 'LOCUS_BREAKER' \\
        > status/${status_filename}

    if [[ -s status/${status_filename} ]]; then
        rm -f locus_breaker_clumped_study_locus/${prefix}.parquet
    else
        rm -f status/${status_filename}
    fi
    """

    stub:
    def prefix = task.ext.prefix ?: meta.studyId
    """
    mkdir -p locus_breaker_clumped_study_locus
    mkdir -p status
    touch locus_breaker_clumped_study_locus/${prefix}.parquet
    """
}
