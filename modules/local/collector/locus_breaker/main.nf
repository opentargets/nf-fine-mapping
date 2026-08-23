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

    input:
    tuple(meta: Map, summary_statistics_path: Path)

    output:
    record(
        meta: meta,
        summary_statistics_path: summary_statistics_path,
        study_locus_path: file("locus_breaker_clumped_study_locus/*.parquet"),
    )

    topic:
    tuple("${task.process}", "collector", "1.1.0") >> "versions"

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: meta.studyId
    """
    mkdir -p locus_breaker_clumped_study_locus

    collector locus_breaker \
        --input ${summary_statistics_path} \
        --output locus_breaker_clumped_study_locus/${prefix}.parquet \
        --lbc_baseline_pvalue 1.0e-05 \
        --lbc_distance_cutoff 250000 \
        --lbc_pvalue_threshold 1.0e-08 \
        --lbc_flanking_distance 100000 \
        --large_loci_size 1500000 \
        --wbc_clump_distance 500000 \
        --wbc_pvalue_threshold 1.0e-05 \
        --collect_locus \
        --remove_mhc \
        ${args}
    """

    stub:
    def prefix = task.ext.prefix ?: meta.studyId
    """
    mkdir -p locus_breaker_clumped_study_locus
    touch locus_breaker_clumped_study_locus/${prefix}.parquet
    """
}
