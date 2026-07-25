nextflow.enable.dsl = 2
nextflow.enable.types = true


process GENTROPY_LOCUS_BREAKER_CLUMPING {
    tag "${meta.route}"
    tag "${meta.traitSet}"
    tag "${meta.ancestry}"
    tag "${meta.runId}"
    tag "${meta.studyId}"

    label "spark"
    label "gentropy"
    label "locus_breaker"

    publishDir "${params.output_dir}", mode: 'copy'

    input:
    tuple(meta: Map, summary_statistics_path: Path)

    stage:
    env 'HYDRA_FULL_ERROR', "1"

    output:
    record(
        meta: meta,
        study_locus_path: file("gentropy_locus_breaker_clumped_study_locus/*", type: 'dir'),
    )

    topic:
    tuple("${task.process}", "gentropy", "3.3.0-dev.64") >> "versions"

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: meta.studyId
    def gentropy_spark_uri = params.gentropy_spark_uri ?: 'local[*]'
    def gentropy_spark_conf = params.gentropy_spark_conf ?: '{}'
    """
    gentropy step=locus_breaker_clumping \
        'step.session.spark_uri="${gentropy_spark_uri}"' \
        '+step.session.extended_spark_conf=${gentropy_spark_conf}' \
        step.session.write_mode=overwrite \
        step.session.log_level=ERROR \
        step.session.output_partitions=1 \
        step.summary_statistics_input_path=${summary_statistics_path} \
        step.clumped_study_locus_output_path="gentropy_locus_breaker_clumped_study_locus/${prefix}" \
        step.lbc_baseline_pvalue=1.0e-05 \
        step.lbc_distance_cutoff=250000 \
        step.lbc_pvalue_threshold=1.0e-08 \
        step.lbc_flanking_distance=100000 \
        step.large_loci_size=1500000 \
        step.wbc_clump_distance=500000 \
        step.wbc_pvalue_threshold=1.0e-05 \
        step.collect_locus=true \
        step.remove_mhc=true \
        ${args}
    """

    stub:
    def prefix = task.ext.prefix ?: meta.studyId
    """
    mkdir -p gentropy_locus_breaker_clumped_study_locus/${prefix}
    touch gentropy_locus_breaker_clumped_study_locus/${prefix}/_SUCCESS
    touch gentropy_locus_breaker_clumped_study_locus/${prefix}/part-00000.parquet

    """
}
