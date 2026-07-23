nextflow.enable.types = true


process locus_breaker_clumping {
    tag "${meta.studyId}"
    tag "${meta.route}"
    tag "${meta.traitSet}"
    tag "${meta.ancestry}"
    tag "${meta.runId}"

    label "spark"
    label "gentropy"
    label "locus_breaker"

    publishDir "${params.output_dir}", mode: 'copy', pattern: 'locus_breaker_clumped_study_locus/*/*.parquet'

    input:
    record(
        summary_statistics_path: Path,
        meta: Map
    )

    stage:
    env 'HYDRA_ERROR', "true"

    output:
    record(
        study_locus_path: file("locus_breaker_clumped_study_locus/${meta.studyId}"),
        meta: meta,
    )

    script:
    """
    gentropy step=locus_breaker_clumping \
        step.session.write_mode=overwrite \
        step.session.log_level=ERROR \
        step.session.output_partitions=1 \
        step.summary_statistics_input_path=${summary_statistics_path} \
        step.clumped_study_locus_output_path="locus_breaker_clumped_study_locus/${meta.studyId}" \
        lbc_baseline_pvalue=1.0e-05 \
        lbc_distance_cutoff=250000 \
        lbc_pvalue_threshold=1.0e-08 \
        lbc_flanking_distance=100000 \
        large_loci_size=1500000 \
        wbc_clump_distance=500000 \
        wbc_pvalue_threshold=1.0e-05 \
        collect_locus=true \
        remove_mhc=true \
    """

    stub:
    """
    mkdir -p locus_breaker_clumped_study_locus/${meta.studyId}
    touch locus_breaker_clumped_study_locus/${meta.studyId}/_SUCCESS
    touch locus_breaker_clumped_study_locus/${meta.studyId}/part-00000.parquet

    """
}
