nextflow.enable.dsl = 2
nextflow.enable.types = true

include { ManifestRecord ; MetaRecord ; LocusRecord } from '../../types.nf'



workflow LOCUS_BREAKER {
    take:
    ch_sumstats: Channel<ManifestRecord>

    main:
    ch_locus = collector_locus_breaker(ch_sumstats)
    ch_locus2 = locus_breaker_clumping(ch_sumstats)

    emit:
    ch_locus  : Channel<LocusRecord> = ch_locus
    ch_locus2 : Channel<LocusRecord> = ch_locus2
}


process collector_locus_breaker {
    tag "${ch_sumstats.meta.route}"
    tag "${ch_sumstats.meta.traitSet}"
    tag "${ch_sumstats.meta.ancestry}"
    tag "${ch_sumstats.meta.runId}"
    tag "${ch_sumstats.meta.studyId}"

    label "collector"
    label "locus_breaker"

    publishDir "${params.output_dir}", mode: 'copy', pattern: 'locus_breaker_clumped_study_locus/*.parquet'

    input:
    ch_sumstats: ManifestRecord

    output:
    record(
        meta: ch_sumstats.meta,
        study_locus_path: file("locus_breaker_clumped_study_locus/${ch_sumstats.meta.studyId}.parquet"),
    )

    script:
    """
    mkdir -p locus_breaker_clumped_study_locus

    collector locus_breaker \
        --input ${ch_sumstats.summary_statistics_path} \
        --output locus_breaker_clumped_study_locus/${ch_sumstats.meta.studyId}.parquet \
        --lbc_baseline_pvalue 1.0e-05 \
        --lbc_distance_cutoff 250000 \
        --lbc_pvalue_threshold 1.0e-08 \
        --lbc_flanking_distance 100000 \
        --large_loci_size 1500000 \
        --wbc_clump_distance 500000 \
        --wbc_pvalue_threshold 1.0e-05 \
        --collect_locus \
        --remove_mhc
    """

    stub:
    """
    mkdir -p locus_breaker_clumped_study_locus
    touch locus_breaker_clumped_study_locus/${ch_sumstats.meta.studyId}.parquet
    """
}


process locus_breaker_clumping {
    tag "${ch_sumstats.meta.route}"
    tag "${ch_sumstats.meta.traitSet}"
    tag "${ch_sumstats.meta.ancestry}"
    tag "${ch_sumstats.meta.runId}"
    tag "${ch_sumstats.meta.studyId}"

    label "spark"
    label "gentropy"
    label "locus_breaker"

    publishDir "${params.output_dir}", mode: 'copy'

    input:
    ch_sumstats: ManifestRecord

    stage:
    env 'HYDRA_FULL_ERROR', "1"

    output:
    record(
        study_locus_path: file("gentropy_locus_breaker_clumped_study_locus/${ch_sumstats.meta.studyId}"),
        meta: ch_sumstats.meta,
    )

    script:
    """
    gentropy step=locus_breaker_clumping \
        step.session.write_mode=overwrite \
        step.session.log_level=ERROR \
        step.session.output_partitions=1 \
        step.summary_statistics_input_path=${ch_sumstats.summary_statistics_path.name} \
        step.clumped_study_locus_output_path="gentropy_locus_breaker_clumped_study_locus/${ch_sumstats.meta.studyId}" \
        step.lbc_baseline_pvalue=1.0e-05 \
        step.lbc_distance_cutoff=250000 \
        step.lbc_pvalue_threshold=1.0e-08 \
        step.lbc_flanking_distance=100000 \
        step.large_loci_size=1500000 \
        step.wbc_clump_distance=500000 \
        step.wbc_pvalue_threshold=1.0e-05 \
        step.collect_locus=true \
        step.remove_mhc=true \
    """

    stub:
    """
    mkdir -p gentropy_locus_breaker_clumped_study_locus/${ch_sumstats.meta.studyId}
    touch gentropy_locus_breaker_clumped_study_locus/${ch_sumstats.meta.studyId}/_SUCCESS
    touch gentropy_locus_breaker_clumped_study_locus/${ch_sumstats.meta.studyId}/part-00000.parquet

    """
}


process clumping_report {

    label "collector"

    input:
    array_of_locus_paths: List<Path>

    output:
    file("clumping_report.parquet")

    script:
    """
    collector clumping_report \
        ${array_of_locus_paths.join(' ')} \
        --output clumping_report.parquet
    """

    stub:
    """
    touch clumping_report.parquet
    """
}
