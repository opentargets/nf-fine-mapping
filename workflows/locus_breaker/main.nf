nextflow.enable.dsl = 2
nextflow.enable.types = true

include { COLLECTOR_LOCUS_BREAKER } from '../../modules/local/collector/locus_breaker/main.nf'
include { COLLECTOR_EMPTY_STATUS as LOCUS_BREAKER_EMPTY_STATUS } from '../../modules/local/collector/empty_status/main.nf'
include { GENTROPY_LOCUS_BREAKER_CLUMPING } from '../../modules/local/gentropy/locus_breaker_clumping/main.nf'


workflow LOCUS_BREAKER {
    take:
    ch_sumstats: Channel<Map>

    main:
    // Keep the process input as a native tuple of Map/Path values. This avoids
    // custom record identity hashing, which would make `-resume` unstable.
    ch_input = ch_sumstats.map { r ->
        tuple(
            [
            runId: r.meta.runId,
            studyId: r.meta.studyId,
            route: r.meta.route,
            ancestry: r.meta.ancestry,
            traitSet: r.meta.traitSet,
            sampleSize: r.meta.sampleSize,
            ],
            r.summary_statistics_path,
        )
    }

    locus_breaker_method = params.locus_breaker_method.toString().toLowerCase()
    if (locus_breaker_method == 'collector') {
        ch_locus_out = COLLECTOR_LOCUS_BREAKER(ch_input)
        ch_locus = ch_locus_out.filter { r -> r.study_locus_path }
        ch_status = ch_locus_out
            .map { r -> r.status_path }
            .filter { path -> path }
    } else if (locus_breaker_method == 'gentropy') {
        ch_locus_out = GENTROPY_LOCUS_BREAKER_CLUMPING(ch_input)
        ch_locus = ch_locus_out
        ch_empty_status_input = ch_locus_out
            .map { r ->
                record(
                    runId: r.meta.runId,
                    logical_path: "gentropy_locus_breaker_clumped_study_locus/${r.meta.studyId}",
                    validation_stage: "LOCUS_BREAKER",
                    dataset_path: r.study_locus_path,
                )
            }
            .unique { row -> "${row.runId}\t${row.logical_path}" }
            .map { row ->
                tuple(row.runId, row.logical_path, row.validation_stage, row.dataset_path)
            }
        ch_status = LOCUS_BREAKER_EMPTY_STATUS(ch_empty_status_input)
            .filter { status_path -> status_path != null }
    } else {
        error "Unsupported locus_breaker_method '${params.locus_breaker_method}'. Expected 'collector' or 'gentropy'."
    }

    emit:
    ch_locus = ch_locus
    ch_status = ch_status
}
