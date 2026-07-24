nextflow.enable.dsl = 2
nextflow.enable.types = true

include { COLLECTOR_LOCUS_BREAKER } from '../../modules/local/collector/locus_breaker/main.nf'
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
        ch_locus = COLLECTOR_LOCUS_BREAKER(ch_input)
    } else if (locus_breaker_method == 'gentropy') {
        ch_locus = GENTROPY_LOCUS_BREAKER_CLUMPING(ch_input)
    } else {
        error "Unsupported locus_breaker_method '${params.locus_breaker_method}'. Expected 'collector' or 'gentropy'."
    }

    emit:
    ch_locus
}
