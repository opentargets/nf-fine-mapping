include { locus_breaker_clumping } from './modules/locus_breaker.nf'
include { clumping_report        } from './modules/clumping_report.nf'


workflow LOCUS_BREAKER {
    take:
    ch_sumstats

    main:
    ch_locus = locus_breaker_clumping(ch_sumstats)
    ch_locus.map { locus_record -> locus_record[0] } | collect
}
