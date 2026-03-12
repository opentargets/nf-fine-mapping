#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

include { Collector } from './modules/collector/main.nf'
// include { Intersection } from './modules/intersection/main.nf'
// include { SuShiE       } from './modules/sushie/main.nf'
// include { SubsetLD     } from './modules/ld/main.nf'

def intro() {
    log.info(
        """
        Multi Ancestry Fine Mapping Pipeline

        Parameters:

        ld_reference:       ${params.ld_reference}
        output directory:   ${params.output_dir}
        manifest:           ${params.manifest}
        chain:              ${params.chain}
        liftover:           ${params.liftover}

    """.stripIndent()
    )
}

def read_ancestries(path) {
    return channel.fromPath(path)
        .splitCsv(header: true, sep: '\t')
        .map { row -> tuple(row.ancestry, row.ldMatrix, row.ldIndex) }
}

def read_manifest(path) {
    return channel.fromPath(path)
        .splitCsv(header: true, sep: '\t')
        .map { row ->
            [
                [trait: row.trait, sampleSize: row.sampleSize, ancestry: row.ancestry],
                file(row.summaryStatisticsPath),
            ]
        }
}

def group_by_trait(input_ch) {
    return input_ch
        .map { meta, parquet ->
            [meta.trait, [sampleSize: meta.sampleSize, ancestry: meta.ancestry], file(parquet)]
        }
        .groupTuple()
}


workflow FINE_MAPPING {
    input_ch = read_manifest(params.manifest)
    ld_reference_ch = read_ancestries(params.ld_reference)
    input_ch.view { it -> log.info("Input manifest: ${it}") }
    ld_reference_ch.view { it -> log.info("LD reference: ${it}") }
    collected = Collector(input_ch)
    collected.view { it -> log.info("Collected: ${it}") }
    grouped = group_by_trait(collected)
    grouped.view { it -> log.info("Grouped: ${it}") }
}
// intersection = Intersection(input_ch)
// variant_intersection_ch = intersection.intersection
// filtered_sumstats_ch = intersection.filtered_sumstats
//     subset_ld_ch = SubsetLD(variant_intersection_ch, ld_reference_ch)
//     SuShiE(filtered_sumstats_ch, subset_ld_ch)

workflow {
    intro()
    FINE_MAPPING()
    workflow.onComplete { log.info("Pipeline complete!") }
}
