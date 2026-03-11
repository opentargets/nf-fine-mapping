#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

include { Intersection } from './modules/intersection/main.nf'
include { SuShiE       } from './modules/sushie/main.nf'
include { SubsetLD     } from './modules/ld/main.nf'

def intro() {
    log.info(
        """
        Multi Ancestry Fine Mapping Pipeline

        Parameters:

        ld_reference:       ${params.ld_reference}
        output directory:   ${params.output_dir}
        manifest:           ${params.manifest}

    """.stripIndent()
    )
}

def read_ancestries(path) {
    return channel.fromPath(path)
        .splitCsv(header: true, sep: '\t')
        .map { row -> tuple(row.ancestry, file(row.ld_file)) }
}

def read_manifest(path) {
    return channel.fromPath(path)
        .splitCsv(header: true, sep: '\t')
        .map { row -> tuple(file(row.sumstats_file), row.ancestry, row.trait, row.sample_size) }
}

workflow FINE_MAPPING {
    input_ch = read_manifest(params.manifest)
    ld_reference_ch = read_ancestries(params.ld_reference)
    intersection = Intersection(input_ch)
    variant_intersection_ch = intersection.intersection
    filtered_sumstats_ch = intersection.filtered_sumstats

    subset_ld_ch = SubsetLD(variant_intersection_ch, ld_reference_ch)
    SuShiE(filtered_sumstats_ch, subset_ld_ch)
}


workflow {
    intro()
    FINE_MAPPING()
    workflow.onComplete { log.info("Pipeline complete!") }
}
