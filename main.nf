#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

include { Collector    } from './modules/collector/main.nf'
include { Intersection } from './modules/intersection/main.nf'
include { Transform    } from './modules/transform/main.nf'
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
                [
                    trait: row.trait,
                    sampleSize: row.sampleSize,
                    ancestry: row.ancestry,
                ],
                file(row.summaryStatisticsPath),
            ]
        }
}

def group_by_trait(input_ch) {
    return input_ch
        .map { meta, parquet ->
            [
                meta.trait,
                [
                    sampleSize: meta.sampleSize,
                    ancestry: meta.ancestry,
                ],
                file(parquet),
            ]
        }
        .groupTuple()
}


def mix_with_intersection(intersection_ch, input_ch) {
    def _input_ch = input_ch.map { meta, sumstat ->
        tuple(
            meta.trait,
            [sampleSize: meta.sampleSize, ancestry: meta.ancestry],
            sumstat,
        )
    }
    return intersection_ch
        .combine(_input_ch, by: 0)
        .map { trait, intersection, meta, sumstat ->
            tuple(trait, meta, sumstat, intersection)
        }
}


def mix_with_ld(transformed_ch, ld_ch) {
    def _transformed = transformed_ch.map { trait, meta, transformed_sumstat ->
        tuple(meta.ancestry, trait, transformed_sumstat)
    }
    return _transformed
        .combine(ld_ch, by: 0)
        .map { ancestry, trait, transformed_sumstat, ld_matrix, ld_index ->
            tuple([ancestry: ancestry, trait: trait], transformed_sumstat, ld_matrix, ld_index)
        }
}

def annotate_with_ld(transformed_ch, ld_ch) {
    def _transform = transformed_ch.map { trait, meta, transformed_sumstat ->
        tuple(trait, meta.ancestry, meta.sampleSize, transformed_sumstat)
    }
    def _ld = ld_ch.map { meta, ld_subset ->
        tuple(meta.trait, meta.ancestry, ld_subset)
    }
    return _transform
        .combine(_ld, by: [0, 1])
        .map { trait, ancestry, sample_size, transformed_sumstat, ld_subset ->
            tuple(trait, ancestry, sample_size, transformed_sumstat, ld_subset)
        }
        .groupTuple(by: 0)
}

workflow FINE_MAPPING {
    input_ch = read_manifest(params.manifest)
    ld_reference_ch = read_ancestries(params.ld_reference)
    // input_ch.view { it -> log.info("Input manifest: ${it}") }
    // ld_reference_ch.view { it -> log.info("LD reference: ${it}") }
    collected = Collector(input_ch)
    // collected.view { it -> log.info("Collected: ${it}") }
    grouped = group_by_trait(collected)
    // grouped.view { it -> log.info("Grouped: ${it}") }
    intersection = Intersection(grouped)
    // intersection.view { it -> log.info("Intersection: ${it}") }
    mixed = mix_with_intersection(intersection, collected)
    // mixed.view { it -> log.info("Mixed: ${it}") }
    transformed = Transform(mixed)
    // transformed.view { it -> log.info("Transformed: ${it}") }
    mixed_with_ld = mix_with_ld(transformed, ld_reference_ch)
    // mixed_with_ld.view { it -> log.info("Mixed with LD: ${it}") }
    subset_ld = SubsetLD(mixed_with_ld)
    // subset_ld.view { it -> log.info("Subset LD: ${it}") }
    ld_annot = annotate_with_ld(transformed, subset_ld)
    // ld_annot.view { it -> log.info("LD Annot: ${it}") }
    SuShiE(ld_annot)
}


workflow {
    intro()
    FINE_MAPPING()
    workflow.onComplete { log.info("Pipeline complete!") }
}
