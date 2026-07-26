#!/usr/bin/env nextflow

nextflow.enable.dsl = 2
nextflow.enable.types = true

include { LOCUS_BREAKER    } from './workflows/locus_breaker/main.nf'
include { LOCUS_COLLECTION } from './workflows/locus_collection/main.nf'
include { LOCUS_ANNOTATION } from './workflows/locus_annotation/main.nf'

params {
    manifest: String
    manifest_base_dir: String
    output_dir: String
    route: String
}

def intro() -> Void {
    def RESET = '\u001B[0m'
    def BOLD = '\u001B[1m'
    def CYAN = '\u001B[36m'
    def GREEN = '\u001B[32m'

    log.info(
        """
        ${CYAN}${BOLD}
        IDIC Infinite Diversity in Infinite Combinations
         __   _______   __    ______
        |  | |       \\ |  |  /      |
        |  | |  .--.  ||  | |  ,----'
        |  | |  |  |  ||  | |  |
        |  | |  '--'  ||  | |  `----.
        |__| |_______/ |__|  \\______|
        ${RESET}

        ${BOLD}Here is your fancy configuration:${RESET}

        ${GREEN}output:${RESET}   ${params.output_dir}
        ${GREEN}manifest:${RESET} ${params.manifest}
        ${GREEN}route:${RESET}    ${params.route}

        """.stripIndent()
    )
}

def manifest_row_to_record(row: List<String>, manifest_base_dir: String) -> Map {
    def traitSet: List<String> = row[5].tokenize(',')
    def summary_statistics_path = row[3].startsWith('/') ? row[3] : "${manifest_base_dir}/${row[3]}"

    def meta = [
        runId: row[0],
        studyId: row[1],
        route: row[2],
        ancestry: row[4],
        traitSet: traitSet,
        sampleSize: row[6].toInteger(),
    ]

    return [
        summary_statistics_path: file(summary_statistics_path),
        meta: meta,
    ]
}


def read_manifest(path: String) -> Channel<Map> {
    def manifest_channel = channel.fromPath(path)
        .flatMap { manifest ->
            manifest.splitCsv(
                sep: '\t',
                skip: 1,
            )
        }
        .map { row ->
            manifest_row_to_record(row as List<String>, params.manifest_base_dir)
        }

    log.info("Manifest file read successfully: ${path}")

    return manifest_channel
}


def filter_manifest_by_route(manifest_channel, route: String) {
    return manifest_channel.filter { row ->
        row.meta.route == route.toString()
    }
}



workflow {

    main:
    intro()
    input_ch = channel.fromPath(params.manifest)
        .flatMap { manifest ->
            manifest.splitCsv(
                sep: '	',
                skip: 1,
            )
        }
        .map { row ->
            manifest_row_to_record(row as List<String>, params.manifest_base_dir)
        }

    log.info("Manifest file read successfully: ${params.manifest}")

    filtered_ch = input_ch.filter { row ->
        row.meta.route == params.route
    }

    locus_out = LOCUS_BREAKER(filtered_ch)

    locus_collection_out = LOCUS_COLLECTION(locus_out)
    full_overlap_loci = locus_collection_out.ch_full_overlap_loci
    partial_overlap_loci = locus_collection_out.ch_partial_overlap_loci
    non_overlap_loci = locus_collection_out.ch_non_overlap_loci
    collect_loci_stats = locus_collection_out.ch_collect_loci_stats
    locus_annotation_out = LOCUS_ANNOTATION(full_overlap_loci)
    locus_annotation = locus_annotation_out.ch_locus_annotation
    fine_mapping_loci = locus_annotation_out.ch_fine_mapping_loci
    ld_pairs = locus_annotation_out.ch_ld_pairs

    publish:
    loci                 = locus_out
    full_overlap_loci    = full_overlap_loci
    partial_overlap_loci = partial_overlap_loci
    non_overlap_loci     = non_overlap_loci
    collect_loci_stats   = collect_loci_stats
    locus_annotation     = locus_annotation
    fine_mapping_loci    = fine_mapping_loci
    ld_pairs             = ld_pairs

    onComplete:
    log.info('Pipeline complete!')
}


output {
    loci {
        path 'locus_breaker_clumped_study_locus'
        mode 'copy'
    }
    full_overlap_loci {
        path 'collected_loci/full_overlaps'
        mode 'copy'
    }
    partial_overlap_loci {
        path 'collected_loci/partial_overlaps'
        mode 'copy'
    }
    non_overlap_loci {
        path 'collected_loci/non_overlaps'
        mode 'copy'
    }
    collect_loci_stats {
        path 'collected_loci/stats'
        mode 'copy'
    }
    locus_annotation {
        path 'locus_annotation'
        mode 'copy'
    }
    fine_mapping_loci {
        path 'locus_annotation/fine_mapping_loci'
        mode 'copy'
    }
    ld_pairs {
        path 'locus_annotation/ld_pairs'
        mode 'copy'
    }
}
