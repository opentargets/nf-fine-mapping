#!/usr/bin/env nextflow

nextflow.enable.dsl = 2
nextflow.enable.types = true

include { LOCUS_BREAKER  } from './workflows/locus_breaker/main.nf'

params {
    manifest: String
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

def manifest_row_to_record(row: List<String>) -> Map {
    def traitSet: List<String> = row[5].tokenize(',')

    def meta = [
        runId: row[0],
        studyId: row[1],
        route: row[2],
        ancestry: row[4],
        traitSet: traitSet,
        sampleSize: row[6].toInteger(),
    ]

    return [
        summary_statistics_path: file(row[3]),
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
            manifest_row_to_record(row as List<String>)
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
            manifest_row_to_record(row as List<String>)
        }

    log.info("Manifest file read successfully: ${params.manifest}")

    filtered_ch = input_ch.filter { row ->
        row.meta.route == params.route
    }

    locus_breaker_out = LOCUS_BREAKER(filtered_ch)
    locus_out = locus_breaker_out.ch_locus
    locus_out2 = locus_breaker_out.ch_locus2

    publish:
    loci  = locus_out
    loci2 = locus_out2

    onComplete:
    log.info('Pipeline complete!')
}


output {
    loci {
        path 'locus_breaker_clumped_study_locus'
        mode 'copy'
    }
    loci2 {
        path 'gentropy_locus_breaker_clumped_study_locus'
        mode 'copy'
    }
}
