#!/usr/bin/env nextflow
nextflow.enable.dsl = 2
include { LOCUS_BREAKER } from './workflows/locus_breaker/main.nf'


def intro() {

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


def read_manifest(path) {
    def new_channel = channel.fromPath(path)
        .splitCsv(header: true, sep: '\t')
        .map { row ->
            record(
                summary_statistics_path: file(row.summaryStatisticsLocation),
                meta: [
                    runId: row.runId,
                    studyId: row.studyId,
                    route: row.route,
                    traitSet: row.traitFromSourceMappedIds,
                    sampleSize: row.sampleSize,
                    ancestry: row.majorAncestry,
                ],
            )
        }
    log.info("Manifest file read successfully: ${path}")
    return new_channel
}


def filter_manifest_by_route(manifest_channel, route) {
    return manifest_channel.filter { row -> row.meta.route == route }
}



workflow FINE_MAPPING {
    input_ch = read_manifest(params.manifest)
    filtered_ch = filter_manifest_by_route(input_ch, params.route)
    loci_ch = LOCUS_BREAKER(filtered_ch)
}


workflow {
    intro()
    FINE_MAPPING()
    workflow.onComplete { log.info("Pipeline complete!") }
}
