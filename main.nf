#!/usr/bin/env nextflow
nextflow.enable.dsl = 2


def intro() {
    log.info(
        """
        IDIC Infinite Diversity in Infinite Combinations

        Parameters:

        output: ${params.output_dir}
        manifest: ${params.manifest}
        route: ${params.route}

    """.stripIndent()
    )
}


def read_manifest(path) {
    return channel.fromPath(path)
        .splitCsv(header: true, sep: '\t')
        .map { row ->
            [
                [
                    runId: row.runId,
                    studyId: row.studyId,
                    route: row.route,
                    traitSet: row.traitFromSourceMappedIds,
                    sampleSize: row.sampleSize,
                    ancestry: row.majorAncestry,
                ],
                file(row.summaryStatisticsLocation),
            ]
        }
}



workflow FINE_MAPPING {
    input_ch = read_manifest(params.manifest) | view { "Manifest: ${it[0]}" }
}


workflow {
    intro()
    FINE_MAPPING()
    workflow.onComplete { log.info("Pipeline complete!") }
}
