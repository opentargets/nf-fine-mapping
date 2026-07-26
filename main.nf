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
    ld_references: List = []
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


def read_manifest(path: String) {
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


def registered_ld_reference_ancestries(ld_references) -> Set<String> {
    if (ld_references == null || !(ld_references instanceof List) || ld_references.isEmpty()) {
        error "Manifest ancestry validation requires non-empty params.ld_references."
    }

    def ancestry_labels = ld_references.collect { entry ->
        if (!(entry instanceof Map) || !entry.containsKey('ancestry')) {
            error "Manifest ancestry validation requires each params.ld_references entry to define ancestry."
        }

        def ancestry = entry.ancestry
        if (ancestry == null || ancestry.toString().isEmpty()) {
            error "Manifest ancestry validation requires each params.ld_references entry to define a non-empty ancestry label."
        }

        ancestry.toString()
    }

    def duplicate_ancestries = ancestry_labels
        .countBy { ancestry -> ancestry }
        .findAll { _ancestry, count -> count > 1 }
        .keySet()
        .toList()
        .sort()

    if (duplicate_ancestries) {
        error "Duplicate ld_references ancestry labels: ${duplicate_ancestries.join(', ')}"
    }

    return ancestry_labels as Set<String>
}


def manifest_validation_status_record(row: Map) -> Map {
    return [
        runId: row.meta.runId,
        path: row.summary_statistics_path.toString(),
        validationStage: 'MANIFEST',
        reason: 'UNREGISTERED_ANCESTRY',
    ]
}


process MANIFEST_VALIDATION_REPORT {
    tag "${runId}"

    input:
    tuple(runId: String, status_records: List<String>)

    output:
    status = file("validation/manifest/*.jsonl")

    script:
    def prefix = task.ext.prefix ?: runId
    def status_lines = status_records.collect { record -> "'${record}'" }.join(' ')
    """
    mkdir -p validation/manifest
    printf '%s\\n' ${status_lines} > validation/manifest/${prefix}.jsonl
    """

    stub:
    def prefix = task.ext.prefix ?: runId
    def status_lines = status_records.collect { record -> "'${record}'" }.join(' ')
    """
    mkdir -p validation/manifest
    printf '%s\\n' ${status_lines} > validation/manifest/${prefix}.jsonl
    """
}


workflow MANIFEST_VALIDATION {
    take:
    manifest_rows

    main:
    def registered_ancestries = registered_ld_reference_ancestries(params.ld_references)
    manifest_validation_rows_ch = manifest_rows.branch { row ->
        supported: registered_ancestries.contains(row.meta.ancestry)
        unsupported: !registered_ancestries.contains(row.meta.ancestry)
    }

    supported_manifest_rows = manifest_validation_rows_ch.supported
    unsupported_manifest_status_input_ch = manifest_validation_rows_ch.unsupported
        .map { row ->
            tuple(row.meta.runId, groovy.json.JsonOutput.toJson(manifest_validation_status_record(row)))
        }
        .groupTuple(by: 0)

    MANIFEST_VALIDATION_REPORT(unsupported_manifest_status_input_ch)
    manifest_validation_status = MANIFEST_VALIDATION_REPORT.out.status

    emit:
    supported_manifest_rows = supported_manifest_rows
    manifest_validation_status = manifest_validation_status
}



workflow {

    main:
    intro()
    manifest_ch = read_manifest(params.manifest)
    filtered_ch = filter_manifest_by_route(manifest_ch, params.route)
    manifest_validation_out = MANIFEST_VALIDATION(filtered_ch)
    supported_manifest_ch = manifest_validation_out.supported_manifest_rows
    manifest_validation_status = manifest_validation_out.manifest_validation_status

    locus_out = LOCUS_BREAKER(supported_manifest_ch)

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
    manifest_validation_status = manifest_validation_status
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
    manifest_validation_status {
        path 'validation/manifest'
        mode 'copy'
    }
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
