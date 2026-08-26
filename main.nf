#!/usr/bin/env nextflow

nextflow.enable.dsl = 2
nextflow.enable.types = true

include { LOCUS_BREAKER    } from './workflows/locus_breaker/main.nf'
include { LOCUS_COLLECTION } from './workflows/locus_collection/main.nf'
include { LOCUS_ANNOTATION } from './workflows/locus_annotation/main.nf'
include { FINE_MAPPING     } from './workflows/fine_mapping/main.nf'
include { validateParameters; paramsSummaryLog; samplesheetToList } from 'plugin/nf-schema'

params {
    manifest: String
    manifest_base_dir: String
    output_dir: String
    route: String
    ld_registry: List = []
    ld_annotation_method: String = 'hailing_ducks'
    hailing_ducks_container: String = 'ghcr.io/project-defiant/hailing-ducks:v1.1.0'
    hailing_ducks_max_cached_blocks: Integer = 8
    fine_mapping_methods: List = ['multisusie']
    validate_params: Boolean = true
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

def manifest_entry_to_record(entry: List, manifest_base_dir: String) -> Map {
    def meta = entry[0] as Map
    def summarystats_location = entry[1] as String
    def trait_from_source_mapped_ids = entry[2] as String

    def summary_statistics_path = (summarystats_location.startsWith('/') || summarystats_location.contains('://'))
        ? summarystats_location
        : "${manifest_base_dir}/${summarystats_location}"

    return [
        summary_statistics_path: file(summary_statistics_path),
        meta: meta + [traitSet: trait_from_source_mapped_ids.tokenize(',')],
    ]
}


def read_manifest(path: String) {
    def manifest_channel = channel.fromList(
        samplesheetToList(path, "assets/schema_manifest.json")
    ).map { entry ->
        manifest_entry_to_record(entry as List, params.manifest_base_dir)
    }

    log.info("Manifest file read successfully: ${path}")

    return manifest_channel
}


def filter_manifest_by_route(manifest_channel, route: String) {
    return manifest_channel.filter { row ->
        row.meta.route == route.toString()
    }
}


def registered_ld_registry_ancestries(ld_registry) -> Set<String> {
    if (ld_registry == null || !(ld_registry instanceof List) || ld_registry.isEmpty()) {
        error "Manifest ancestry validation requires non-empty params.ld_registry."
    }

    def required_fields = ['ancestry', 'bm_path', 'ht_path']

    def ancestry_labels = ld_registry.collect { entry ->
        if (!(entry instanceof Map) || !entry.containsKey('ancestry')) {
            error "Manifest ancestry validation requires each params.ld_registry entry to define ancestry."
        }

        def missing_fields = required_fields.findAll { field ->
            !entry.containsKey(field) || entry[field] == null || entry[field].toString().trim().isEmpty()
        }
        if (missing_fields) {
            error "Manifest ancestry validation requires each params.ld_registry entry to define non-empty ${missing_fields.join(', ')}."
        }

        def ancestry = entry.ancestry
        if (ancestry == null || ancestry.toString().isEmpty()) {
            error "Manifest ancestry validation requires each params.ld_registry entry to define a non-empty ancestry label."
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
        error "Duplicate ld_registry ancestry labels: ${duplicate_ancestries.join(', ')}"
    }

    return ancestry_labels as Set<String>
}


def validate_canonical_region_size_floor() -> Void {
    def locus_breaker_method = params.locus_breaker_method.toString().toLowerCase()
    // validateParameters() (which would otherwise coerce these to Integer per
    // the schema) is only called when params.validate_params is true, and this
    // check runs unconditionally regardless -- so a String value supplied via
    // CLI or -params-file must be coerced explicitly here, or a numeric
    // comparison silently becomes a lexicographic String comparison.
    def max_region_span_bp = params.canonical_region_max_region_span_bp.toInteger()
    def large_loci_size = params.locus_breaker_large_loci_size.toInteger()
    if (locus_breaker_method == 'collector' && max_region_span_bp < large_loci_size + 1) {
        error "params.canonical_region_max_region_span_bp (${max_region_span_bp}) must be at least params.locus_breaker_large_loci_size + 1 (${large_loci_size + 1}) when locus_breaker_method is 'collector'; the locus breaker emits loci up to locus_breaker_large_loci_size wide, an inclusive span of large_loci_size + 1, so a smaller region-size limit cannot accommodate even a single locus-breaker locus."
    }
}


def manifest_validation_status_record(row: Map) -> Map {
    return [
        runId: row.meta.runId,
        path: row.summary_statistics_path.toString(),
        validationStage: 'MANIFEST',
        reason: 'UNREGISTERED_ANCESTRY',
    ]
}


def invalid_run_ids_from_status_channels(status_channels) {
    def active_status_channels = status_channels.findAll { status_channel -> status_channel != null }
    if (!active_status_channels) {
        return channel.value([] as Set<String>)
    }

    def combined_status_channels = active_status_channels.tail().inject(active_status_channels.head()) { mixed_status_channel, status_channel ->
        mixed_status_channel.mix(status_channel)
    }

    def parsed_status_records = combined_status_channels.flatMap { status_path ->
        if (status_path == null) {
            return []
        }
        status_path.readLines()
            .findAll { line -> line }
            .collect { line -> new groovy.json.JsonSlurper().parseText(line) as Map }
    }

    def collect_invalid_run_ids = parsed_status_records
        .map { status_record -> status_record.runId.toString() }
        .unique()
        .collect()
        .map { invalid_run_ids -> invalid_run_ids as Set<String> }

    return collect_invalid_run_ids
}


def invalid_fine_mapping_locus_set_ids_from_status_channels(status_channels) {
    def active_status_channels = status_channels.findAll { status_channel -> status_channel != null }
    if (!active_status_channels) {
        return channel.value([] as Set<String>)
    }

    def combined_status_channels = active_status_channels.tail().inject(active_status_channels.head()) { mixed_status_channel, status_channel ->
        mixed_status_channel.mix(status_channel)
    }

    def parsed_status_records = combined_status_channels.flatMap { status_path ->
        if (status_path == null) {
            return []
        }
        status_path.readLines()
            .findAll { line -> line }
            .collect { line -> new groovy.json.JsonSlurper().parseText(line) as Map }
    }

    return parsed_status_records
        .map { status_record -> status_record.fineMappingLocusSetId.toString() }
        .unique()
        .collect()
        .map { invalid_locus_set_ids -> invalid_locus_set_ids as Set<String> }
}


def filter_rows_by_invalid_run_ids(rows, invalid_run_ids, extract_run_id) {
    return rows
        .combine(invalid_run_ids)
        .filter { row, collected_invalid_run_ids ->
            !collected_invalid_run_ids.contains(extract_run_id.call(row).toString())
        }
        .map { row, _collected_invalid_run_ids -> row }
}


process MANIFEST_VALIDATION_REPORT {
    tag "${runId}"
    container 'ubuntu:22.04'

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


process PIPELINE_VERSIONS_REPORT {
    container 'ubuntu:22.04'

    input:
    version_records: List<String>

    output:
    versions = file("pipeline_info/software_versions.jsonl")

    script:
    def version_lines = version_records.collect { record -> "'${record}'" }.join(' ')
    """
    mkdir -p pipeline_info
    printf '%s\\n' ${version_lines} > pipeline_info/software_versions.jsonl
    """

    stub:
    def version_lines = version_records.collect { record -> "'${record}'" }.join(' ')
    """
    mkdir -p pipeline_info
    printf '%s\\n' ${version_lines} > pipeline_info/software_versions.jsonl
    """
}


workflow MANIFEST_VALIDATION {
    take:
    manifest_rows

    main:
    def registered_ancestries = registered_ld_registry_ancestries(
        params.ld_registry
    )
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
    if (params.validate_params) {
        validateParameters()
    }
    validate_canonical_region_size_floor()
    log.info paramsSummaryLog(workflow)
    manifest_ch = read_manifest(params.manifest)
    filtered_ch = filter_manifest_by_route(manifest_ch, params.route)
    manifest_validation_out = MANIFEST_VALIDATION(filtered_ch)
    manifest_validation_status = manifest_validation_out.manifest_validation_status
    manifest_invalid_run_ids = invalid_run_ids_from_status_channels([manifest_validation_status])
    supported_manifest_ch = filter_rows_by_invalid_run_ids(
        manifest_validation_out.supported_manifest_rows,
        manifest_invalid_run_ids,
        { manifest_row -> manifest_row.meta.runId },
    )

    locus_breaker_out = LOCUS_BREAKER(supported_manifest_ch)
    locus_breaker_status = locus_breaker_out.ch_status
    locus_out = filter_rows_by_invalid_run_ids(
        locus_breaker_out.ch_locus,
        manifest_invalid_run_ids,
        { locus_row -> locus_row.meta.runId },
    )

    locus_collection_out = LOCUS_COLLECTION(locus_out)
    locus_collection_status = locus_collection_out.ch_locus_collection_status
    published_locus_out = filter_rows_by_invalid_run_ids(
        locus_out,
        manifest_invalid_run_ids,
        { locus_row -> locus_row.meta.runId },
    )
    full_overlap_loci = filter_rows_by_invalid_run_ids(
        locus_collection_out.ch_full_overlap_loci,
        manifest_invalid_run_ids,
        { collected_locus_row -> collected_locus_row.runId },
    )
    partial_overlap_loci = filter_rows_by_invalid_run_ids(
        locus_collection_out.ch_partial_overlap_loci,
        manifest_invalid_run_ids,
        { collected_locus_row -> collected_locus_row.runId },
    )
    non_overlap_loci = filter_rows_by_invalid_run_ids(
        locus_collection_out.ch_non_overlap_loci,
        manifest_invalid_run_ids,
        { collected_locus_row -> collected_locus_row.runId },
    )
    collect_loci_stats = filter_rows_by_invalid_run_ids(
        locus_collection_out.ch_collect_loci_stats,
        manifest_invalid_run_ids,
        { collected_locus_row -> collected_locus_row.runId },
    )
    locus_annotation_out = LOCUS_ANNOTATION(full_overlap_loci)
    locus_annotation_status = locus_annotation_out.ch_ld_pair_stats_status
    locus_annotation = locus_annotation_out.ch_locus_annotation
    fine_mapping_locus_sets = locus_annotation.map { annotation_row -> annotation_row.fine_mapping_locus_set_path }
    ld_pair_stats = locus_annotation.map { annotation_row -> annotation_row.stats_path }
    fine_mapping_out = FINE_MAPPING(locus_annotation)

    ch_versions = channel.topic('versions')
        .map { record -> groovy.json.JsonOutput.toJson([process: record[0], tool: record[1], version: record[2]]) }
        .collect()
        .map { records -> (records.unique()) as List<String> }
    PIPELINE_VERSIONS_REPORT(ch_versions)
    pipeline_versions = PIPELINE_VERSIONS_REPORT.out.versions

    publish:
    manifest_validation_status = manifest_validation_status
    locus_breaker_status   = locus_breaker_status
    locus_collection_status = locus_collection_status
    locus_annotation_status = locus_annotation_status
    loci                 = published_locus_out
    full_overlap_loci    = full_overlap_loci
    partial_overlap_loci = partial_overlap_loci
    non_overlap_loci     = non_overlap_loci
    collect_loci_stats   = collect_loci_stats
    fine_mapping_locus_sets = fine_mapping_locus_sets
    ld_pair_stats         = ld_pair_stats
    multisusie_results    = fine_mapping_out.ch_multisusie
    susiex_results        = fine_mapping_out.ch_susiex
    sushie_results        = fine_mapping_out.ch_sushie
    pipeline_versions     = pipeline_versions

    onComplete:
    log.info('Pipeline complete!')
}


output {
    // workflow.output.dir is set to params.output_dir in nextflow.config;
    // all paths here are relative to that base.
    manifest_validation_status {
        path '.'
        mode 'copy'
    }
    loci {
        path '.'
        mode 'copy'
    }
    multisusie_results {
        path '.'
        mode 'copy'
    }
    susiex_results {
        path '.'
        mode 'copy'
    }
    sushie_results {
        path '.'
        mode 'copy'
    }
    pipeline_versions {
        path '.'
        mode 'copy'
    }
    locus_breaker_status {
        path '.'
        mode 'copy'
    }
    locus_collection_status {
        path '.'
        mode 'copy'
    }
    locus_annotation_status {
        path '.'
        mode 'copy'
    }
    // COLLECT_CANONICAL_REGIONS writes locus parquets under fine_mapping_locus_sets/;
    // publishing to collected_loci/ makes the final path collected_loci/fine_mapping_locus_sets/*.
    full_overlap_loci {
        path 'collected_loci'
        mode 'copy'
    }
    partial_overlap_loci {
        path 'collected_loci'
        mode 'copy'
    }
    non_overlap_loci {
        path 'collected_loci'
        mode 'copy'
    }
    // stats.parquet / stats.json are flat files — collected_loci/stats/ is the target.
    collect_loci_stats {
        path 'collected_loci/stats'
        mode 'copy'
    }
    // HAILING_DUCKS_LD_ANNOTATION writes fine_mapping_locus_sets/ and
    // hailing_ducks_ld_annotation/ — locus_annotation/ is the correct base.
    fine_mapping_locus_sets {
        path 'locus_annotation'
        mode 'copy'
    }
    ld_pair_stats {
        path 'locus_annotation'
        mode 'copy'
    }
}
