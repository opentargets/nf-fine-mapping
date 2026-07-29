nextflow.enable.dsl = 2
nextflow.enable.types = true

include { MULTISUSIE_FINE_MAPPING } from '../../modules/local/multisusie/fine_mapping/main.nf'
include { SUSIEX_FINE_MAPPING } from '../../modules/local/susiex/fine_mapping/main.nf'
include { SUSHIE_FINE_MAPPING } from '../../modules/local/sushie/fine_mapping/main.nf'


workflow FINE_MAPPING {
    take:
    ch_locus_annotation: Channel<Map>

    main:
    def configured_methods = params.fine_mapping_methods ?: ['multisusie']
    if (!(configured_methods instanceof List) || configured_methods.isEmpty()) {
        error "params.fine_mapping_methods must be a non-empty list."
    }

    def fine_mapping_methods = configured_methods.collect { method ->
        method.toString().toLowerCase()
    }
    def duplicate_methods = fine_mapping_methods
        .countBy { method -> method }
        .findAll { _method, count -> count > 1 }
        .keySet()
        .toList()
        .sort()
    if (duplicate_methods) {
        error "Duplicate fine-mapping methods: ${duplicate_methods.join(', ')}"
    }

    def supported_methods = ['multisusie', 'susiex', 'sushie'] as Set
    def unsupported_methods = fine_mapping_methods
        .findAll { method -> !supported_methods.contains(method) }
        .sort()
    if (unsupported_methods) {
        error "Unsupported fine-mapping methods: ${unsupported_methods.join(', ')}"
    }

    ch_fine_mapping_input = ch_locus_annotation.map { r ->
        tuple(
            r.runId,
            r.fine_mapping_locus_set_id,
            r.metas,
            r.fine_mapping_locus_set_path,
            r.multi_ancestry_pairwise_ld_path,
        )
    }

    if (fine_mapping_methods.contains('multisusie')) {
        ch_multisusie = MULTISUSIE_FINE_MAPPING(ch_fine_mapping_input)
    } else {
        ch_multisusie = channel.empty()
    }

    if (fine_mapping_methods.contains('susiex')) {
        ch_susiex = SUSIEX_FINE_MAPPING(ch_fine_mapping_input)
    } else {
        ch_susiex = channel.empty()
    }

    if (fine_mapping_methods.contains('sushie')) {
        ch_sushie = SUSHIE_FINE_MAPPING(ch_fine_mapping_input)
    } else {
        ch_sushie = channel.empty()
    }

    emit:
    ch_multisusie = ch_multisusie
    ch_susiex = ch_susiex
    ch_sushie = ch_sushie
}
