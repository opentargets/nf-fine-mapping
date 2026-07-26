nextflow.enable.dsl = 2
nextflow.enable.types = true

include { GENTROPY_FINE_MAPPING_LOCUS_SET_LD_ANNOTATION } from '../../modules/local/gentropy/fine_mapping_locus_set_ld_annotation/main.nf'
include { COLLECTOR_CHECK_LD_PAIR_STATS } from '../../modules/local/collector/ld_pair_stats/main.nf'


workflow LOCUS_ANNOTATION {
    take:
    ch_full_overlap_loci: Channel<Map>

    main:
    ch_gentropy_ld_annotation_input = ch_full_overlap_loci.map { r ->
        if (!params.ld_registry) {
            error "LD annotation requires params.ld_registry when full-overlap loci are present."
        }

        def registry = params.ld_registry
        def requested_ancestries = r.metas.collect { meta -> meta.ancestry.toString() } as Set
        def selected_registry = registry.findAll { entry ->
            requested_ancestries.contains(entry.ancestry.toString())
        }
        if (selected_registry.size() != requested_ancestries.size()) {
            error "LD annotation registry is missing an ancestry required by run ${r.runId}."
        }

        tuple(
            r.runId,
            r.metas,
            r.collected_locus_path,
            registry.collect { entry -> file(entry.vi_path) },
            registry.collect { entry -> entry.bm_path.toString() },
            registry.collect { entry -> entry.ancestry.toString() },
        )
    }

    ch_gentropy_ld_annotation = GENTROPY_FINE_MAPPING_LOCUS_SET_LD_ANNOTATION(ch_gentropy_ld_annotation_input)
    ch_ld_pair_stats_status = COLLECTOR_CHECK_LD_PAIR_STATS(ch_gentropy_ld_annotation.stats)

    ch_locus_annotation = ch_gentropy_ld_annotation.annotation.map { runId, fine_mapping_locus_set_path, multi_ancestry_pairwise_ld_path, stats_path ->
        record(
            runId: runId,
            fine_mapping_locus_set_path: fine_mapping_locus_set_path,
            multi_ancestry_pairwise_ld_path: multi_ancestry_pairwise_ld_path,
            stats_path: stats_path,
        )
    }

    emit:
    ch_locus_annotation = ch_locus_annotation
    ch_fine_mapping_locus_sets = ch_locus_annotation.map { r -> r.fine_mapping_locus_set_path }
    ch_multi_ancestry_pairwise_ld = ch_locus_annotation.map { r -> r.multi_ancestry_pairwise_ld_path }
    ch_ld_pair_stats = ch_locus_annotation.map { r -> r.stats_path }
    ch_ld_pair_stats_status = ch_ld_pair_stats_status
}
